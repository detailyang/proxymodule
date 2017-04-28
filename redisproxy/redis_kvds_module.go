package redisproxy

import (
	"bufio"
	"bytes"
	"fmt"

	"github.com/absolute8511/proxymodule/common"
	"github.com/absolute8511/proxymodule/redisproxy/kvds"
)

type KVDSRedisProxyModule interface {
	RedisProxyModule
	SetUsedAsKVDSModule() error
	CheckUsedAsKVDSModule() bool
}

type KVModule struct {
	Protocol string
	proxy    KVDSRedisProxyModule
	handler  *CmdRouter
}

type KVClusters map[string]*KVModule

func NewKVClusters() KVClusters {
	return make(map[string]*KVModule)
}

func (clusters KVClusters) AddKVModule(name string, module *KVModule) error {
	if _, ok := clusters[name]; ok {
		return fmt.Errorf("%s already in the KVDS back-end modules")
	} else {
		clusters[name] = module
	}
	return nil
}

func (clusters KVClusters) GetKVModule(name string) (*KVModule, bool) {
	module, ok := clusters[name]
	return module, ok
}

type KVDSProxy struct {
	conf      *kvds.Conf
	namespace map[string]KVClusters
	ac        *kvds.AccessControl
}

func init() {
	RegisterRedisProxyModule("kvds", NewKVDSProxy)
}

func NewKVDSProxy() RedisProxyModule {
	return &KVDSProxy{
		namespace: make(map[string]KVClusters),
	}
}

func (self *KVDSProxy) GetProxyName() string {
	return "kvds-proxy"
}

func (self *KVDSProxy) InitConf(loadConfig func(v interface{}) error) error {
	self.conf = &kvds.Conf{}
	if err := loadConfig(self.conf); err != nil {
		return err
	}

	kvds.SetLogger(redisLog)

	if ac, err := kvds.NewAccessControl(common.GlobalControlCenter); err != nil {
		return err
	} else {
		self.ac = ac
	}

	for ns, clusters := range self.conf.Namespace {
		self.namespace[ns] = NewKVClusters()
		for cluster, conf := range clusters {
			module := KVModule{
				handler: NewCmdRouter(),
			}

			var proxy RedisProxyModule
			switch conf.Protocol {
			case "AEROSPIKE":
				{
					proxy = CreateRedis2AerospikeProxy()
				}
			case "CODIS":
				{
					proxy = CreateCodisProxy()
				}
			case "ZRDB":
				{
					proxy = CreateZRDBProxy()
				}
			default:
				redisLog.Errorf("unknown protocol:%s ", conf.Protocol)
				continue
			}

			if kvdsProxy, ok := proxy.(KVDSRedisProxyModule); ok {
				module.proxy = kvdsProxy
			} else {
				redisLog.Errorf("the RedisProxyModule for protocol:%s can not be used as KVDSRedisProxyModule", conf.Protocol)
				continue
			}

			if err := module.proxy.InitConf(func(v interface{}) error {
				redisLog.Infof("init kvds module [%s, %s], protocol:%s, config from : %s", ns, cluster, conf.Protocol, conf.ConfPath)
				return common.LoadModuleConfFromFile(conf.ConfPath, v)
			}); err != nil {
				redisLog.Errorf("init kvds module [%s, %s] from:%s failed, err:%s", ns, cluster, conf.ConfPath, err.Error())
				continue
			}

			if !module.proxy.CheckUsedAsKVDSModule() {
				redisLog.Errorf("checking of used as KVDS module by cluster [%s, %s] of protocol:%s failed", ns, cluster, conf.Protocol)
			} else if err := module.proxy.SetUsedAsKVDSModule(); err != nil {
				redisLog.Errorf("set as KVDS module by cluster [%s, %s] of protocol:%s failed, err:%s", ns, cluster, conf.Protocol, err.Error())
			} else {
				module.Protocol = conf.Protocol
				module.proxy.RegisterCmd(module.handler)
				if err := self.namespace[ns].AddKVModule(cluster, &module); err != nil {
					redisLog.Errorf("add kv module:%s into namespace:%s failed as:%s", cluster, ns, err.Error())
				} else {
					redisLog.Errorf("kv module [%s, %s] of protocol:%s come into service", ns, cluster, conf.Protocol)
				}
			}
		}
	}

	return nil
}

func (self *KVDSProxy) RegisterCmd(router *CmdRouter) {
	router.Register("info", self.commandInfo)

	for cmd, _ := range kvds.WriteCommands {
		router.Register(cmd, self.writeCmdExecute)
	}

	for cmd, _ := range kvds.ReadCommands {
		router.Register(cmd, self.readCmdExecute)
	}
}

func (self *KVDSProxy) Stop() {
	for _, clusters := range self.namespace {
		for _, module := range clusters {
			module.proxy.Stop()
		}
	}
}

func (kvds *KVDSProxy) GetStatisticsModule() ProxyStatisticsModule {
	return nil
}

func (self *KVDSProxy) writeCmdExecute(c *Client, resp ResponseWriter) error {
	key, err := kvds.ParseRedisKey(string(c.Args[0]))
	if err != nil {
		return err
	}

	if _, ok := self.namespace[key.Namespace]; !ok {
		return fmt.Errorf("no namespace named:%s exists", key.Namespace)
	}

	rule, err := self.ac.GetWriteRule(key)
	if err != nil {
		return err
	}

	if rule.CurCluster.Empty() {
		return fmt.Errorf("the current cluster used to write [%s, %s] is empty", key.Namespace, key.Table)
	}

	//make a copy of the original command arguments in case of
	//the key transformation used in data migration from cluster to cluster
	cmdArgs := make([][]byte, len(c.Args))
	copy(cmdArgs, c.Args)

	if cluster := rule.PreCluster; !cluster.Empty() {
		redisLog.Debugf("kvds write data to previous cluster [%s, %s], cmd: %s",
			key.Namespace, cluster.Name, string(c.catGenericCommand()))

		c.Args = cluster.KeyTransfer.Transform(c.cmd, cmdArgs)
		err = self.doCommand(key.Namespace, cluster.Name, c, &kvds.DummyRespWriter{})
		//set the arguments back to original
		c.Args = cmdArgs
		if err != nil {
			redisLog.Errorf("do write command failed at previous cluster [%s, %s], err:%s", key.Namespace, cluster.Name, err.Error())
			return err
		}
	}

	redisLog.Debugf("kvds write data to current cluster [%s, %s], cmd: %s", key.Namespace, key.Table, string(c.catGenericCommand()))
	c.Args = rule.CurCluster.KeyTransfer.Transform(c.cmd, cmdArgs)
	if err = self.doCommand(key.Namespace, rule.CurCluster.Name, c, resp); err != nil {
		redisLog.Errorf("do write command failed at current cluster [%s, %s], err:%s", key.Namespace, rule.CurCluster.Name, err.Error())
	}

	c.Args = cmdArgs
	return err
}

func (self *KVDSProxy) readCmdExecute(c *Client, resp ResponseWriter) error {
	key, err := kvds.ParseRedisKey(string(c.Args[0]))
	if err != nil {
		return err
	}

	if _, ok := self.namespace[key.Namespace]; !ok {
		return fmt.Errorf("no namespace named:%s has been used right now", key.Namespace)
	}

	rule, err := self.ac.GetReadRule(key)
	if err != nil {
		return err
	}

	if rule.CurCluster.Empty() {
		return fmt.Errorf("the current cluster used to read [%s, %s] is empty",
			key.Namespace, key.Table)
	}

	cmdArgs := make([][]byte, len(c.Args))
	copy(cmdArgs, c.Args)

	defer func() {
		//set the arguments back to original before return
		c.Args = cmdArgs
	}()

	if !rule.PreCluster.Empty() {
		redisLog.Debugf("table [%s, %s] read current cluster [%s] and previous cluster [%s]",
			key.Namespace, key.Table, rule.CurCluster.Name, rule.PreCluster.Name)

		buf := &bytes.Buffer{}
		respBuf := NewRespWriter(bufio.NewWriter(buf))

		//read current cluster at first
		c.Args = rule.CurCluster.KeyTransfer.Transform(c.cmd, cmdArgs)
		if err := self.doCommand(key.Namespace, rule.CurCluster.Name, c, respBuf); err != nil {
			redisLog.Errorf("read [%s, %s] from current cluster:%s failed, error:%s, fall back to read previous cluster:%s",
				key.Namespace, key.Table, rule.CurCluster.Name, err.Error(), rule.PreCluster.Name)
		} else {
			respBuf.Flush()
			if IsNilValue(buf.Bytes()) {
				redisLog.Infof("read [%s, %s] from current cluster:%s return empty, fall back to read previous cluster:%s",
					key.Namespace, key.Table, rule.CurCluster.Name, rule.PreCluster.Name)
			} else {
				resp.WriteRawBytes(buf.Bytes())
				return nil
			}
		}

		c.Args = rule.PreCluster.KeyTransfer.Transform(c.cmd, cmdArgs)
		return self.doCommand(key.Namespace, rule.PreCluster.Name, c, resp)
	} else {
		c.Args = rule.CurCluster.KeyTransfer.Transform(c.cmd, cmdArgs)
		return self.doCommand(key.Namespace, rule.CurCluster.Name, c, resp)
	}
}

/*
format of the command response
# Namespace, 3 clusters in service
#0.cluster-name
protocol:AEROSPIKE
#Info:
....
*/
func (self *KVDSProxy) commandInfo(c *Client, resp ResponseWriter) error {
	var info bytes.Buffer
	buf := &bytes.Buffer{}
	respWriter := NewRespWriter(bufio.NewWriter(buf))

	for ns, clusters := range self.namespace {
		info.WriteString(fmt.Sprintf("# %s, %d clusters in service\r\n", ns, len(clusters)))
		index := 0
		for name, cluster := range clusters {
			info.WriteString(fmt.Sprintf("#%d.%s\r\n", index, name))
			info.WriteString(fmt.Sprintf("protocol:%s\r\n", cluster.Protocol))
			if cmdHandler, ok := cluster.handler.GetCmdHandler("info"); ok {
				if err := cmdHandler(c, respWriter); err != nil {
					redisLog.Warningf("command:info executed failed at cluster:[%s, %s]", ns, name)
				} else {
					respWriter.Flush()
					//the response from command handle is in RESP protocol and we should remove the
					//protocol header for human readable
					rawData := bytes.SplitN(buf.Bytes(), respTerm, 2)
					if len(rawData) == 2 {
						info.WriteString("#Info:\r\n")
						info.Write(rawData[1])
					}
				}
				buf.Reset()
			}
			info.WriteString("\r\n")
			index++
		}
	}

	resp.WriteBulk(info.Bytes())

	return nil
}

func (self *KVDSProxy) doCommand(ns string, cluster string, c *Client, resp ResponseWriter) error {
	module, ok := self.namespace[ns].GetKVModule(cluster)
	if !ok {
		return fmt.Errorf("no cluster named:%s exists in namespace:%s ", ns, cluster)
	}

	if cmdHandler, ok := module.handler.GetCmdHandler(c.cmd); !ok {
		return fmt.Errorf("command:%s is not supported by [%s, %s] ", c.cmd, ns, cluster)
	} else {
		return cmdHandler(c, resp)
	}
}
