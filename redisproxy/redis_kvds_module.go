package redisproxy

import (
	"bufio"
	"bytes"
	"fmt"

	"github.com/absolute8511/proxymodule/common"
	"github.com/absolute8511/proxymodule/redisproxy/kvds"
)

type BackendStore struct {
	Protocol       string
	Cluster        string
	ModuleConfPath string
}

type KVDSConf struct {
	KVStore []BackendStore
}

type KVModule struct {
	proxy   RedisProxyModule
	handler *CmdRouter
}

type KVDSProxy struct {
	conf    *KVDSConf
	modules map[string]KVModule
	ac      *kvds.AccessControl
}

func init() {
	RegisterRedisProxyModule("kvds", NewKVDSProxy)
}

func NewKVDSProxy() RedisProxyModule {
	return &KVDSProxy{
		modules: make(map[string]KVModule),
	}
}

func (self *KVDSProxy) GetProxyName() string {
	return "kvds-proxy"
}

func (self *KVDSProxy) InitConf(loadConfig func(v interface{}) error) error {
	self.conf = &KVDSConf{}
	if err := loadConfig(self.conf); err != nil {
		return err
	}

	kvds.SetLogger(redisLog)

	if ac, err := kvds.NewAccessControl(common.GlobalControlCenter); err != nil {
		return err
	} else {
		self.ac = ac
	}

	for _, KVConf := range self.conf.KVStore {
		module := KVModule{
			handler: NewCmdRouter(),
		}
		switch KVConf.Protocol {
		case "AEROSPIKE":
			{
				module.proxy = CreateRedis2AerospikeProxy()
			}
		case "CODIS":
			{
				module.proxy = CreateCodisProxy()
			}
		//TODO, support ZanRedisDB
		default:
			redisLog.Errorf("unknown protocol:%s ", KVConf.Protocol)
			continue
		}

		module.proxy.InitConf(func(v interface{}) error {
			redisLog.Infof("Init module config from : %s", KVConf.ModuleConfPath)
			return common.LoadModuleConfFromFile(KVConf.ModuleConfPath, v)
		})
		module.proxy.RegisterCmd(module.handler)
		self.modules[KVConf.Cluster] = module
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
	for _, module := range self.modules {
		module.proxy.Stop()
	}
}

func (kvds *KVDSProxy) GetStatisticsModule() ProxyStatisticsModule {
	return nil
}

func key2Cluster(key *kvds.KVDSKey) (string, error) {
	return "codis-all", nil
}

func (self *KVDSProxy) writeCmdExecute(c *Client, resp ResponseWriter) error {
	//use dummyWriter to avoid multiple replies to client when there are more than one cluster to write
	dummyWriter := &kvds.DummyRespWriter{}

	var err error
	key, err := kvds.ParseRedisKey(string(c.Args[0]))
	if err != nil {
		return err
	}

	rule, err := self.ac.GetWriteRule(key)
	if err != nil {
		return err
	}

	//make a copy of the original command arguments in case of
	//the key transformation used in data migration from cluster to cluster
	cmdArgs := make([][]byte, len(c.Args))
	for i, arg := range c.Args {
		cmdArgs[i] = append(cmdArgs[i], arg...)
	}

	for _, cluster := range rule.PreCluster {
		cmdHandler, ok := self.modules[cluster.Name].handler.GetCmdHandler(c.cmd)
		if !ok {
			return fmt.Errorf("command:%s is not supported by cluster:%s", c.cmd, cluster)
		} else {
			c.Args = cluster.KeyTransfer.Transform(c.cmd, cmdArgs)
			redisLog.Debugf("kvds write data to cluster: %s, cmd: %s", cluster, string(c.catGenericCommand()))
			err = cmdHandler(c, dummyWriter)
			if err != nil {
				redisLog.Errorf("kvds write data to cluster: %s failed, cmd: %s", cluster, string(c.catGenericCommand()))
				return err
			}
		}
	}

	for i, cluster := range rule.CurCluster {
		cmdHandler, ok := self.modules[cluster.Name].handler.GetCmdHandler(c.cmd)
		if !ok {
			return fmt.Errorf("command:%s is not supported by cluster: %s", c.cmd, cluster)
		} else {
			redisLog.Debugf("kvds write data to cluster: %s, cmd: %s", cluster, string(c.catGenericCommand()))
			if i == len(rule.CurCluster)-1 {
				err = cmdHandler(c, resp)
			} else {
				err = cmdHandler(c, dummyWriter)
			}
			if err != nil {
				redisLog.Errorf("kvds write data to cluster: %s failed, cmd: %s", cluster, string(c.catGenericCommand()))
				return err
			}
		}
	}

	return err
}

func (self *KVDSProxy) readCmdExecute(c *Client, resp ResponseWriter) error {
	key, err := kvds.ParseRedisKey(string(c.Args[0]))
	if err != nil {
		return err
	}

	rule, err := self.ac.GetReadRule(key)
	if err != nil {
		return err
	}

	cmdArgs := make([][]byte, len(c.Args))
	copy(cmdArgs, c.Args)

	for _, cluster := range rule.CurCluster {
		cmdHandler, ok := self.modules[cluster.Name].handler.GetCmdHandler(c.cmd)
		c.Args = cluster.KeyTransfer.Transform(c.cmd, cmdArgs)
		if !ok {
			redisLog.Errorf("command:%s is not supported by cluster:%s ", c.cmd, cluster)
		} else {
			if err = cmdHandler(c, resp); err == nil {
				return err
			} else {
				redisLog.Errorf("command:%s executed failed by cluster:%s ", c.catGenericCommand(), cluster)
			}
		}
	}

	if len(rule.CurCluster) > 0 {
		redisLog.Infof("kvds read current clusters failed, fail back to read previous clusters, info: [ %s ]", c.catGenericCommand())
	}

	//实现数据的动态迁移 => LiveMigration字段
	for _, cluster := range rule.PreCluster {
		cmdHandler, ok := self.modules[cluster.Name].handler.GetCmdHandler(c.cmd)
		c.Args = cluster.KeyTransfer.Transform(c.cmd, cmdArgs)
		if !ok {
			redisLog.Warningf("command:%s is not supported by cluster:%s ", c.cmd, cluster)
		} else {
			if err = cmdHandler(c, resp); err == nil {
				return err
			} else {
				redisLog.Warningf("command:%s executed failed by cluster:%s ", c.catGenericCommand(), cluster)
			}
		}
	}

	return fmt.Errorf("command:%s executed failed in all clusters", c.catGenericCommand())
}

func (self *KVDSProxy) commandInfo(c *Client, resp ResponseWriter) error {
	/*format of the command response
	#aerospike-cluster1
	protocal:AEROSPIKE
	#info
	....
	*/

	var info bytes.Buffer
	buf := &bytes.Buffer{}
	respWriter := NewRespWriter(bufio.NewWriter(buf))

	for _, store := range self.conf.KVStore {
		info.WriteString(fmt.Sprintf("#%s\r\n", store.Cluster))
		info.WriteString(fmt.Sprintf("protocol:%s\r\n", store.Protocol))
		if module, ok := self.modules[store.Cluster]; ok {

			if cmdHandler, ok := module.handler.GetCmdHandler("info"); ok {
				if err := cmdHandler(c, respWriter); err != nil {
					redisLog.Warningf("command:info executed failed at cluster:%s", store.Cluster)
				} else {
					respWriter.Flush()
					//the response from command handle is in RESP protocol and we should remove the
					//protocol header for human readable
					rawData := bytes.SplitN(buf.Bytes(), respTerm, 2)
					if len(rawData) == 2 {
						info.WriteString("#info\r\n")
						info.Write(rawData[1])
					}
				}
				buf.Reset()
			}
		}
		info.WriteString("\r\n\r\n")
	}

	resp.WriteBulk(info.Bytes())

	return nil
}
