package redisproxy

import (
	"bufio"
	"bytes"
	"fmt"
	"math/rand"
	"strings"

	"github.com/absolute8511/proxymodule/common"
	"github.com/absolute8511/proxymodule/redisproxy/kvds"
	"github.com/absolute8511/proxymodule/redisproxy/stats"
)

type KVModule struct {
	Protocol string
	proxy    RedisProxyModule
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
	stats.ModuleStats

	conf      *kvds.Conf
	namespace map[string]KVClusters
	ac        *kvds.AccessControl
}

func init() {
	RegisterRedisProxyModule("kvds", NewKVDSProxy)
}

func NewKVDSProxy() RedisProxyModule {
	return &KVDSProxy{
		namespace:   make(map[string]KVClusters),
		ModuleStats: kvds.NewStats(),
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

			switch conf.Protocol {
			case "AEROSPIKE":
				{
					module.proxy = CreateRedis2AerospikeProxy()
				}
			case "CODIS":
				{
					module.proxy = CreateCodisProxy()
				}
			case "ZRDB":
				{
					module.proxy = CreateZRDBProxy()
				}
			default:
				redisLog.Errorf("unknown protocol:%s ", conf.Protocol)
				continue
			}

			if err := module.proxy.InitConf(func(v interface{}) error {
				redisLog.Infof("init kvds module [%s, %s], protocol:%s, config from : %s", ns, cluster, conf.Protocol, conf.ConfPath)
				return common.LoadModuleConfFromFile(conf.ConfPath, v)
			}); err != nil {
				redisLog.Errorf("init kvds module [%s, %s] from:%s failed, err:%s", ns, cluster, conf.ConfPath, err.Error())
				continue
			}

			module.Protocol = conf.Protocol
			module.proxy.RegisterCmd(module.handler)
			if err := self.namespace[ns].AddKVModule(cluster, &module); err != nil {
				redisLog.Errorf("add kv module:%s into namespace:%s failed as:%s", cluster, ns, err.Error())
			} else {
				self.ModuleStats.(*kvds.Stats).AddMemStats(cluster, module.proxy.GetStats())
				redisLog.Infof("kv module [%s, %s] of protocol:%s come into service", ns, cluster, conf.Protocol)
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

func (self *KVDSProxy) GetStats() stats.ModuleStats {
	return self.ModuleStats
}

func (self *KVDSProxy) writeCmdExecute(c *Client, resp ResponseWriter) error {
	ns, table, err := kvds.ExtractNamespceTable(c.cmd, c.Args)
	if err != nil {
		return err
	}

	if _, ok := self.namespace[ns]; !ok {
		return fmt.Errorf("no namespace named:%s exists", ns)
	}

	route, err := self.ac.GetTableRoute(ns, table)
	if err != nil {
		return err
	}

	if route.CurCluster.Empty() {
		return fmt.Errorf("the current cluster used to write [%s, %s] is empty", ns, table)
	}

	self.ModuleStats.UpdateStats(c.cmd, genStatsKey(ns, table), 1)

	if clster := &route.PreCluster; !clster.Empty() {
		redisLog.Debugf("write to previous cluster [%s, %s], cmd: %s",
			ns, clster.Name, string(c.catGenericCommand()))

		err = self.doCommand(ns, clster, c, &kvds.DummyRespWriter{})
		if err != nil {
			redisLog.Errorf("write failed at previous cluster [%s, %s], err:%s", ns, clster.Name, err.Error())
			return err
		}
	}

	redisLog.Debugf("write to current cluster [%s, %s], cmd: %s", ns, table, string(c.catGenericCommand()))

	if err = self.doCommand(ns, &route.CurCluster, c, resp); err != nil {
		redisLog.Errorf("write failed at current cluster [%s, %s], err:%s", ns,
			route.CurCluster.Name, err.Error())
	}

	return err
}

func (self *KVDSProxy) readCmdExecute(c *Client, resp ResponseWriter) error {
	ns, table, err := kvds.ExtractNamespceTable(c.cmd, c.Args)
	if err != nil {
		return err
	}

	if _, ok := self.namespace[ns]; !ok {
		return fmt.Errorf("no namespace named:%s has been used right now", ns)
	}

	route, err := self.ac.GetTableRoute(ns, table)
	if err != nil {
		return err
	}

	if route.CurCluster.Empty() {
		return fmt.Errorf("the current cluster used to read [%s, %s] is empty", ns, table)
	}

	self.ModuleStats.UpdateStats(c.cmd, genStatsKey(ns, table), 1)

	if !route.PreCluster.Empty() {
		if tryGradation(c, route) {
			redisLog.Debugf("table [%s, %s] read current cluster [%s] and may fallback to previous cluster [%s]",
				ns, table, route.CurCluster.Name, route.PreCluster.Name)

			buf := &bytes.Buffer{}
			respBuf := NewRespWriter(bufio.NewWriter(buf))

			//Read current cluster at first.
			if err := self.doCommand(ns, &route.CurCluster, c, respBuf); err != nil {
				redisLog.Errorf("read [%s, %s] from current cluster:%s failed, error:%s, fallback to read previous cluster:%s",
					ns, table, route.CurCluster.Name, err.Error(), route.PreCluster.Name)
			} else {
				respBuf.Flush()
				if IsNilValue(buf.Bytes()) {
					redisLog.Debugf("read [%s, %s] from current cluster:%s return empty, fallback to read previous cluster:%s",
						ns, table, route.CurCluster.Name, route.PreCluster.Name)
				} else {
					resp.WriteRawBytes(buf.Bytes())
					return nil
				}
			}
		}
		redisLog.Debugf("read [%s, %s] from previous cluster:%s", ns, table, route.PreCluster.Name)
		return self.doCommand(ns, &route.PreCluster, c, resp)
	}

	redisLog.Debugf("read [%s, %s] from current cluster:%s", ns, table, route.CurCluster.Name)
	return self.doCommand(ns, &route.CurCluster, c, resp)
}

/*
format of the command response
# Namespace, 3 clusters in service
#0.cluster-name
protocol:AEROSPIKE
#Info:
....

TODO, support section
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
						info.Write(bytes.TrimRight(rawData[1], "\r\n"))
					}
				}
				buf.Reset()
			}
			info.WriteString("\r\n")
			index++
		}
	}

	info.WriteString(self.ModuleStats.String())
	resp.WriteBulk(info.Bytes())

	return nil
}

func (self *KVDSProxy) doCommand(ns string, clster *kvds.Cluster, c *Client, resp ResponseWriter) error {
	module, ok := self.namespace[ns].GetKVModule(clster.Name)
	if !ok {
		return fmt.Errorf("no cluster named:%s exists in namespace:%s ", ns, clster.Name)
	}

	cmdArgs := make([][]byte, len(c.Args))
	copy(cmdArgs, c.Args)

	// Set the command arguments before return.
	defer func() {
		c.Args = cmdArgs
	}()

	c.Args = clster.Convert(c.cmd, c.Args)

	if cmdHandler, ok := module.handler.GetCmdHandler(c.cmd); !ok {
		return fmt.Errorf("command:%s is not supported by [%s, %s] ", c.cmd, ns, clster.Name)
	} else {
		return cmdHandler(c, resp)
	}
}

func genStatsKey(namespace string, table string) string {
	return namespace + ":" + table
}

func tryGradation(c *Client, route *kvds.Route) bool {
	if len(route.Gradation) > 0 {
		var percent float64
		percent = route.Gradation[kvds.HostWildCard].Percent
		host := (strings.SplitN(c.remoteAddr, ":", 2))[0]
		if v, ok := route.Gradation[host]; ok {
			percent = v.Percent
		}
		if rand.Float64() < percent/100 {
			return true
		}
	}
	return false
}
