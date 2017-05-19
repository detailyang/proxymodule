package redisproxy

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/absolute8511/go-zanredisdb"
	"github.com/absolute8511/proxymodule/redisproxy/kvds"
	"github.com/absolute8511/proxymodule/redisproxy/zrdb"
)

type ZRDBConf struct {
	DialTimeout  int64
	ReadTimeout  int64
	WriteTimeout int64
	TendInterval int64
	LookupList   []string
	Password     string
	Namespace    []string

	MonitorApp      string
	MonitorBusiness string
}

type ZRDBProxy struct {
	dynamically      bool
	conf             *ZRDBConf
	router           map[string]*zanredisdb.ZanRedisClient
	asKVDSModule     bool
	statisticsModule *zrdb.StatisticsModule
}

func init() {
	RegisterRedisProxyModule("zanredisdb-proxy", CreateZRDBProxy)
}

func CreateZRDBProxy() RedisProxyModule {
	return &ZRDBProxy{
		router: make(map[string]*zanredisdb.ZanRedisClient),
	}
}

func (proxy *ZRDBProxy) GetProxyName() string {
	return "ZanRedisDB-Proxy"
}

func (proxy *ZRDBProxy) InitConf(loadConfig func(v interface{}) error) error {

	proxy.conf = &ZRDBConf{
		TendInterval:    zrdb.DefaultTendInterval,
		DialTimeout:     zrdb.DefaultDialTimeout,
		ReadTimeout:     zrdb.DefaultReadTimeout,
		WriteTimeout:    zrdb.DefaultWriteTimeout,
		MonitorApp:      zrdb.DefaultMonitorApp,
		MonitorBusiness: zrdb.DefaultMonitorBusiness,
	}

	if err := loadConfig(proxy.conf); err != nil {
		return err
	}

	if len(proxy.conf.Namespace) == 0 {
		proxy.dynamically = true
	} else {
		proxy.dynamically = false
		for _, ns := range proxy.conf.Namespace {
			if _, err := proxy.newZRClient(ns); err != nil {
				redisLog.Errorf("add router for namespace:%s failed, err: %s", ns, err.Error())
			} else {
				redisLog.Infof("add router for namespace:%s to handle ZanRedisDB request", ns)
			}
		}
	}

	proxy.statisticsModule = zrdb.NewStatisticsModule(proxy.conf.MonitorApp, proxy.conf.MonitorBusiness)

	return nil
}

func (proxy *ZRDBProxy) RegisterCmd(router *CmdRouter) {
	router.Register("get", commandSingleKeyExec(proxy))
	router.Register("set", commandSingleKeyExec(proxy))
	router.Register("del", commandSingleKeyExec(proxy))

	for _, hashCmd := range zrdb.HashCmds {
		router.Register(hashCmd, commandSingleKeyExec(proxy))
	}

	//register list commands
	for _, listCmd := range zrdb.ListCmds {
		router.Register(listCmd, commandSingleKeyExec(proxy))
	}

	//register set commands
	for _, setCmd := range zrdb.SetCmds {
		router.Register(setCmd, commandSingleKeyExec(proxy))
	}

	//register zset commands
	for _, zsetCmd := range zrdb.ZSetCmds {
		router.Register(zsetCmd, commandSingleKeyExec(proxy))
	}
}

func (proxy *ZRDBProxy) GetStatisticsModule() ProxyStatisticsModule {
	return proxy.statisticsModule
}

func (proxy *ZRDBProxy) Stop() {
	for _, cli := range proxy.router {
		cli.Stop()
	}
}

func (proxy *ZRDBProxy) cmdExec(cmd string, resp ResponseWriter, pk *zanredisdb.PKey, cmdArgs ...interface{}) error {
	var zrClient *zanredisdb.ZanRedisClient
	var err error

	zrClient, ok := proxy.router[pk.Namespace]
	if !ok && proxy.dynamically {
		zrClient, err = proxy.newZRClient(pk.Namespace)
		if err != nil {
			return err
		} else {
			redisLog.Infof("dynamically add router for namespace:%s to handle ZanRedisDB request", pk.Namespace)
		}
	}

	if zrClient != nil {
		if reply, err := zrClient.DoRedis(cmd, pk.ShardingKey(), true, cmdArgs...); err == nil {
			WriteValue(resp, reply)
			return nil
		} else {
			return err
		}
	} else {
		return fmt.Errorf("can not find router to handle ZanRedisDB request of namespace:%s", pk.Namespace)
	}
}

func (proxy *ZRDBProxy) newZRClient(namespace string) (*zanredisdb.ZanRedisClient, error) {
	if _, ok := proxy.router[namespace]; ok {
		return nil, fmt.Errorf("router to handle ZanRedisDB request of namespace:%s already registered", namespace)
	}

	zrClient := zanredisdb.NewZanRedisClient(&zanredisdb.Conf{
		LookupList:   proxy.conf.LookupList,
		DialTimeout:  time.Duration(proxy.conf.DialTimeout) * time.Second,
		ReadTimeout:  time.Duration(proxy.conf.ReadTimeout) * time.Second,
		WriteTimeout: time.Duration(proxy.conf.WriteTimeout) * time.Second,
		TendInterval: proxy.conf.TendInterval,
		Namespace:    namespace,
		Password:     proxy.conf.Password,
	})
	zrClient.Start()
	proxy.router[namespace] = zrClient

	return zrClient, nil
}

func (proxy *ZRDBProxy) CheckUsedAsKVDSModule() bool {
	if len(proxy.conf.Namespace) == 1 {
		return true
	} else {
		return false
	}
}

func (proxy *ZRDBProxy) SetUsedAsKVDSModule() error {
	if !proxy.CheckUsedAsKVDSModule() {
		return errors.New("the ZanRedisDB proxy module can't be used as a module of KVDS, please check the configuration")
	} else {
		proxy.asKVDSModule = true
		proxy.dynamically = false
		return nil
	}
}

func commandSingleKeyExec(proxy *ZRDBProxy) func(c *Client, resp ResponseWriter) error {
	return func(c *Client, resp ResponseWriter) error {
		proxy.statisticsModule.Sampling(c.cmd)
		var pk *zanredisdb.PKey
		cmdArgs := make([]interface{}, len(c.Args))
		var err error
		if !proxy.asKVDSModule {
			pk, err = zrdb.ParseKey(c.Args[0])
			if err != nil {
				return err
			}
			for i, v := range c.Args {
				cmdArgs[i] = v
			}
		} else {
			fields := bytes.SplitN(c.Args[0], []byte(kvds.KeySep), 3)
			if len(fields) != 3 {
				return zrdb.ErrKeyInvalid
			} else {
				pk = zanredisdb.NewPKey(proxy.conf.Namespace[0], string(fields[1]), fields[2])
			}
			cmdArgs[0] = pk.RawKey
			for i, v := range c.Args[1:] {
				cmdArgs[i+1] = v
			}
		}
		return proxy.cmdExec(c.cmd, resp, pk, cmdArgs...)
	}
}

//func commandMultiKeyExec(proxy *ZRDBConf) func(c *Client, resp ResponseWriter) error {
//}
