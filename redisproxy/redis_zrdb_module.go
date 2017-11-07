package redisproxy

import (
	"errors"
	"fmt"
	"time"

	"github.com/absolute8511/go-zanredisdb"
	"github.com/absolute8511/proxymodule/redisproxy/stats"
	"github.com/absolute8511/proxymodule/redisproxy/zrdb"
)

type ZRDBConf struct {
	DialTimeout   int64
	ReadTimeout   int64
	WriteTimeout  int64
	TendInterval  int64
	IdleTimeout   int64
	MaxActiveConn int
	MaxIdleConn   int

	LookupList []string
	Password   string
	Namespace  []string
}

type ZRDBProxy struct {
	stats.ModuleStats
	dynamically  bool
	conf         *ZRDBConf
	router       map[string]*zanredisdb.ZanRedisClient
	asKVDSModule bool
}

func init() {
	RegisterRedisProxyModule("zanredisdb-proxy", CreateZRDBProxy)
}

func CreateZRDBProxy() RedisProxyModule {
	return &ZRDBProxy{
		router:      make(map[string]*zanredisdb.ZanRedisClient),
		ModuleStats: stats.NewProxyModuleStats(),
	}
}

func (proxy *ZRDBProxy) GetProxyName() string {
	return "ZanRedisDB-Proxy"
}

func (proxy *ZRDBProxy) InitConf(loadConfig func(v interface{}) error) error {
	proxy.conf = &ZRDBConf{
		TendInterval:  zrdb.DefaultTendInterval,
		DialTimeout:   zrdb.DefaultDialTimeout,
		ReadTimeout:   zrdb.DefaultReadTimeout,
		WriteTimeout:  zrdb.DefaultWriteTimeout,
		IdleTimeout:   zrdb.DefaultIdleTimeout,
		MaxActiveConn: zrdb.DefaultMaxActiveConn,
		MaxIdleConn:   zrdb.DefaultMaxIdleConn,
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
				redisLog.Infof("add router for namespace:%s to handle request", ns)
			}
		}
	}

	proxy.ModuleStats = stats.NewProxyModuleStats()

	return nil
}

func (proxy *ZRDBProxy) RegisterCmd(router *CmdRouter) {
	router.Register("get", commandSingleKeyExec(proxy))
	router.Register("set", commandSingleKeyExec(proxy))
	router.Register("del", commandSingleKeyExec(proxy))
	router.Register("expire", commandSingleKeyExec(proxy))
	router.Register("ttl", commandSingleKeyExec(proxy))
	router.Register("persist", commandSingleKeyExec(proxy))
	router.Register("setex", commandSingleKeyExec(proxy))
	router.Register("setnx", commandSingleKeyExec(proxy))
	router.Register("incr", commandSingleKeyExec(proxy))
	router.Register("exists", commandMultiKeyExec(proxy))
	router.Register("mset", commandMultiKVExec(proxy))
	router.Register("mget", proxy.mgetCommand)

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

	for _, jsonCmd := range zrdb.JsonCmds {
		router.Register(jsonCmd, commandSingleKeyExec(proxy))
	}

	for _, GeoCmd := range zrdb.GeoCmds {
		router.Register(GeoCmd, commandSingleKeyExec(proxy))
	}
}

func (proxy *ZRDBProxy) GetStats() stats.ModuleStats {
	return proxy.ModuleStats
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
			redisLog.Infof("dynamically add router for namespace:%s to handle request", pk.Namespace)
		}
	}

	if zrClient != nil {
		proxy.UpdateStats(cmd, convZKey2Table(pk), 1)
		if reply, err := zrClient.DoRedis(cmd, pk.ShardingKey(), true, cmdArgs...); err == nil {
			WriteValue(resp, reply)
			return nil
		} else {
			return err
		}
	} else {
		return fmt.Errorf("can not find router to handle request of namespace:%s", pk.Namespace)
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
		IdleTimeout:  time.Duration(proxy.conf.IdleTimeout) * time.Second,

		MaxActiveConn: proxy.conf.MaxActiveConn,
		MaxIdleConn:   proxy.conf.MaxIdleConn,
		TendInterval:  proxy.conf.TendInterval,
		Namespace:     namespace,
		Password:      proxy.conf.Password,
	})
	zrClient.Start()
	proxy.router[namespace] = zrClient

	return zrClient, nil
}

func commandSingleKeyExec(proxy *ZRDBProxy) func(c *Client, resp ResponseWriter) error {
	return func(c *Client, resp ResponseWriter) error {
		if len(c.Args) == 0 {
			return ErrCmdParams
		}
		if pk, err := zrdb.ParseKey(c.Args[0]); err != nil {
			return err
		} else {
			cmdArgs := make([]interface{}, len(c.Args))
			for i, v := range c.Args {
				cmdArgs[i] = v
			}
			return proxy.cmdExec(c.cmd, resp, pk, cmdArgs...)
		}
	}
}

// exists, delete
// TODO, the keys may cross partitions.
func commandMultiKeyExec(proxy *ZRDBProxy) func(c *Client, resp ResponseWriter) error {
	return func(c *Client, resp ResponseWriter) error {
		if len(c.Args) == 0 {
			return ErrCmdParams
		}

		cmdArgs := make([]interface{}, len(c.Args))
		for i, rawKey := range c.Args {
			if _, err := zrdb.ParseKey(rawKey); err != nil {
				return err
			}
			cmdArgs[i] = rawKey
		}

		if pk, err := zrdb.ParseKey(c.Args[0]); err != nil {
			return err
		} else {
			return proxy.cmdExec(c.cmd, resp, pk, cmdArgs...)
		}
	}
}

// mset
// TODO, the keys may cross partitions.
func commandMultiKVExec(proxy *ZRDBProxy) func(c *Client, resp ResponseWriter) error {
	return func(c *Client, resp ResponseWriter) error {
		if len(c.Args)%2 != 0 || len(c.Args) == 0 {
			return ErrCmdParams
		}

		cmdArgs := make([]interface{}, len(c.Args))
		for i := 0; i < len(c.Args); i += 2 {
			if _, err := zrdb.ParseKey(c.Args[i]); err != nil {
				return err
			}
		}
		for i, rawArg := range c.Args {
			cmdArgs[i] = rawArg
		}

		if pk, err := zrdb.ParseKey(c.Args[0]); err != nil {
			return err
		} else {
			return proxy.cmdExec(c.cmd, resp, pk, cmdArgs...)
		}
	}
}

func (proxy *ZRDBProxy) mgetCommand(c *Client, resp ResponseWriter) error {
	if len(c.Args) == 0 {
		return ErrCmdParams
	}

	var pKeys []*zanredisdb.PKey
	var Namespace string
	for _, key := range c.Args {
		pk, err := zrdb.ParseKey(key)
		if err != nil {
			return err
		} else if Namespace == "" {
			Namespace = pk.Namespace
		} else if Namespace != pk.Namespace {
			return errors.New("can not use MGET to get keys cross namespaces")
		}

		pKeys = append(pKeys, pk)

		proxy.UpdateStats("mget", convZKey2Table(pk), 1)
	}

	var zrClient *zanredisdb.ZanRedisClient
	var err error
	zrClient, ok := proxy.router[Namespace]
	if !ok && proxy.dynamically {
		if zrClient, err = proxy.newZRClient(Namespace); err != nil {
			return err
		}
		redisLog.Infof("dynamically add router for namespace:%s to handle request", Namespace)
	}

	if zrClient == nil {
		return fmt.Errorf("can not find router to handle request of namespace:%s", Namespace)
	}

	if rsp, err := zrClient.KVMGet(pKeys...); err != nil {
		return err
	} else {
		WriteValue(resp, rsp)
		return nil
	}
}

func convZKey2Table(key *zanredisdb.PKey) string {
	return key.Namespace + ":" + key.Set
}
