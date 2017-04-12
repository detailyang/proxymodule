package redisproxy

import (
	"fmt"
	"time"

	"github.com/absolute8511/go-zanredisdb"
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
}

type ZRDBProxy struct {
	dynamically bool
	conf        *ZRDBConf
	router      map[string]*zanredisdb.ZanRedisClient
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
		TendInterval: zrdb.DefaultTendInterval,
		DialTimeout:  zrdb.DefaultDialTimeout,
		ReadTimeout:  zrdb.DefaultReadTimeout,
		WriteTimeout: zrdb.DefaultWriteTimeout,
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

	return nil
}

func (proxy *ZRDBProxy) RegisterCmd(router *CmdRouter) {
	router.Register("get", proxy.cmdExec)
	router.Register("set", proxy.cmdExec)
	router.Register("del", proxy.cmdExec)
}

func (proxy *ZRDBProxy) GetStatisticsModule() ProxyStatisticsModule {
	return nil
}

func (proxy *ZRDBProxy) Stop() {
	for _, cli := range proxy.router {
		cli.Stop()
	}
}

func (proxy *ZRDBProxy) cmdExec(c *Client, resp ResponseWriter) error {
	pk, err := zrdb.ParseKey(c.Args[0])
	if err != nil {
		return err
	}

	var zrClient *zanredisdb.ZanRedisClient

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
		cmdArgs := make([]interface{}, len(c.Args))
		for i, v := range c.Args {
			cmdArgs[i] = v
		}

		if reply, err := zrClient.DoRedis(c.cmd, pk.RawKey, true, cmdArgs...); err == nil {
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
