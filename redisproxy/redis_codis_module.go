package redisproxy

import (
	"bytes"
	"sync"
	"time"

	"github.com/absolute8511/proxymodule/redisproxy/stats"
	"github.com/absolute8511/redigo/redis"
)

var (
	supportedCommands []string
)

const (
	defaultTendInterval = 3
)

type CodisServer struct {
	ServerAddr string
}

type CodisProxyConf struct {
	TendInterval int64
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	DialTimeout  time.Duration

	ServerList []CodisServer
}

type CodisProxy struct {
	sync.Mutex
	stats.ModuleStats
	conf    *CodisProxyConf
	cluster *CodisCluster
}

func init() {
	RegisterRedisProxyModule("codis-proxy", CreateCodisProxy)

	supportedCommands = []string{
		"get", "mget", "setnx", "del",
		"set", "setex", "exists", "expire",
		"ttl", "incr", "incrby", "decr",
		"decrby", "hget", "hgetall", "hmget",
		"hmset", "hset", "hdel", "hexists",
		"hincrby", "mset", "rpush", "getset",
		"lpush", "lrange", "llen", "lrem",
		"zrangebyscore", "zrange", "zrevrange", "zrevrangebyscore",
		"zcard", "zrank", "zrevrank", "sadd",
		"srem", "sismember", "sinterstore", "sdiffstore",
		"sinter", "sunion", "ssize", "keys",
		"sdiff", "smembers", "spop", "scard",
		"srandmember", "zadd", "zremrangebyscore", "zrem",
		"zcount",
	}
}

func CreateCodisProxy() RedisProxyModule {
	return &CodisProxy{}
}

func (proxy *CodisProxy) GetProxyName() string {
	return "codis-proxy"
}

func (proxy *CodisProxy) SupportedCommands() []string {
	return supportedCommands
}

func (proxy *CodisProxy) InitConf(loadConfig func(v interface{}) error) error {
	proxy.conf = &CodisProxyConf{}
	if err := loadConfig(proxy.conf); err != nil {
		return err
	}

	if proxy.conf.TendInterval <= 0 {
		proxy.conf.TendInterval = defaultTendInterval
	}

	proxy.cluster = NewCodisCluster(proxy.conf)
	proxy.ModuleStats = stats.NewProxyModuleStats()

	return nil
}

func (proxy *CodisProxy) RegisterCmd(router *CmdRouter) {
	maxRetry := int(len(proxy.conf.ServerList)/2) + 1

	commandExec := func(c *Client, resp ResponseWriter) error {
		if len(c.Args) < 1 {
			return ErrCmdParams
		}

		cmdArgs := make([]interface{}, len(c.Args))
		for i, v := range c.Args {
			cmdArgs[i] = v
		}

		proxy.UpdateStats(c.cmd, codisKey2Table(c.Args[0]), 1)

		var reply interface{}
		var err error
		var conn redis.Conn

		for i := 0; i < maxRetry; i++ {
			conn, err = proxy.cluster.GetConn()
			if err != nil {
				redisLog.Warningf("command execute failed [%d, %s], err: %s",
					i, c.catGenericCommand(), err.Error())
			} else {
				if reply, err = conn.Do(c.cmd, cmdArgs...); err != nil {
					conn.Close()
					redisLog.Warningf("command execute failed [%d, %s], err: %s", i,
						c.catGenericCommand(), err.Error())
				} else {
					WriteValue(resp, reply)
					conn.Close()
					return nil
				}
			}
		}

		return err
	}

	for _, cmd := range supportedCommands {
		router.Register(cmd, commandExec)
	}
}

func (proxy *CodisProxy) GetStats() stats.ModuleStats {
	return proxy.ModuleStats
}

func (proxy *CodisProxy) Stop() {
	proxy.cluster.Close()
}

func (proxy *CodisProxy) CheckUsedAsKVDSModule() bool {
	return true
}

func (proxy *CodisProxy) SetUsedAsKVDSModule() error {
	return nil
}

func codisKey2Table(ck []byte) string {
	if parts := bytes.Split(ck, []byte(":")); len(parts) >= 2 {
		return string(parts[0])
	} else {
		return "other"
	}
}
