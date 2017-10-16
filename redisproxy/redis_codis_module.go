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

type CodisServer struct {
	ServerAddr string
}

type CodisProxyConf struct {
	TendInterval  int64
	ReadTimeout   time.Duration
	WriteTimeout  time.Duration
	DialTimeout   time.Duration
	MaxActiveConn int
	MaxIdleConn   int
	IdleTimeout   int64

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
	proxy.conf = &CodisProxyConf{
		DialTimeout:   defaultDialTimeout,
		IdleTimeout:   defaultIdleTimeout,
		MaxActiveConn: defaultMaxActiveConn,
		MaxIdleConn:   defaultMaxIdleConn,
		ReadTimeout:   defaultReadTimeout,
		TendInterval:  defaultTendInterval,
		WriteTimeout:  defaultWriteTimeout,
	}
	if err := loadConfig(proxy.conf); err != nil {
		return err
	}

	proxy.cluster = NewCodisCluster(proxy.conf)
	proxy.ModuleStats = stats.NewProxyModuleStats()

	return nil
}

func (proxy *CodisProxy) RegisterCmd(router *CmdRouter) {
	maxRetry := int(len(proxy.conf.ServerList)/2) + 1
	if maxRetry < 3 {
		maxRetry = 3
	}

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

func codisKey2Table(ck []byte) string {
	if parts := bytes.Split(ck, []byte(":")); len(parts) >= 2 {
		return string(parts[0])
	} else {
		return "other"
	}
}
