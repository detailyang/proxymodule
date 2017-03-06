package redisproxy

import (
	"sync/atomic"
	"time"

	"github.com/garyburd/redigo/redis"
)

var (
	supportedCommands []string
)

type CodisServer struct {
	ServerAddr string
}

type CodisProxyConf struct {
	ServerList []CodisServer
}

type CodisProxy struct {
	conf     *CodisProxyConf
	connPool *redis.Pool
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
		"zcount", "info",
	}
}

func CreateCodisProxy() RedisProxyModule {
	return &CodisProxy{}
}

func (proxy *CodisProxy) GetProxyName() string {
	return "codis-proxy"
}

func (proxy *CodisProxy) InitConf(loadConfig func(v interface{}) error) error {
	proxy.conf = &CodisProxyConf{}
	if err := loadConfig(proxy.conf); err != nil {
		return err
	}

	var rotate int64
	dialF := func() (redis.Conn, error) {
		picked := (atomic.AddInt64(&rotate, 1)) % int64(len(proxy.conf.ServerList))

		s := proxy.conf.ServerList[picked]
		return redis.Dial("tcp", s.ServerAddr)
	}

	proxy.connPool = &redis.Pool{
		MaxIdle:     256,
		MaxActive:   512,
		IdleTimeout: 120 * time.Second,
		Dial:        dialF,
	}

	return nil
}

func (proxy *CodisProxy) RegisterCmd(router *CmdRouter) {
	commandExec := func(c *Client, resp ResponseWriter) error {

		conn := proxy.connPool.Get()
		defer conn.Close()

		cmdArgs := make([]interface{}, len(c.Args))
		for i, v := range c.Args {
			cmdArgs[i] = v
		}

		if reply, err := conn.Do(c.cmd, cmdArgs...); err != nil {
			return err
		} else {
			WriteValue(resp, reply)
			return nil
		}
	}

	for _, cmd := range supportedCommands {
		router.Register(cmd, commandExec)
	}
}

func (proxy *CodisProxy) GetStatisticsModule() ProxyStatisticsModule {
	return nil
}

func (proxy *CodisProxy) Stop() {
	proxy.connPool.Close()
}
