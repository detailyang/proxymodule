package redisproxy

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/garyburd/redigo/redis"
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
	ServerList   []CodisServer
}

type CodisProxy struct {
	sync.Mutex
	conf           *CodisProxyConf
	connPool       *redis.Pool
	ServerList     []CodisServer
	quitC          chan struct{}
	wg             sync.WaitGroup
	tendServerList []CodisServer //buffer used in method 'tendServer'
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
	return &CodisProxy{
		quitC: make(chan struct{}),
	}
}

func (proxy *CodisProxy) GetProxyName() string {
	return "codis-proxy"
}

func (proxy *CodisProxy) InitConf(loadConfig func(v interface{}) error) error {
	proxy.conf = &CodisProxyConf{}
	if err := loadConfig(proxy.conf); err != nil {
		return err
	}

	proxy.ServerList = make([]CodisServer, 0, len(proxy.conf.ServerList))
	proxy.tendServerList = make([]CodisServer, 0, len(proxy.conf.ServerList))

	//check the state of servers at init
	proxy.tendServers()
	if len(proxy.ServerList) == 0 {
		redisLog.Errorf("no server is available at codis proxy start, %v", proxy.conf.ServerList)
	}

	var rotate int64
	dialF := func() (redis.Conn, error) {

		proxy.Mutex.Lock()
		servLen := len(proxy.ServerList)
		if servLen == 0 {
			proxy.Mutex.Unlock()
			return nil, errors.New("no server is available right now")
		}
		picked := (atomic.AddInt64(&rotate, 1) % int64(servLen))
		s := proxy.ServerList[picked]
		proxy.Mutex.Unlock()

		return redis.Dial("tcp", s.ServerAddr)

	}

	testF := func(c redis.Conn, t time.Time) (err error) {
		if time.Since(t) > 60*time.Second {
			_, err = c.Do("PING")
		}
		return
	}

	proxy.connPool = &redis.Pool{
		MaxIdle:      256,
		MaxActive:    512,
		IdleTimeout:  120 * time.Second,
		Dial:         dialF,
		TestOnBorrow: testF,
	}

	proxy.wg.Add(1)

	go func() {
		defer proxy.wg.Done()
		proxy.tend()
	}()

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

func (proxy *CodisProxy) tend() {
	if proxy.conf.TendInterval == 0 {
		proxy.conf.TendInterval = defaultTendInterval
	}

	tendTicker := time.NewTicker(time.Duration(proxy.conf.TendInterval) * time.Second)
	defer tendTicker.Stop()

	for {
		select {
		case <-tendTicker.C:
			proxy.tendServers()
		case <-proxy.quitC:
			redisLog.Debugf("tend routine for codis proxy exit")
			return
		}
	}
}

func (proxy *CodisProxy) tendServers() {
	for _, s := range proxy.conf.ServerList {
		conn, err := redis.Dial("tcp", s.ServerAddr)
		if err != nil {
			redisLog.Warningf("dial to codis server failed, disable the address: %s, err: %s", s, err.Error())
		} else {
			if _, err = conn.Do("PING"); err != nil {
				redisLog.Warningf("ping codis server failed, disable the address: %s, err: %s", s, err.Error())
			} else {
				redisLog.Debugf("codis server: %v is available", s)
				proxy.tendServerList = append(proxy.tendServerList, s)
			}
			conn.Close()
		}
	}

	if len(proxy.tendServerList) <= 0 {
		redisLog.Errorf("no server node in serviceable state")
	} else {
		proxy.Mutex.Lock()
		tmp := proxy.ServerList
		proxy.ServerList = proxy.tendServerList
		proxy.tendServerList = tmp[:0]
		proxy.Mutex.Unlock()
	}
}

func (proxy *CodisProxy) Stop() {
	close(proxy.quitC)
	proxy.connPool.Close()
	proxy.wg.Wait()
}
