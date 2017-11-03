package redisproxy

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/absolute8511/proxymodule/redisproxy/stats"
	"github.com/absolute8511/redigo/redis"
	"github.com/bluele/gcache"
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
	conf       *CodisProxyConf
	cluster    *CodisCluster
	localCache *PrefixLocalCache
}

func init() {
	RegisterRedisProxyModule("codis-proxy", CreateCodisProxy)

	supportedCommands = []string{
		"mget", "setnx", "del",
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
	return &CodisProxy{
		localCache: NewPrefixLocalCache(),
	}
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

func (proxy *CodisProxy) getCommand(c *Client, resp ResponseWriter) error {
	if len(c.Args) < 1 {
		return ErrCmdParams
	}

	proxy.UpdateStats(c.cmd, codisKey2Table(c.Args[0]), 1)

	key := string(c.Args[0])

	//Get value from local-cache first.
	value, needCache := proxy.localCache.Get(key)
	if value != nil {
		resp.WriteBulk(value)
		return nil
	}

	var reply interface{}
	var err error
	var conn redis.Conn

	maxRetry := int(len(proxy.conf.ServerList)/2) + 1
	if maxRetry < 3 {
		maxRetry = 3
	}

	for i := 0; i < maxRetry; i++ {
		if conn, err = proxy.cluster.GetConn(); err != nil {
			redisLog.Warningf("command execute failed [%d, %s], err: %s",
				i, c.catGenericCommand(), err.Error())
			continue
		}
		if reply, err = conn.Do(c.cmd, c.Args[0]); err != nil {
			conn.Close()
			redisLog.Warningf("command execute failed [%d, %s], err: %s", i,
				c.catGenericCommand(), err.Error())
			continue
		}

		WriteValue(resp, reply)
		conn.Close()
		if needCache {
			if v, ok := reply.([]byte); ok {
				proxy.localCache.Set(key, v)
			}
		}
		return nil
	}

	return err
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

	router.Register("get", proxy.getCommand)
	router.Register("addcache", proxy.addCacheCmd)
	router.Register("delcache", proxy.delCacheCmd)
	router.Register("cachepruge", proxy.cachePrugeCmd)
	router.Register("cacheinfo", proxy.cacheInfoCmd)
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

type localCache struct {
	gcache.Cache
	Threshold int
}

type PrefixLocalCache struct {
	sync.RWMutex
	cache map[string]*localCache
}

func NewPrefixLocalCache() *PrefixLocalCache {
	return &PrefixLocalCache{
		cache: make(map[string]*localCache),
	}
}

func (c *PrefixLocalCache) Set(key string, value []byte) error {
	var lc *localCache
	c.RLock()
	for prefix, v := range c.cache {
		if strings.HasPrefix(key, prefix) {
			lc = v
			break
		}
	}
	c.RUnlock()
	if lc != nil {
		if len(value) > lc.Threshold {
			return lc.Set(key, value)
		}
	}

	return nil
}

func (c *PrefixLocalCache) Get(key string) ([]byte, bool) {
	var lc *localCache
	c.RLock()
	for prefix, v := range c.cache {
		if strings.HasPrefix(key, prefix) {
			lc = v
			break
		}
	}
	c.RUnlock()

	if lc != nil {
		if v, err := lc.Get(key); err != nil {
			return nil, true
		} else {
			return v.([]byte), true
		}
	}

	return nil, false
}

func (c *PrefixLocalCache) AddCache(prefix string, ttl time.Duration, size int,
	threshold int) error {
	c.Lock()
	if _, exists := c.cache[prefix]; exists {
		c.Unlock()
		return fmt.Errorf("local cache for prefix:%s already exist", prefix)
	}

	lc := &localCache{
		Cache:     gcache.New(size).Expiration(ttl).ARC().Build(),
		Threshold: threshold,
	}

	c.cache[prefix] = lc
	c.Unlock()

	return nil
}

func (c *PrefixLocalCache) DelCache(prefix string) bool {
	defer c.Unlock()
	c.Lock()
	if _, exists := c.cache[prefix]; exists {
		delete(c.cache, prefix)
		return true
	} else {
		return false
	}
}

func (c *PrefixLocalCache) Purge(prefix string) bool {
	c.RLock()
	lc, exists := c.cache[prefix]
	c.RUnlock()

	if exists {
		lc.Purge()
		return true
	} else {
		return false
	}
}

func (c *PrefixLocalCache) Info() []byte {
	buf := new(bytes.Buffer)
	c.RLock()
	for prefix, lc := range c.cache {
		buf.WriteString(fmt.Sprintf("[%s]: %d, Threshold:%dByte, HitCount:%d, MissCount:%d, HitRate:%f", prefix,
			lc.Len(), lc.Threshold, lc.HitCount(), lc.MissCount(), lc.HitRate()))
	}
	c.RUnlock()

	return buf.Bytes()
}

// addcache prefix 50 B/KB/MB size ttl s/ms
func (proxy *CodisProxy) addCacheCmd(c *Client, resp ResponseWriter) error {
	if len(c.Args) != 6 {
		return ErrCmdParams
	}

	prefix := string(c.Args[0])
	threshold, err := strconv.Atoi(string(c.Args[1]))
	if err != nil || threshold < 0 {
		return ErrCmdParams
	}

	switch string(c.Args[2]) {
	case "MB":
		threshold *= 1024 * 1024
	case "KB":
		threshold *= 1024
	case "B":
	default:
		return ErrCmdParams
	}

	size, err := strconv.Atoi(string(c.Args[3]))
	if err != nil || threshold < 0 {
		return ErrCmdParams
	}

	ttl, err := strconv.Atoi(string(c.Args[4]))
	if err != nil || ttl < 0 {
		return ErrCmdParams
	}

	switch string(c.Args[5]) {
	case "ms":
	case "s":
		ttl *= 1000
	default:
		return ErrCmdParams
	}

	if err := proxy.localCache.AddCache(prefix, time.Duration(ttl)*time.Millisecond,
		size, threshold); err != nil {
		return err
	} else {
		resp.WriteString("OK")
		return nil
	}

}

// delcache prefix
func (proxy *CodisProxy) delCacheCmd(c *Client, resp ResponseWriter) error {
	if len(c.Args) != 1 {
		return ErrCmdParams
	}

	if proxy.localCache.DelCache(string(c.Args[0])) {
		resp.WriteInteger(1)
	} else {
		resp.WriteInteger(0)
	}

	return nil
}

// cachepruge prefix
func (proxy *CodisProxy) cachePrugeCmd(c *Client, resp ResponseWriter) error {
	if len(c.Args) != 1 {
		return ErrCmdParams
	}

	if proxy.localCache.Purge(string(c.Args[0])) {
		resp.WriteInteger(1)
	} else {
		resp.WriteInteger(0)
	}

	return nil
}

// cacheinfo
func (proxy *CodisProxy) cacheInfoCmd(c *Client, resp ResponseWriter) error {
	resp.WriteBulk(proxy.localCache.Info())
	return nil
}
