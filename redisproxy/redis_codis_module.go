package redisproxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/absolute8511/proxymodule/common"
	"github.com/absolute8511/proxymodule/redisproxy/stats"
	"github.com/absolute8511/redigo/redis"
	"github.com/bluele/gcache"
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
}

func CreateCodisProxy() RedisProxyModule {
	return &CodisProxy{
		localCache: NewPrefixLocalCache(),
	}
}

func (proxy *CodisProxy) GetProxyName() string {
	return "codis-proxy"
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

func (proxy *CodisProxy) readCommand(c *Client, resp ResponseWriter) error {
	if len(c.Args) > 0 {
		proxy.UpdateStats(c.cmd, codisKey2Table(c.Args[0]), 1)
	}

	var needCache bool = false
	var value interface{}
	var cacheKey string
	if c.cmd == "get" {
		//Try to get value from local-cache first.
		cacheKey = string(c.catGenericCommand())
		value, needCache = proxy.localCache.Get(cacheKey)
		if value != nil {
			WriteValue(resp, value)
			return nil
		}
	}

	cmdArgs := make([]interface{}, len(c.Args))
	for i, v := range c.Args {
		cmdArgs[i] = v
	}

	reply, err := proxy.doCommand(c.cmd, cmdArgs...)
	if err == nil {
		WriteValue(resp, reply)
		if needCache {
			var size int = 1
			if c.cmd == "get" {
				if b, err := redis.Bytes(reply, nil); err == nil {
					size = len(b)
				}
			}
			proxy.localCache.Set(cacheKey, reply, size)
		}
	}
	return err
}

func (proxy *CodisProxy) writeCommand(c *Client, resp ResponseWriter) error {
	if len(c.Args) < 1 {
		return ErrCmdParams
	}

	proxy.UpdateStats(c.cmd, codisKey2Table(c.Args[0]), 1)

	cmdArgs := make([]interface{}, len(c.Args))
	for i, v := range c.Args {
		cmdArgs[i] = v
	}

	if reply, err := proxy.doCommand(c.cmd, cmdArgs...); err == nil {
		WriteValue(resp, reply)
		return nil
	} else {
		return err
	}
}

func (proxy *CodisProxy) doCommand(cmd string, cmdArgs ...interface{}) (interface{}, error) {
	maxRetry := int(len(proxy.conf.ServerList)/2) + 1
	if maxRetry < 3 {
		maxRetry = 3
	}

	var reply interface{}
	var err error
	var conn redis.Conn

	for i := 0; i < maxRetry; i++ {
		conn, err = proxy.cluster.GetConn()
		if err != nil {
			redisLog.Warningf("command execute failed [%d, %s], err: %s",
				i, cmd, err.Error())
			continue
		}

		if reply, err = conn.Do(cmd, cmdArgs...); err != nil {
			conn.Close()
			redisLog.Warningf("command execute failed [%d, %s], err: %s",
				i, cmd, err.Error())
		} else {
			conn.Close()
			return reply, nil
		}
	}
	return nil, err
}

func (proxy *CodisProxy) RegisterCmd(router *CmdRouter) {
	for readCmd, _ := range common.ReadCommands {
		router.Register(readCmd, proxy.readCommand)
	}

	for writeCmd, _ := range common.WriteCommands {
		router.Register(writeCmd, proxy.writeCommand)
	}

	router.Register("eval", proxy.writeCommand)
	router.Register("info", proxy.readCommand)
	router.Register("stats", proxy.statsCommand)

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
		return "others"
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

func (c *PrefixLocalCache) Set(key string, value interface{}, size int) error {
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
		if size > lc.Threshold {
			return lc.Set(key, value)
		}
	}

	return nil
}

func (c *PrefixLocalCache) Get(key string) (interface{}, bool) {
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
			return v, true
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
		buf.WriteString(fmt.Sprintf("[%s]: %d, Threshold:%dByte, HitCount:%d, MissCount:%d, HitRate:%f; ", prefix,
			lc.Len(), lc.Threshold, lc.HitCount(), lc.MissCount(), lc.HitRate()))
	}
	c.RUnlock()

	return buf.Bytes()
}

// addcache cachecmd 50 B/KB/MB size ttl s/ms arg0 args1
func (proxy *CodisProxy) addCacheCmd(c *Client, resp ResponseWriter) error {
	if len(c.Args) != 7 {
		return ErrCmdParams
	}

	/*
		if _, ok := common.ReadCommands[string(c.Args[0])]; !ok {
			return errors.New("Can only cache read commands.")
		}
	*/
	if string(c.Args[0]) != "get" {
		return errors.New("Can only cache 'get' commands.")
	}

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

	b := make([][]byte, 1+len(c.Args[6:]))
	b[0] = c.Args[0]
	copy(b[1:], c.Args[6:])
	prefix := string(bytes.Join(b, []byte(" ")))

	if err := proxy.localCache.AddCache(prefix, time.Duration(ttl)*time.Millisecond,
		size, threshold); err != nil {
		return err
	} else {
		resp.WriteString("OK")
		return nil
	}

}

// delcache command arg0 arg1 ...
func (proxy *CodisProxy) delCacheCmd(c *Client, resp ResponseWriter) error {
	if len(c.Args) < 1 {
		return ErrCmdParams
	}

	cacheKey := string(bytes.Join(c.Args, []byte(" ")))
	if proxy.localCache.DelCache(cacheKey) {
		resp.WriteInteger(1)
	} else {
		resp.WriteInteger(0)
	}

	return nil
}

// cachepruge command arg0 arg1 ...
func (proxy *CodisProxy) cachePrugeCmd(c *Client, resp ResponseWriter) error {
	if len(c.Args) < 1 {
		return ErrCmdParams
	}

	cacheKey := string(bytes.Join(c.Args, []byte(" ")))
	if proxy.localCache.Purge(cacheKey) {
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

// stats [json]
func (proxy *CodisProxy) statsCommand(c *Client, resp ResponseWriter) error {
	if len(c.Args) >= 1 && strings.ToLower(string(c.Args[0])) == "json" {
		stats, err := json.Marshal(proxy.ModuleStats.GetStatsData())
		if err != nil {
			return err
		}
		resp.WriteBulk(stats)
	} else {
		resp.WriteBulk([]byte(proxy.ModuleStats.String()))
	}
	return nil
}
