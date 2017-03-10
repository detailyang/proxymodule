package redisproxy

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absolute8511/proxymodule/common"
)

var (
	supportedCommands []string
)

const (
	defaultTendInterval = 3

	cpMonitorApp      = "tether"
	cpMonitorBusiness = "tether.codis-proxy"
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
	conf             *CodisProxyConf
	cluster          *CodisCluster
	statisticsModule *CodisStatisticsModule
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
		statisticsModule: NewCodisStatisticsModule(),
	}
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

	proxy.cluster = NewCodisCluster(proxy.conf.ServerList, proxy.conf.TendInterval)

	return nil
}

func (proxy *CodisProxy) RegisterCmd(router *CmdRouter) {
	maxRetry := int(len(proxy.conf.ServerList)/2) + 1

	commandExec := func(c *Client, resp ResponseWriter) error {

		cmdArgs := make([]interface{}, len(c.Args))
		for i, v := range c.Args {
			cmdArgs[i] = v
		}

		proxy.statisticsModule.Sampling(c.cmd)

		var reply interface{}
		var err error

		for i := 0; i < maxRetry; i++ {
			conn, err := proxy.cluster.GetConn()
			if err != nil {
				redisLog.Warningf("command execute failed [%d, %s]", i, c.catGenericCommand())
			} else {
				if reply, err = conn.Do(c.cmd, cmdArgs...); err != nil {
					conn.Close()
					redisLog.Warningf("command execute failed [%d, %s]", i, c.catGenericCommand())
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

func (proxy *CodisProxy) GetStatisticsModule() ProxyStatisticsModule {
	return proxy.statisticsModule
}

func (proxy *CodisProxy) Stop() {
	proxy.cluster.Close()
}

func NewCodisStatisticsModule() *CodisStatisticsModule {
	return &CodisStatisticsModule{
		statisticsData: make(map[string]uint64),
		swapData:       make(map[string]uint64),
		slowOperation:  make(map[string]uint64),
	}
}

type CodisStatisticsModule struct {
	sync.Mutex
	statisticsData map[string]uint64
	swapData       map[string]uint64

	accumulatedOpTime int64
	failedOperation   uint64
	slowOperation     map[string]uint64
}

func (self *CodisStatisticsModule) IncrOpTime(cost int64) {
	atomic.AddInt64(&self.accumulatedOpTime, cost)
}

func (self *CodisStatisticsModule) IncrFailedOperation() {
	atomic.AddUint64(&self.failedOperation, 1)
}

func (self *CodisStatisticsModule) IncrSlowOperation(cost time.Duration) {
	defer self.Mutex.Unlock()
	self.Mutex.Lock()

	switch {
	case cost > 20*time.Millisecond:
		self.slowOperation["20ms"] += 1
		fallthrough
	case cost > 10*time.Millisecond:
		self.slowOperation["10ms"] += 1
		fallthrough
	case cost > 5*time.Millisecond:
		self.slowOperation["5ms"] += 1
		fallthrough
	case cost > 2*time.Millisecond:
		self.slowOperation["2ms"] += 1
	default:
		return
	}
}

func (self *CodisStatisticsModule) Sampling(Cmd string) {
	self.Mutex.Lock()
	self.statisticsData[Cmd] += 1
	self.Mutex.Unlock()
}

func (self *CodisStatisticsModule) GenMonitorData() []byte {
	self.Mutex.Lock()
	tmp := self.statisticsData
	self.statisticsData = self.swapData
	self.swapData = tmp
	self.Mutex.Unlock()

	monitorData := make([]*common.MonitorData, 0, len(self.swapData)+1)

	var total uint64
	for operation, count := range self.swapData {
		opSample := common.NewMonitorData(cpMonitorApp, cpMonitorBusiness)
		opSample.Metrics["OpCount"] = count
		opSample.Tags["Operation"] = operation

		monitorData = append(monitorData, opSample)
		total += count

		//clear the already stored data
		self.swapData[operation] = 0
	}

	overall := common.NewMonitorData(cpMonitorApp, cpMonitorBusiness)
	overall.Metrics["Total"] = total

	costTime := uint64(atomic.SwapInt64(&self.accumulatedOpTime, 0)) / 1000
	var avgCost uint64
	if total != 0 {
		avgCost = uint64(costTime / total)
	}
	overall.Metrics["Avg"] = avgCost

	overall.Metrics["Failed"] = atomic.SwapUint64(&self.failedOperation, 0)

	monitorData = append(monitorData, overall)

	self.Mutex.Lock()
	for datum, count := range self.slowOperation {
		slowSample := common.NewMonitorData(cpMonitorApp, cpMonitorBusiness)
		slowSample.Metrics["SlowCount"] = count
		slowSample.Tags["datum"] = datum
		monitorData = append(monitorData, slowSample)

		//clear the already stored data
		self.slowOperation[datum] = 0
	}
	self.Mutex.Unlock()

	if monitorBytes, err := json.Marshal(monitorData); err != nil {
		return nil
	} else {
		return monitorBytes
	}
}
