package redisproxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/absolute8511/proxymodule/common"
	as "github.com/aerospike/aerospike-client-go"
)

const (
	asMonitorBinuess  = "tether.kvproxy"
	asMonitorApp      = "kvstore"
	asMonitorCategory = "category"
)

func NewAerospikeProxyStatistics() *AerospikeProxyStatistics {
	statisticsModule := &AerospikeProxyStatistics{
		statisticsData: make(map[string]map[statisticsUnit]uint64),
		slowOperation: map[string]uint64{
			"2ms": 0, "5ms": 0, "10ms": 0, "20ms": 0,
		},
	}

	statisticsModule.genMonitorData = wrapGenMonitorData(statisticsModule)

	return statisticsModule
}

type statisticsUnit struct {
	namespace string
	set       string
}

type AerospikeProxyStatistics struct {
	sync.Mutex
	statisticsData map[string]map[statisticsUnit]uint64

	accumulatedOpTime int64
	failedOperation   uint64
	slowOperation     map[string]uint64

	genMonitorData func() []byte
}

func (self *AerospikeProxyStatistics) GenInfoBytes() []byte {
	var buffer bytes.Buffer
	var slowBuffer bytes.Buffer
	var total uint64

	buffer.WriteString("#Statistic\r\n")

	self.Mutex.Lock()
	for cmd, data := range self.statisticsData {
		var sum uint64
		for unit, count := range data {
			sum += count
			buffer.WriteString(fmt.Sprintf("%s %s,%s:%d\r\n",
				cmd, unit.namespace, unit.set, count))
		}
		buffer.WriteString(fmt.Sprintf("%s:%d\r\n", cmd, sum))
		total += sum
	}
	for cost, count := range self.slowOperation {
		slowBuffer.WriteString(fmt.Sprintf("%s slow operation:%d\r\n", cost, count))
	}
	self.Mutex.Unlock()

	buffer.WriteString(fmt.Sprintf("total operation:%d\r\n", total))

	buffer.WriteString(fmt.Sprintf("failed operation:%d\r\n",
		atomic.LoadUint64(&self.failedOperation)))

	buffer.Write(slowBuffer.Bytes())

	buffer.WriteString(fmt.Sprintf("accumulated time:%dus\r\n",
		atomic.LoadInt64(&self.accumulatedOpTime)/1000))

	return buffer.Bytes()
}

func (self *AerospikeProxyStatistics) IncrFailedOperation() {
	atomic.AddUint64(&self.failedOperation, 1)
}

func (self *AerospikeProxyStatistics) IncrSlowOperation(cost time.Duration) {
	self.Mutex.Lock()
	defer self.Mutex.Unlock()

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

func (self *AerospikeProxyStatistics) IncrOpTime(duration int64) {
	atomic.AddInt64(&self.accumulatedOpTime, duration)
}

func (self *AerospikeProxyStatistics) Statistic(Cmd string, key *as.Key, args [][]byte) {
	self.Mutex.Lock()
	defer self.Mutex.Unlock()

	units := []statisticsUnit{
		statisticsUnit{
			namespace: key.Namespace(),
			set:       key.SetName(),
		},
	}

	if Cmd == "mget" || Cmd == "del" {
		for _, arg := range args {
			keyEx, err := parserRedisKey(string(arg))
			if err != nil {
				return
			} else {
				units = append(units, statisticsUnit{
					namespace: keyEx.Namespace(),
					set:       keyEx.SetName(),
				})
			}
		}
	}

	for _, unit := range units {
		if data, ok := self.statisticsData[Cmd]; ok {
			if _, ok := data[unit]; ok {
				self.statisticsData[Cmd][unit] += 1
			} else {
				self.statisticsData[Cmd][unit] = 1
			}
		} else {
			self.statisticsData[Cmd] = make(map[statisticsUnit]uint64)
			self.statisticsData[Cmd][unit] = 1
		}
	}
}

func (self *AerospikeProxyStatistics) GenMonitorData() []byte {
	return self.genMonitorData()
}

/*
current metrics command:
get, del, ttl, exists, mget, set ,setex, expire
*/
func wrapGenMonitorData(proxy *AerospikeProxyStatistics) (f func() []byte) {

	supportedCommands := []string{
		"get", "del", "set", "setex", "exists", "mget", "expire",
		"ttl", "hget", "hgetall", "hmget", "hmset", "hset", "hdel", "hexists",
		"info", "incr", "incrby", "hincrby", "decr", "decrby"}

	metricsCmd := map[string]struct{}{
		"get":     struct{}{},
		"del":     struct{}{},
		"ttl":     struct{}{},
		"exists":  struct{}{},
		"mget":    struct{}{},
		"set":     struct{}{},
		"setex":   struct{}{},
		"expire":  struct{}{},
		"hget":    struct{}{},
		"hset":    struct{}{},
		"incr":    struct{}{},
		"incrby":  struct{}{},
		"hincrby": struct{}{},
		"decr":    struct{}{},
		"decrby":  struct{}{},
	}

	snapshot := struct {
		statisticsData    map[string]map[statisticsUnit]uint64
		accumulatedOpTime int64
		failedOperation   uint64
		slowOperation     map[string]uint64
	}{
		make(map[string]map[statisticsUnit]uint64),
		0, 0, make(map[string]uint64),
	}

	rawMonitorData := make(map[string]map[statisticsUnit]uint64)
	rawSlowOperation := make(map[string]uint64)

	for _, cmd := range supportedCommands {
		snapshot.statisticsData[cmd] = make(map[statisticsUnit]uint64)
		rawMonitorData[cmd] = make(map[statisticsUnit]uint64)
	}

	f = func() []byte {

		failedOperation := atomic.LoadUint64(&proxy.failedOperation)
		accumulatedOpTime := atomic.LoadInt64(&proxy.accumulatedOpTime)
		var total uint64

		proxy.Mutex.Lock()
		for cmd, data := range proxy.statisticsData {
			if _, ok := snapshot.statisticsData[cmd]; ok {
				for unit, count := range data {
					if _, b := snapshot.statisticsData[cmd][unit]; b {
						rawMonitorData[cmd][unit] = count - snapshot.statisticsData[cmd][unit]
					} else {
						rawMonitorData[cmd][unit] = count
					}
					snapshot.statisticsData[cmd][unit] = count
					total += rawMonitorData[cmd][unit]
				}
			} else {
				redisLog.Errorf("execute unsupported command, %s", cmd)
			}
		}
		for k, v := range proxy.slowOperation {
			rawSlowOperation[k] = v
		}
		proxy.Mutex.Unlock()

		integratedData := make(map[string]map[string]uint64)

		for cmd, rawData := range rawMonitorData {
			if _, ok := metricsCmd[cmd]; ok {
				for unit, count := range rawData {
					category := statisticsUnit2Category(unit)
					if _, ok := integratedData[category]; ok {
						integratedData[category][cmd] = count
					} else {
						integratedData[category] = make(map[string]uint64)
						integratedData[category][cmd] = count
					}
				}
			}
		}

		var proxyMonitorData []*MonitorData

		for category, metrics := range integratedData {
			monitorSample := NewMonitorData(asMonitorApp, asMonitorBinuess)
			monitorSample.Tags[asMonitorCategory] = category

			for cmd, count := range metrics {
				monitorSample.Metrics[cmd] = count
			}
			proxyMonitorData = append(proxyMonitorData, monitorSample)

		}

		for cost, count := range rawSlowOperation {
			monitorSample := NewMonitorData(asMonitorApp, asMonitorBinuess)
			monitorSample.Tags[asMonitorCategory] = cost

			monitorSample.Metrics["Slow"] = count - snapshot.slowOperation[cost]
			snapshot.slowOperation[cost] = count
			proxyMonitorData = append(proxyMonitorData, monitorSample)
		}

		//the overall monitor data
		overViewData := NewMonitorData(asMonitorApp, asMonitorBinuess)
		overViewData.Metrics["Failed"] = failedOperation - snapshot.failedOperation
		snapshot.failedOperation = failedOperation
		overViewData.Metrics["CostTime"] = uint64(accumulatedOpTime-snapshot.accumulatedOpTime) / 1000
		snapshot.accumulatedOpTime = accumulatedOpTime
		overViewData.Metrics["Total"] = total
		if total == 0 {
			overViewData.Metrics["Avg"] = 0
		} else {
			overViewData.Metrics["Avg"] = overViewData.Metrics["CostTime"] / total
		}

		//overViewData.Tags[asMonitorCategory] = "OverView"

		proxyMonitorData = append(proxyMonitorData, overViewData)

		if encodedData, err := json.Marshal(proxyMonitorData); err != nil {
			return nil
		} else {
			return encodedData
		}
	}

	return f
}

func statisticsUnit2Category(unit statisticsUnit) string {
	return unit.namespace + ":" + unit.set
}
