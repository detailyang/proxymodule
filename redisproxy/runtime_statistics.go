package redisproxy

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"

	as "github.com/aerospike/aerospike-client-go"
)

var gProxyRunTimeStatistics *ProxyRunTimeStatistics

func init() {
	gProxyRunTimeStatistics = newProxyRunTimeStatistics()
}

func newProxyRunTimeStatistics() *ProxyRunTimeStatistics {
	return &ProxyRunTimeStatistics{
		statisticsData: make(map[string]map[statisticsUnit]uint64),
	}
}

type statisticsUnit struct {
	namespace string
	set       string
}

type ProxyRunTimeStatistics struct {
	sync.Mutex
	statisticsData map[string]map[statisticsUnit]uint64

	accumulatedOpTime int64
	failedOperation   uint64
	slowOperation     uint64
}

func (self *ProxyRunTimeStatistics) GenInfoBytes() []byte {
	var buffer bytes.Buffer
	var total uint64

	buffer.WriteString("#Statistic\r\n")

	self.Mutex.Lock()
	for cmd, data := range self.statisticsData {
		var sum uint64
		for unit, count := range data {
			sum += count
			buffer.WriteString(fmt.Sprintf("%s %s,%s:%d\r\n",
				cmd, unit.set, unit.namespace, count))
		}
		buffer.WriteString(fmt.Sprintf("%s:%d\r\n", cmd, sum))
		total += sum
	}
	self.Mutex.Unlock()

	buffer.WriteString(fmt.Sprintf("total operation:%d\r\n", total))

	buffer.WriteString(fmt.Sprintf("failed operation:%d\r\n",
		atomic.LoadUint64(&self.failedOperation)))

	buffer.WriteString(fmt.Sprintf("slow operation:%d\r\n",
		atomic.LoadUint64(&self.slowOperation)))

	buffer.WriteString(fmt.Sprintf("accumulated time:%dms\r\n",
		atomic.LoadInt64(&self.accumulatedOpTime)))

	return buffer.Bytes()
}

func (self *ProxyRunTimeStatistics) IncrFailedOperation() {
	atomic.AddUint64(&self.failedOperation, 1)
}

func (self *ProxyRunTimeStatistics) IncrSlowOperation() {
	atomic.AddUint64(&self.slowOperation, 1)
}

func (self *ProxyRunTimeStatistics) IncrOpTime(duration int64) {
	atomic.AddInt64(&self.accumulatedOpTime, duration)
}

func (self *ProxyRunTimeStatistics) Statistic(Cmd string, key *as.Key, args [][]byte) {
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
