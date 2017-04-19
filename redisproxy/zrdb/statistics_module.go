package zrdb

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absolute8511/proxymodule/common"
)

const (
	DefaultMonitorApp      = "zankv"
	DefaultMonitorBusiness = "ZanRedisDB"
)

func NewStatisticsModule(app, business string) *StatisticsModule {
	module := &StatisticsModule{
		monitorApp:      app,
		monitorBusiness: business,
		statisticsData:  make(map[string]uint64),
		swapData:        make(map[string]uint64),
		slowOperation:   make(map[string]uint64),
	}

	return module
}

type StatisticsModule struct {
	monitorApp      string
	monitorBusiness string

	sync.Mutex
	statisticsData map[string]uint64
	swapData       map[string]uint64

	accumulatedOpTime int64
	failedOperation   uint64
	slowOperation     map[string]uint64
}

func (self *StatisticsModule) IncrOpTime(cost int64) {
	atomic.AddInt64(&self.accumulatedOpTime, cost)
}

func (self *StatisticsModule) IncrFailedOperation() {
	atomic.AddUint64(&self.failedOperation, 1)
}

func (self *StatisticsModule) IncrSlowOperation(cost time.Duration) {
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

func (self *StatisticsModule) Sampling(Cmd string) {
	self.Mutex.Lock()
	self.statisticsData[Cmd] += 1
	self.Mutex.Unlock()
}

func (self *StatisticsModule) GenMonitorData() []byte {
	self.Mutex.Lock()
	tmp := self.statisticsData
	self.statisticsData = self.swapData
	self.swapData = tmp
	self.Mutex.Unlock()

	monitorData := make([]*common.MonitorData, 0, len(self.swapData)+1)

	var total uint64
	for operation, count := range self.swapData {
		opSample := common.NewMonitorData(self.monitorApp, self.monitorBusiness)
		opSample.Metrics["OpCount"] = count
		opSample.Tags["Operation"] = operation

		monitorData = append(monitorData, opSample)
		total += count

		//clear the already stored data
		self.swapData[operation] = 0
	}

	overall := common.NewMonitorData(self.monitorApp, self.monitorBusiness)
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
		slowSample := common.NewMonitorData(self.monitorApp, self.monitorBusiness)
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
