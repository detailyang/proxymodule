package stats

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absolute8511/proxymodule/common"
)

type ModuleStats interface {
	IncrFailed()
	IncrCost(time.Duration)
	UpdateSlowStats(time.Duration)
	UpdateStats(string, string, uint64)
	String() string
	GetStatsData() interface{}
}

const (
	statsStringHeader = "#Statistic\r\n"
)

type TableStats struct {
	sync.RWMutex
	data  map[string]*uint64
	total uint64
}

func (s *TableStats) IncrBy(t string, deta uint64) {
	s.RLock()
	v, ok := s.data[t]
	s.RUnlock()
	if !ok {
		s.Lock()
		v := new(uint64)
		*v = deta
		s.data[t] = v
		s.Unlock()
	} else {
		atomic.AddUint64(v, deta)
	}

	atomic.AddUint64(&s.total, deta)
}

func (s *TableStats) Total() uint64 {
	return atomic.LoadUint64(&s.total)
}

func (s *TableStats) Incr(t string) {
	s.IncrBy(t, 1)
}

func (s *TableStats) Empty() bool {
	return atomic.LoadUint64(&s.total) == 0
}

func (s *TableStats) String() string {
	var buf bytes.Buffer
	s.RLock()
	for t, v := range s.data {
		buf.WriteString(fmt.Sprintf("%s:%d\r\n", t, atomic.LoadUint64(v)))
	}
	s.RUnlock()
	buf.WriteString(fmt.Sprintf("total:%d\r\n", atomic.LoadUint64(&s.total)))

	return buf.String()
}

func (s *TableStats) Dump() interface{} {
	d := make(map[string]uint64)
	s.RLock()
	for t, v := range s.data {
		d[t] = atomic.LoadUint64(v)
	}
	s.RUnlock()
	return d
}

func NewTableStats() *TableStats {
	return &TableStats{
		data: make(map[string]*uint64),
	}
}

type CmdStats struct {
	sync.Mutex
	stats map[string]*TableStats
}

func NewCmdStats() *CmdStats {
	s := &CmdStats{
		stats: make(map[string]*TableStats),
	}

	for cmd, _ := range common.ReadCommands {
		s.stats[cmd] = NewTableStats()
	}

	for cmd, _ := range common.WriteCommands {
		s.stats[cmd] = NewTableStats()
	}
	return s
}

func (cs *CmdStats) updateStats(cmd string, table string, deta uint64) {
	if ts, ok := cs.stats[cmd]; ok {
		ts.IncrBy(table, deta)
	}
}

func (cs *CmdStats) Copy() interface{} {
	copied := make(map[string]interface{})
	for cmd, ts := range cs.stats {
		if ts.Empty() {
			continue
		}
		copied[cmd] = ts.Dump()
	}
	return copied
}

func (cs *CmdStats) String() string {
	var buf bytes.Buffer
	var total uint64
	for cmd, ts := range cs.stats {
		if !ts.Empty() {
			buf.WriteString(fmt.Sprintf("%s:%d\r\n", cmd, ts.Total()))
			buf.WriteString(ts.String())
			total += ts.Total()
		}
	}
	buf.WriteString(fmt.Sprintf("total:%d\r\n", total))
	return buf.String()
}

type SlowStats struct {
	sync.Mutex
	stats map[string]uint64
}

func NewSlowStats() *SlowStats {
	return &SlowStats{
		stats: make(map[string]uint64),
	}
}

func (ss *SlowStats) updateStats(cost time.Duration) {
	ss.Lock()

	switch {
	case cost > time.Second:
		ss.stats["1s"] += 1
		fallthrough
	case cost > 500*time.Millisecond:
		ss.stats["500ms"] += 1
		fallthrough
	case cost > 200*time.Millisecond:
		ss.stats["200ms"] += 1
		fallthrough
	case cost > 100*time.Millisecond:
		ss.stats["100ms"] += 1
		fallthrough
	case cost > 50*time.Millisecond:
		ss.stats["50ms"] += 1
		fallthrough
	case cost > 20*time.Millisecond:
		ss.stats["20ms"] += 1
		fallthrough
	case cost > 10*time.Millisecond:
		ss.stats["10ms"] += 1
		fallthrough
	case cost > 5*time.Millisecond:
		ss.stats["5ms"] += 1
		fallthrough
	case cost > 2*time.Millisecond:
		ss.stats["2ms"] += 1
		fallthrough
	case cost > time.Millisecond:
		ss.stats["1ms"] += 1
	default:
		return
	}

	ss.Unlock()
}

func (ss *SlowStats) String() string {
	ss.Lock()
	if len(ss.stats) == 0 {
		ss.Unlock()
		return ""
	}

	var buf bytes.Buffer
	buf.WriteString("#SlowStats:\r\n")
	for threshold, v := range ss.stats {
		buf.WriteString(fmt.Sprintf("cost %s: %d\r\n", threshold, v))
	}
	ss.Unlock()

	return buf.String()
}

func (ss *SlowStats) Copy() map[string]uint64 {
	ss.Lock()
	if len(ss.stats) == 0 {
		ss.Unlock()
		return nil
	}
	copied := make(map[string]uint64, len(ss.stats))
	for threshold, v := range ss.stats {
		copied[threshold] = v
	}
	ss.Unlock()
	return copied
}

type ProxyModuleStats struct {
	stats *CmdStats
	slow  *SlowStats

	failed     uint64
	accumuCost uint64
}

func (self *ProxyModuleStats) IncrFailed() {
	atomic.AddUint64(&self.failed, 1)
}

func (self *ProxyModuleStats) IncrCost(du time.Duration) {
	atomic.AddUint64(&self.accumuCost, uint64(du))
}

func NewProxyModuleStats() *ProxyModuleStats {
	return &ProxyModuleStats{
		stats: NewCmdStats(),
		slow:  NewSlowStats(),
	}
}

func (self *ProxyModuleStats) UpdateStats(redCmd string, t string, deta uint64) {
	self.stats.updateStats(redCmd, t, deta)
}

func (self *ProxyModuleStats) UpdateSlowStats(cost time.Duration) {
	self.slow.updateStats(cost)
}

func (self *ProxyModuleStats) GetStatsData() interface{} {
	stats := struct {
		CmdStats   interface{} `json:"cmd_stats,omitempty"`
		SlowStats  interface{} `json:"slow_stats,omitempty"`
		Failed     uint64      `json:"failed,omitempty"`
		AccumuCost uint64      `json:"accumulate_cost,omitempty"`
	}{}

	if cStats := self.stats.Copy(); cStats != nil {
		stats.CmdStats = cStats
	}

	if cSlow := self.slow.Copy(); cSlow != nil {
		stats.SlowStats = cSlow
	}

	stats.Failed = atomic.LoadUint64(&self.failed)
	stats.AccumuCost = atomic.LoadUint64(&self.accumuCost) / 1000
	return &stats
}

func (self *ProxyModuleStats) String() string {
	buf := bytes.NewBufferString(statsStringHeader)

	if v := self.stats.String(); v != "" {
		buf.WriteString(v)
	}
	if v := self.slow.String(); v != "" {
		buf.WriteString(v)
	}

	if failed := atomic.LoadUint64(&self.failed); failed != 0 {
		buf.WriteString(fmt.Sprintf("failed count:%d\r\n", failed))
	}

	if cost := atomic.LoadUint64(&self.accumuCost); cost != 0 {
		buf.WriteString(fmt.Sprintf("accumulated cost time:%dus\r\n", cost))
	}

	if buf.Len() == len(statsStringHeader) {
		//no valuable stats data
		return ""
	} else {
		return buf.String()
	}
}
