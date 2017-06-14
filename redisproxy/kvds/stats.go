package kvds

import (
	"bytes"
	"fmt"

	"github.com/absolute8511/proxymodule/redisproxy/stats"
)

type Stats struct {
	stats.ModuleStats
	MemStats map[string]stats.ModuleStats
}

func (s *Stats) String() string {
	buf := bytes.NewBufferString(s.ModuleStats.String())
	buf.WriteString("\r\n#Cluster Module Statistic:\r\n")
	for mem, ms := range s.MemStats {
		buf.WriteString(fmt.Sprintf("#Module:%s\r\n", mem))
		buf.WriteString(ms.String())
		buf.WriteString("\r\n")
	}

	return buf.String()
}

func (s *Stats) GetStatsData() interface{} {
	statsData := make(map[string]interface{})
	for mem, ms := range s.MemStats {
		statsData[mem] = ms.GetStatsData()
	}
	statsData["kvds"] = s.ModuleStats.GetStatsData()
	return statsData
}

func NewStats() *Stats {
	return &Stats{
		ModuleStats: stats.NewProxyModuleStats(),
		MemStats:    make(map[string]stats.ModuleStats),
	}
}

func (s *Stats) AddMemStats(mem string, stats stats.ModuleStats) {
	s.MemStats[mem] = stats
}
