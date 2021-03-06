package proxymodule

import (
	"fmt"
	"sync"
	"time"

	"gitlab.qima-inc.com/shiwei/TetherMonitorSDK"

	"github.com/absolute8511/grace/gracenet"
	"github.com/absolute8511/proxymodule/common"
	"github.com/absolute8511/proxymodule/redisproxy"
	"github.com/wangjian-pg/dccproxy"
)

var proxyModuleLog common.Logger

type ProxyModuleMgr struct {
	sync.Mutex
	servers      map[string]common.ModuleProxyServer
	confList     []common.ProxyConf
	monitorQuitC chan struct{}
}

func NewProxyModuleMgr(c []common.ProxyConf) *ProxyModuleMgr {
	return &ProxyModuleMgr{
		servers:      make(map[string]common.ModuleProxyServer),
		confList:     c,
		monitorQuitC: make(chan struct{}),
	}
}

func SetLogger(level int32, l common.Logger) {
	proxyModuleLog = l

	redisproxy.SetLogger(level, proxyModuleLog)
	dccproxy.SetLogger(level, proxyModuleLog)
}

func (self *ProxyModuleMgr) StartAll(grace *gracenet.Net) error {
	self.Mutex.Lock()
	defer self.Mutex.Unlock()

	for _, conf := range self.confList {
		if conf.ProxyType == "REDIS" {
			s := redisproxy.NewRedisProxy(conf.LocalProxyAddr,
				conf.ModuleName,
				conf.ModuleConfPath, grace)
			if s == nil {
				return fmt.Errorf("failed start proxy: %v", conf.ModuleName)
			}
			go s.Start()
			self.servers[conf.ModuleName] = s
		} else if conf.ProxyType == "DCC" {
			if s, err := dccproxy.NewDccProxy(conf.LocalProxyAddr,
				conf.ModuleConfPath, grace); err != nil {
				return err
			} else {
				go s.Start()
				self.servers[conf.ModuleName] = s
			}
		} else {
			return fmt.Errorf("unknown proxy type: %v", conf.ProxyType)
		}
	}

	go self.DoProxyModulesMonitor()

	return nil
}

func (self *ProxyModuleMgr) StopAll() {
	self.Mutex.Lock()
	defer self.Mutex.Unlock()

	defer func() {
		if proxyModuleLog != nil {
			proxyModuleLog.Flush()
		}
	}()

	close(self.monitorQuitC)

	for _, s := range self.servers {
		s.Stop()
	}
}

func (self *ProxyModuleMgr) DoProxyModulesMonitor() {
	flushTicker := time.NewTicker(60 * time.Second)
	defer flushTicker.Stop()

	for {
		select {
		case <-flushTicker.C:
			self.Mutex.Lock()
			for proxyName, proxyServer := range self.servers {
				if statisticsData := proxyServer.ProxyStatisticsData(); statisticsData != nil && len(statisticsData) > 0 {
					monitorsdk.AddProxyModuleMonitorData(proxyName, statisticsData)
				}
			}
			self.Mutex.Unlock()
		case <-self.monitorQuitC:
			break
		}
	}
}
