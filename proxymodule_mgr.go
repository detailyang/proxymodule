package proxymodule

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/absolute8511/grace/gracenet"
	"github.com/absolute8511/proxymodule/common"
	"github.com/absolute8511/proxymodule/redisproxy"
	"github.com/wangjian-pg/dccproxy"
)

var proxyModuleLog common.Logger

type ProxyModuleMgr struct {
	sync.Mutex
	servers  map[string]common.ModuleProxyServer
	confList []common.ProxyConf
	wg       *sync.WaitGroup
}

func NewProxyModuleMgr(c *common.ProxyModuleConf) *ProxyModuleMgr {
	mgr := &ProxyModuleMgr{
		servers:  make(map[string]common.ModuleProxyServer),
		confList: c.ProxyConfList,
		wg:       &sync.WaitGroup{},
	}

	common.GlobalControlCenter = common.NewControlCenter(c.DccServers,
		c.DccBackupFile, c.DccTag, c.DccEnv)

	return mgr
}

func SetLogger(level int32, l common.Logger) {
	proxyModuleLog = l

	redisproxy.SetLogger(level, proxyModuleLog)
	dccproxy.SetLogger(level, proxyModuleLog)
}

func SetLoggerLevel(level int32) {
	redisproxy.SetLoggerLevel(level)
	dccproxy.SetLoggerLevel(level)
}

func (self *ProxyModuleMgr) StartAll(grace *gracenet.Net) error {
	self.Mutex.Lock()
	defer self.Mutex.Unlock()

	for _, conf := range self.confList {
		var proxyModule common.ModuleProxyServer
		var err error
		switch conf.ProxyType {
		case "REDIS":
			proxyModule = redisproxy.NewRedisProxy(conf.LocalProxyAddr,
				conf.ModuleName,
				conf.ModuleConfPath, grace)
			if proxyModule == nil {
				return fmt.Errorf("failed start proxy: %v", conf.ModuleName)
			}
			self.servers[conf.ModuleName] = proxyModule
		case "DCC":
			if proxyModule, err = dccproxy.NewDccProxy(conf.LocalProxyAddr,
				conf.ModuleConfPath, grace); err != nil {
				return err
			} else {
				self.servers[conf.ModuleName] = proxyModule
			}
		default:
			return fmt.Errorf("unknown proxy type: %v", conf.ProxyType)
		}

		self.wg.Add(1)
		go func() {
			defer self.wg.Done()
			proxyModule.Start()
		}()
	}

	return nil
}

func (self *ProxyModuleMgr) StopAll() {
	if common.GlobalControlCenter != nil {
		common.GlobalControlCenter.Close()
	}

	defer func() {
		if proxyModuleLog != nil {
			proxyModuleLog.Flush()
		}
	}()

	self.Mutex.Lock()
	for name, s := range self.servers {
		s.Stop()
		delete(self.servers, name)
	}
	self.Mutex.Unlock()

	self.wg.Wait()
}

func (self *ProxyModuleMgr) StopAllGracefully() (chan struct{}, error) {
	if err := self.CheckGraceful(); err != nil {
		return nil, err
	}

	self.Mutex.Lock()

	total := len(self.servers)
	var finished int32

	proxyDoneCh := make(map[string]chan struct{})

	for name, proxy := range self.servers {
		graceProxy, _ := proxy.(common.GraceModuleProxyServer)
		if doneC, err := graceProxy.StopGracefully(); err != nil {
			self.Mutex.Unlock()
			return nil, fmt.Errorf("proxy module:%s stop gracefully failed, err:%s", name, err.Error())
		} else {
			proxyDoneCh[name] = doneC
		}
	}
	self.Mutex.Unlock()

	finishedCh := make(chan struct{})

	for name, doneC := range proxyDoneCh {
		go func(name string, doneCh chan struct{}) {
			select {
			case <-doneCh:
				if atomic.AddInt32(&finished, 1) == int32(total) {
					close(finishedCh)
				}
			}
		}(name, doneC)
	}

	return finishedCh, nil
}

//does all the proxies contained in the manager support stop gracefully
func (self *ProxyModuleMgr) CheckGraceful() error {
	defer self.Mutex.Unlock()
	self.Mutex.Lock()

	for name, proxy := range self.servers {
		if _, ok := proxy.(common.GraceModuleProxyServer); !ok {
			return fmt.Errorf("proxy module: %s does not support stop gracefully", name)
		}
	}

	return nil
}

func (self *ProxyModuleMgr) GetModuleStats(module string) (interface{}, error) {
	defer self.Mutex.Unlock()
	self.Mutex.Lock()

	if proxyModule, ok := self.servers[module]; ok {
		return proxyModule.GetStatsData(), nil
	} else {
		return nil, fmt.Errorf("unknown proxy module:%s", module)
	}
}
