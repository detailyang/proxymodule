package proxymodule

import (
	"fmt"
	"github.com/absolute8511/grace/gracenet"
	"github.com/absolute8511/proxymodule/common"
	"github.com/absolute8511/proxymodule/redisproxy"
)

var proxyModuleLog common.Logger

type ProxyModuleMgr struct {
	servers  map[string]common.ModuleProxyServer
	confList []common.ProxyConf
}

func NewProxyModuleMgr(c []common.ProxyConf) *ProxyModuleMgr {
	return &ProxyModuleMgr{
		servers:  make(map[string]common.ModuleProxyServer),
		confList: c,
	}
}

func SetLogger(level int32, l common.Logger) {
	proxyModuleLog = l
	redisproxy.SetLogger(level, proxyModuleLog)
}

func (self *ProxyModuleMgr) StartAll(grace *gracenet.Net) error {
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
		} else {
			return fmt.Errorf("unknown proxy type: %v", conf.ProxyType)
		}
	}

	return nil
}

func (self *ProxyModuleMgr) StopAll() {
	for _, s := range self.servers {
		s.Stop()
	}

	if proxyModuleLog != nil {
		proxyModuleLog.Flush()
	}
}
