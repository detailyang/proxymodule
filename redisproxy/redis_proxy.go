package redisproxy

import (
	"github.com/absolute8511/grace/gracenet"
	"github.com/absolute8511/proxymodule/common"
	"net"
	"strings"
	"sync"
)

var redisLog = common.NewLevelLogger(1, &common.GLogger{})

type RedisProxyModule interface {
	RegisterCmd(*CmdRouter)
	InitConf(func(v interface{}) error) error
	Stop()
	GetProxyName() string
}

type RedisProxyModuleCreateFunc func() RedisProxyModule

var gRedisProxyModuleFactory = make(map[string]RedisProxyModuleCreateFunc)

func RegisterRedisProxyModule(name string, h RedisProxyModuleCreateFunc) {
	gRedisProxyModuleFactory[name] = h
}

type RedisProxy struct {
	laddr       string
	quitChan    chan bool
	proxyModule RedisProxyModule
	router      *CmdRouter
	wg          sync.WaitGroup
	l           net.Listener
	grace       *gracenet.Net
}

// should call only once before any proxy started.
func SetLogger(level int32, l common.Logger) {
	redisLog.Logger = l
	redisLog.SetLevel(level)
}

func NewRedisProxy(addr string, module string, moduleConfig string, grace *gracenet.Net) *RedisProxy {
	if _, ok := gRedisProxyModuleFactory[module]; !ok {
		redisLog.Errorf("redis proxy module not found: %v", module)
		return nil
	}

	rp := &RedisProxy{
		laddr:       addr,
		quitChan:    make(chan bool),
		proxyModule: gRedisProxyModuleFactory[module](),
		router:      NewCmdRouter(),
		grace:       grace,
	}
	if rp.proxyModule == nil {
		redisLog.Errorf("create module failed: %v", module)
		return nil
	}
	err := rp.proxyModule.InitConf(func(v interface{}) error {
		redisLog.Infof("Init module config from : %v", moduleConfig)
		return common.LoadModuleConfFromFile(moduleConfig, v)
	})
	if err != nil {
		redisLog.Errorf("init module configure %v failed: %v", moduleConfig, err)
		return nil
	}
	rp.proxyModule.RegisterCmd(rp.router)
	return rp
}

func (self *RedisProxy) Start() {
	defer redisLog.Flush()
	self.wg.Add(1)
	defer self.wg.Done()
	redisLog.Infof("redis proxy module %v on : %v", self.proxyModule.GetProxyName(), self.laddr)
	defer redisLog.Infof("redis proxy %v stopped.", self.proxyModule.GetProxyName())

	var err error
	if self.grace != nil {
		if strings.HasPrefix(self.laddr, "unix://") {
			unixpath := self.laddr[len("unix://"):]
			self.l, err = self.grace.Listen("unix", unixpath)
		} else {
			self.l, err = self.grace.Listen("tcp", self.laddr)
		}
	} else {
		self.l, err = net.Listen("tcp", self.laddr)
	}
	if err != nil {
		redisLog.Errorf("err: %v", err)
		return
	}

	self.ServeRedis()
}

func (self *RedisProxy) Stop() {
	close(self.quitChan)
	if self.l != nil {
		self.l.Close()
	}
	self.proxyModule.Stop()
	self.wg.Wait()
	redisLog.Infof("wait redis proxy done: %v", self.proxyModule.GetProxyName())
	redisLog.Flush()
}

func (self *RedisProxy) ServeRedis() {
	pool := &sync.Pool{New: func() interface{} { return NewEmptyClientRESP(self.quitChan) }}
	for {
		// accept client request and call handler
		conn, err := self.l.Accept()
		if err != nil {
			redisLog.Infof("accept error: %v", err)
			break
		}
		client := pool.Get().(*RespClient)
		client.Reset(conn)
		client.RegCmds = self.router
		self.wg.Add(1)
		go func() {
			defer self.wg.Done()
			client.Run()
			pool.Put(client)
		}()
	}
}
