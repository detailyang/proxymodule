package redisproxy

import (
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absolute8511/grace/gracenet"
	"github.com/absolute8511/proxymodule/common"
)

const (
	connChannelLength = 128
	acceptInterval    = 3
)

type netListenerEx interface {
	net.Listener
	SetDeadline(t time.Time) error
}

var redisLog = common.NewLevelLogger(1, nil)

type ProxyStatisticsModule interface {
	IncrSlowOperation(time.Duration)
	IncrFailedOperation()
	IncrOpTime(int64)
	GenMonitorData() []byte
}

type RedisProxyModule interface {
	RegisterCmd(*CmdRouter)
	InitConf(func(v interface{}) error) error
	Stop()
	GetProxyName() string
	GetStatisticsModule() ProxyStatisticsModule
}

type RedisProxyModuleCreateFunc func() RedisProxyModule

var gRedisProxyModuleFactory = make(map[string]RedisProxyModuleCreateFunc)

func RegisterRedisProxyModule(name string, h RedisProxyModuleCreateFunc) {
	gRedisProxyModuleFactory[name] = h
}

type RedisProxy struct {
	laddrs      []string
	quitChan    chan bool
	hotUpgradeC chan struct{}
	proxyModule RedisProxyModule
	router      *CmdRouter
	wg          sync.WaitGroup
	listeners   []net.Listener
	grace       *gracenet.Net
}

// should call only once before any proxy started.
func SetLogger(level int32, l common.Logger) {
	redisLog.Logger = l
	redisLog.SetLevel(level)
}

func NewRedisProxy(addrs string, module string, moduleConfig string, grace *gracenet.Net) *RedisProxy {
	if _, ok := gRedisProxyModuleFactory[module]; !ok {
		redisLog.Errorf("redis proxy module not found: %v", module)
		return nil
	}

	rp := &RedisProxy{
		laddrs:      strings.Split(addrs, ","),
		quitChan:    make(chan bool),
		hotUpgradeC: make(chan struct{}),
		proxyModule: gRedisProxyModuleFactory[module](),
		router:      NewCmdRouter(),
		grace:       grace,
		listeners:   make([]net.Listener, 0, len(addrs)),
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
	self.wg.Add(1)
	defer self.wg.Done()
	redisLog.Infof("redis proxy module %v on : %v", self.proxyModule.GetProxyName(), self.laddrs)
	defer redisLog.Infof("redis proxy %v stopped.", self.proxyModule.GetProxyName())

	var err error

	for _, laddr := range self.laddrs {
		var l net.Listener
		if self.grace != nil {
			if strings.HasPrefix(laddr, "unix://") {
				unixpath := laddr[len("unix://"):]
				l, err = self.grace.Listen("unix", unixpath)
			} else {
				l, err = self.grace.Listen("tcp", laddr)
			}
		} else {
			if strings.HasPrefix(laddr, "unix://") {
				unixpath := laddr[len("unix://"):]
				l, err = net.Listen("unix", unixpath)
			} else {
				l, err = net.Listen("tcp", laddr)
			}
		}
		if err != nil {
			redisLog.Errorf("listen address laddr[%s] err [%v], proxy start failed", laddr, err)
			return
		} else {
			self.listeners = append(self.listeners, l)
		}
	}

	self.ServeRedis()
}

func (self *RedisProxy) Stop() {
	close(self.quitChan)

	for _, l := range self.listeners {
		l.Close()
	}

	self.wg.Wait()

	self.proxyModule.Stop()
	redisLog.Infof("quit: wait redis proxy done: %v, address: %v", self.proxyModule.GetProxyName(), self.laddrs)
}

func (self *RedisProxy) HotUpgrade() {
	close(self.hotUpgradeC)
	self.wg.Wait()

	self.proxyModule.Stop()
	redisLog.Infof("hot upgrade: wait redis proxy done: %v, address: %v", self.proxyModule.GetProxyName(), self.laddrs)
}

func (self *RedisProxy) ServeRedis() {
	pool := &sync.Pool{New: func() interface{} { return NewEmptyClientRESP(self.quitChan) }}
	connCh := make(chan net.Conn, connChannelLength)
	pendingListeners := int64(len(self.listeners))

	for _, l := range self.listeners {
		self.wg.Add(1)
		go func(l net.Listener) {
			defer func() {
				if atomic.AddInt64(&pendingListeners, -1) == 0 {
					redisLog.Info("all pending listeners have been stopped from accepting connections, close connCh")
					close(connCh)
				}
				select {
				case <-self.quitChan:
					if ul, ok := l.(*net.UnixListener); ok {
						//close UnixListener will not unlink the unix domain socket file at the upgraded child process
						//we need to remove it manually
						os.Remove(ul.Addr().String())
					}
				default:
				}
				self.wg.Done()
			}()

			lex, ok := l.(netListenerEx)
			if !ok {
				redisLog.Errorf("use unknown Listener at %s", l.Addr().String())
				return
			}
			du := time.Duration(acceptInterval) * time.Second
			for {
				//set timeout to prevent the program from blocked forever
				lex.SetDeadline(time.Now().Add(du))
				if conn, err := l.Accept(); err != nil {
					redisLog.Infof("accept at address: %s, error: %v", l.Addr().String(), err)
				} else {
					connCh <- conn
				}

				select {
				case <-self.quitChan:
					redisLog.Infof("process has been stoped, stop accepting connections from %s", l.Addr().String())
					return
				case <-self.hotUpgradeC:
					redisLog.Infof("hot upgrade, process [pid: %d] stop accepting connections from %s", os.Getpid(), l.Addr().String())
					return
				default:
					continue
				}
			}
		}(l)
	}

	for {
		select {
		case conn, ok := <-connCh:
			if ok {
				client := pool.Get().(*RespClient)
				client.Reset(conn)
				client.RegCmds = self.router
				client.proxyStatistics = self.proxyModule.GetStatisticsModule()

				self.wg.Add(1)
				go func() {
					defer self.wg.Done()
					client.Run()
					pool.Put(client)
				}()
			} else {
				redisLog.Infoln("all listeners have been stopped, ServeRedis exit now")
				return
			}
		}
	}
}

func (self *RedisProxy) ProxyStatisticsData() []byte {
	if statisticsModule := self.proxyModule.GetStatisticsModule(); statisticsModule != nil {
		return statisticsModule.GenMonitorData()
	} else {
		return nil
	}
}
