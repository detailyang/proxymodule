package redisproxy

import (
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absolute8511/go-zanredisdb"
	"github.com/absolute8511/grace/gracenet"
	"github.com/absolute8511/proxymodule/common"
	"github.com/absolute8511/proxymodule/redisproxy/stats"
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

type RedisProxyModule interface {
	RegisterCmd(*CmdRouter)
	InitConf(func(v interface{}) error) error
	Stop()
	GetProxyName() string
	GetStats() stats.ModuleStats
}

type RedisProxyModuleCreateFunc func() RedisProxyModule

var gRedisProxyModuleFactory = make(map[string]RedisProxyModuleCreateFunc)

func RegisterRedisProxyModule(name string, h RedisProxyModuleCreateFunc) {
	gRedisProxyModuleFactory[name] = h
}

type RedisProxy struct {
	laddrs      []string
	quitChan    chan bool
	graceQuitC  chan struct{}
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

	zanredisdb.SetLogger(redisLog.Level(), redisLog.Logger)
}

func SetLoggerLevel(level int32) {
	redisLog.SetLevel(level)

	zanredisdb.SetLogger(redisLog.Level(), redisLog.Logger)
}

func NewRedisProxy(addrs string, module string, moduleConfig string, grace *gracenet.Net) *RedisProxy {
	if _, ok := gRedisProxyModuleFactory[module]; !ok {
		redisLog.Errorf("redis proxy module not found: %v", module)
		return nil
	}

	rp := &RedisProxy{
		laddrs:      strings.Split(addrs, ","),
		quitChan:    make(chan bool),
		graceQuitC:  make(chan struct{}),
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

func (self *RedisProxy) StopGracefully() (chan struct{}, error) {
	redisLog.Infof("redis proxy: %s, listened on: %v start to stop gracefully",
		self.proxyModule.GetProxyName(), self.laddrs)

	close(self.graceQuitC)

	doneC := make(chan struct{})

	go func() {
		//use a timer to set a time threshold to wait
		graceTimer := time.Tick(5 * time.Minute)
		go func() {
			select {
			case <-graceTimer:
				redisLog.Warningf("redis proxy: %s, wait too long to stop gracefully,"+
					"all pending connections will be closed right now", self.proxyModule.GetProxyName())
				close(self.quitChan)
			case <-doneC:
				return
			}
		}()

		self.wg.Wait()
		self.proxyModule.Stop()

		close(doneC)
		redisLog.Infof("stop gracefully: wait redis proxy done: %s, address: %v",
			self.proxyModule.GetProxyName(), self.laddrs)

	}()

	return doneC, nil
}

func (self *RedisProxy) ServeRedis() {
	connCh := make(chan net.Conn, connChannelLength)
	pendingListeners := int64(len(self.listeners))

	for _, l := range self.listeners {
		self.wg.Add(1)
		go func(l net.Listener) {
			defer func() {
				redisLog.Infof("stop accepting connections from %s", l.Addr().String())
				if atomic.AddInt64(&pendingListeners, -1) == 0 {
					redisLog.Infof("all pending listeners have been stopped from accepting connections, close connCh")
					close(connCh)
				}
				select {
				case <-self.graceQuitC:
				default:
					if ul, ok := l.(*net.UnixListener); ok {
						//close UnixListener will not unlink the unix domain socket file at the upgraded child process
						//we need to remove it manually
						os.Remove(ul.Addr().String())
					}
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
					if netErr, ok := err.(net.Error); !(ok && netErr.Timeout()) {
						redisLog.Infof("accept at address: %s, error: %v", l.Addr().String(), err)
					}
				} else {
					connCh <- conn
				}

				select {
				case <-self.quitChan:
					redisLog.Infof("process has been stopped, stop accepting connections from %s", l.Addr().String())
					return
				case <-self.graceQuitC:
					redisLog.Infof("quit gracefully, process [pid: %d] stop accepting connections from %s",
						os.Getpid(), l.Addr().String())
					return
				default:
					continue
				}
			}
		}(l)
	}

	pool := &sync.Pool{New: func() interface{} { return NewEmptyClientRESP(self.quitChan) }}

	for {
		select {
		case conn, ok := <-connCh:
			if ok {
				client := pool.Get().(*RespClient)
				client.Reset(conn)
				client.RegCmds = self.router
				client.moduleStats = self.proxyModule.GetStats()

				self.wg.Add(1)
				go func() {
					defer func() {
						client.reset() //reset must be called before reuse the client
						pool.Put(client)
						self.wg.Done()
					}()
					client.Run()
				}()
			} else {
				redisLog.Infoln("all listeners have been stopped, ServeRedis exit now")
				return
			}
		}
	}
}

func (self *RedisProxy) GetStatsData() interface{} {
	if stats := self.proxyModule.GetStats(); stats != nil {
		return stats.GetStatsData()
	} else {
		return nil
	}
}
