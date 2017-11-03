package redisproxy

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absolute8511/redigo/redis"
)

const (
	defaultMaxIdleConn   = 128
	defaultMaxActiveConn = 1024
	defaultTendInterval  = 3
	defaultReadTimeout   = 3
	defaultWriteTimeout  = 3
	defaultDialTimeout   = 5
	defaultIdleTimeout   = 120
)

type CodisHost struct {
	addr     string
	connPool *redis.QueuePool
}

type CodisCluster struct {
	sync.RWMutex
	ServerList []CodisServer

	rotate int64
	nodes  []*CodisHost

	tendInterval  int64
	maxIdleConn   int
	maxActiveConn int
	idleTimeout   int64

	wg    sync.WaitGroup
	quitC chan struct{}

	dialF func(string) (redis.Conn, error)
}

func NewCodisCluster(conf *CodisProxyConf) *CodisCluster {
	cluster := &CodisCluster{
		quitC:         make(chan struct{}),
		tendInterval:  conf.TendInterval,
		maxActiveConn: conf.MaxActiveConn,
		maxIdleConn:   conf.MaxIdleConn,
		idleTimeout:   conf.IdleTimeout,
		ServerList:    make([]CodisServer, len(conf.ServerList)),
		nodes:         make([]*CodisHost, 0, len(conf.ServerList)),
	}

	copy(cluster.ServerList, conf.ServerList)

	cluster.dialF = func(addr string) (redis.Conn, error) {
		return redis.DialTimeout("tcp", addr, conf.DialTimeout*time.Second,
			conf.ReadTimeout*time.Second, conf.WriteTimeout*time.Second)
	}

	cluster.Tend()

	if len(cluster.nodes) == 0 {
		redisLog.Errorln("no node in server list is available at init")
	}

	cluster.wg.Add(1)

	go cluster.tendNodes()

	return cluster
}

func (cluster *CodisCluster) GetConn() (redis.Conn, error) {
	cluster.RLock()

	if len(cluster.nodes) == 0 {
		cluster.RUnlock()
		return nil, errors.New("no server is available right now")
	}

	picked := atomic.AddInt64(&cluster.rotate, 1) % int64(len(cluster.nodes))

	connPool := cluster.nodes[picked].connPool

	cluster.RUnlock()

	return connPool.Get(250 * time.Millisecond)
}

func (cluster *CodisCluster) Tend() {
	availableHosts := make(map[string]struct{})
	flag := struct{}{}

	for _, host := range cluster.ServerList {
		conn, err := cluster.dialF(host.ServerAddr)
		if err != nil {
			redisLog.Warningf("dial to codis server failed, disable the address: %s, err: %s", host.ServerAddr, err.Error())
		} else {
			if _, err = conn.Do("PING"); err != nil {
				redisLog.Warningf("ping codis server failed, disable the address: %s, err: %s", host.ServerAddr, err.Error())
			} else {
				redisLog.Debugf("codis server: %v is available", host)
				availableHosts[host.ServerAddr] = flag
				conn.Close()
			}
		}
	}

	if len(availableHosts) == 0 {
		redisLog.Errorln("no server node is at serviceable state")
		return
	}

	usedNodes := make([]*CodisHost, 0, len(cluster.ServerList))
	newNodes := usedNodes[:0]

	cluster.RLock()
	for _, node := range cluster.nodes {
		usedNodes = append(usedNodes, node)
	}
	cluster.RUnlock()

	var delNodes []*CodisHost
	for _, node := range usedNodes {
		if _, ok := availableHosts[node.addr]; !ok {
			delNodes = append(delNodes, node)
		} else {
			newNodes = append(newNodes, node)
			delete(availableHosts, node.addr)
		}
	}

	testF := func(c redis.Conn, t time.Time) (err error) {
		if time.Since(t) > 60*time.Second {
			_, err = c.Do("PING")
		}
		return
	}

	for addr, _ := range availableHosts {
		newNode := &CodisHost{addr: addr}
		newNode.connPool = redis.NewQueuePool(
			func() (redis.Conn, error) { return cluster.dialF(newNode.addr) },
			cluster.maxIdleConn,
			cluster.maxActiveConn,
		)
		newNode.connPool.TestOnBorrow = testF
		newNode.connPool.IdleTimeout = time.Duration(cluster.idleTimeout) * time.Second
		redisLog.Infof("host:%v is available and come into service", newNode.addr)
		newNodes = append(newNodes, newNode)
	}

	if len(availableHosts) != 0 || len(delNodes) != 0 {
		cluster.Lock()
		cluster.nodes = newNodes
		cluster.Unlock()
	}

	for _, node := range delNodes {
		redisLog.Infof("host:%s is unavailable and deleted from cluster", node.addr)
		node.connPool.Close()
	}
}

func (cluster *CodisCluster) tendNodes() {
	tendTicker := time.NewTicker(time.Duration(cluster.tendInterval) * time.Second)
	defer func() {
		tendTicker.Stop()
		cluster.wg.Done()
	}()

	for {
		select {
		case <-tendTicker.C:
			cluster.Tend()
		case <-cluster.quitC:
			cluster.Lock()
			nodes := cluster.nodes
			cluster.nodes = []*CodisHost{}
			cluster.Unlock()
			for _, node := range nodes {
				node.connPool.Close()
			}
			redisLog.Debugf("go routine for tend codis cluster exit")
			return
		}
	}

}

func (cluster *CodisCluster) Close() {
	close(cluster.quitC)

	cluster.wg.Wait()
}
