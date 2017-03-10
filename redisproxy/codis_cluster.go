package redisproxy

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/garyburd/redigo/redis"
)

type CodisHost struct {
	addr     string
	connPool *redis.Pool
}

type CodisCluster struct {
	sync.Mutex
	ServerList []CodisServer

	rotate       int64
	nodes        []*CodisHost
	tendInterval int64
	wg           sync.WaitGroup
	quitC        chan struct{}
}

func NewCodisCluster(hosts []CodisServer, TendInterval int64) *CodisCluster {
	cluster := &CodisCluster{
		quitC:        make(chan struct{}),
		tendInterval: TendInterval,
		ServerList:   make([]CodisServer, len(hosts)),
		nodes:        make([]*CodisHost, 0, len(hosts)),
	}

	copy(cluster.ServerList, hosts)

	cluster.Tend()

	if len(cluster.nodes) == 0 {
		redisLog.Errorln("no node in server list is available at init")
	}

	cluster.wg.Add(1)

	go cluster.tendNodes()

	return cluster
}

func (cluster *CodisCluster) GetConn() (redis.Conn, error) {
	cluster.Mutex.Lock()

	if len(cluster.nodes) == 0 {
		cluster.Mutex.Unlock()
		return nil, errors.New("no server is available right now")
	}

	picked := atomic.AddInt64(&cluster.rotate, 1) % int64(len(cluster.nodes))

	conn := cluster.nodes[picked].connPool.Get()

	cluster.Mutex.Unlock()

	return conn, nil
}

func (cluster *CodisCluster) Tend() {
	availableHosts := make(map[string]struct{})
	flag := struct{}{}

	for _, host := range cluster.ServerList {
		conn, err := redis.Dial("tcp", host.ServerAddr)
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

	cluster.Mutex.Lock()
	for _, node := range cluster.nodes {
		usedNodes = append(usedNodes, node)
	}
	cluster.Mutex.Unlock()

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

	dialDecorator := func(addr string) func() (redis.Conn, error) {
		return func() (redis.Conn, error) {
			return redis.Dial("tcp", addr)
		}
	}

	for addr, _ := range availableHosts {
		newNode := &CodisHost{addr: addr}
		newNode.connPool = &redis.Pool{
			MaxIdle:      256,
			MaxActive:    512,
			IdleTimeout:  120 * time.Second,
			TestOnBorrow: testF,
			Dial:         dialDecorator(addr),
		}
		redisLog.Infof("host:%v is available and come into service", newNode.addr)
		newNodes = append(newNodes, newNode)
	}

	cluster.Mutex.Lock()
	cluster.nodes = newNodes
	cluster.Mutex.Unlock()

	for _, node := range delNodes {
		redisLog.Infof("host:%v is unavailable and deleted from cluster", node.addr)
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
			redisLog.Debugf("go routine for tend codis cluster exit")
			return
		}
	}

}

func (cluster *CodisCluster) Close() {
	cluster.Mutex.Lock()
	for _, node := range cluster.nodes {
		node.connPool.Close()
	}
	cluster.Mutex.Unlock()

	close(cluster.quitC)

	cluster.wg.Wait()
}
