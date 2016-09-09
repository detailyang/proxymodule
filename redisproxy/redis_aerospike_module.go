package redisproxy

import (
	"errors"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	as "github.com/aerospike/aerospike-client-go"
	"github.com/aerospike/aerospike-client-go/logger"
)

var (
	ErrKeyInvalid = errors.New("Invalid Aerospike Key")
	ErrCmdParams  = errors.New("command params error")
	ErrFieldValue = errors.New("Invalid field value")
)

type AerospikeRedisConf struct {
	AerospikeServers []string
	Timeout          int
}

type AerospikeRedisProxy struct {
	sync.RWMutex
	asClient        *as.Client
	asServers       []string
	conf            *AerospikeRedisConf
	proxyStatistics *AerospikeProxyStatistics
}

func CreateRedis2AerospikeProxy() RedisProxyModule {
	return &AerospikeRedisProxy{
		asServers:       make([]string, 0),
		proxyStatistics: NewAerospikeProxyStatistics(),
	}
}

func init() {
	RegisterRedisProxyModule("redis2aerospike", CreateRedis2AerospikeProxy)
}

func (self *AerospikeRedisProxy) GetProxyName() string {
	return "redis2aerospike"
}

func (self *AerospikeRedisProxy) Stop() {
	self.asClient.Close()
}

func (self *AerospikeRedisProxy) InitConf(f func(v interface{}) error) error {
	self.conf = &AerospikeRedisConf{}
	err := f(self.conf)
	if err != nil {
		return err
	}

	redisLog.Infof("module configuration: %v", self.conf)

	self.asServers = self.conf.AerospikeServers
	hosts := make([]*as.Host, len(self.asServers))
	for i, server := range self.asServers {
		h, portStr, err := net.SplitHostPort(server)
		if err != nil {
			continue
		}
		port, _ := strconv.Atoi(portStr)
		hosts[i] = as.NewHost(h, port)
	}
	logger.Logger.SetLogger(redisLog)
	if redisLog.Level() > 1 {
		logger.Logger.SetLevel(logger.INFO)
	} else {
		logger.Logger.SetLevel(logger.WARNING)
	}
	self.asClient, err = as.NewClientWithPolicyAndHost(nil, hosts...)
	if err != nil {
		redisLog.Errorf("failed to init aerospike client: %v", err)
		return err
	}
	self.asClient.DefaultPolicy.Timeout = time.Second * time.Duration(int64(self.conf.Timeout))
	self.asClient.DefaultWritePolicy.SendKey = true
	self.asClient.DefaultWritePolicy.Expiration = math.MaxUint32
	return nil
}

func (self *AerospikeRedisProxy) GetStatisticsModule() ProxyStatisticsModule {
	return self.proxyStatistics
}

func (self *AerospikeRedisProxy) wrapParserRedisKey(f AsCommandFunc) CommandFunc {
	return func(c *Client, w ResponseWriter) error {
		if len(c.Args) == 0 {
			return ErrCmdParams
		}
		k, err := parserRedisKey(string(c.Args[0]))
		if err != nil {
			return err
		}

		var ArgEx [][]byte
		if len(c.Args) > 1 {
			ArgEx = c.Args[1:]
		} else {
			ArgEx = nil
		}

		self.proxyStatistics.Statistic(c.cmd, k, ArgEx)

		return f(c, k, w)
	}
}

func (self *AerospikeRedisProxy) wrapParserRedisKeyAndField(f AsCommandFuncWithBins) CommandFunc {
	return func(c *Client, w ResponseWriter) error {
		if len(c.Args) == 0 {
			return ErrCmdParams
		}
		k, fields, err := parserRedisKeyAndFields(c.Args)
		if err != nil {
			return err
		}

		self.proxyStatistics.Statistic(c.cmd, k, nil)

		return f(c, k, fields, w)
	}
}

func (self *AerospikeRedisProxy) RegisterCmd(router *CmdRouter) {
	router.Register("get", self.wrapParserRedisKey(self.getCommand))
	router.Register("del", self.wrapParserRedisKey(self.delCommand))
	router.Register("set", self.wrapParserRedisKey(self.setCommand))
	router.Register("setex", self.wrapParserRedisKey(self.setexCommand))
	router.Register("exists", self.wrapParserRedisKey(self.existsCommand))
	router.Register("mget", self.wrapParserRedisKey(self.mgetCommand))
	router.Register("expire", self.wrapParserRedisKey(self.expireCommand))
	router.Register("ttl", self.wrapParserRedisKey(self.ttlCommand))
	router.Register("hget", self.wrapParserRedisKeyAndField(self.hgetCommand))
	router.Register("hgetall", self.wrapParserRedisKeyAndField(self.hgetallCommand))
	router.Register("hmget", self.wrapParserRedisKeyAndField(self.hmgetCommand))
	router.Register("hmset", self.wrapParserRedisKeyAndField(self.hmsetCommand))
	router.Register("hset", self.wrapParserRedisKeyAndField(self.hsetCommand))
	router.Register("hdel", self.wrapParserRedisKeyAndField(self.hdelCommand))
	router.Register("hexists", self.wrapParserRedisKeyAndField(self.hexistsCommand))
	router.Register("info", self.infoCommand)
}

func parserRedisKey(key string) (*as.Key, error) {
	// redis key ns:set:key
	strList := strings.SplitN(key, ":", 3)
	if len(strList) != 3 {
		//return as.NewKey("test", "redis-test", key)
		return nil, ErrKeyInvalid
	}
	return as.NewKey(strList[0], strList[1], strList[2])
}

func parserRedisKeyAndFields(args [][]byte) (*as.Key, []*as.Bin, error) {
	if len(args) == 0 {
		return nil, nil, ErrCmdParams
	}
	key, err := parserRedisKey(string(args[0]))
	if err != nil {
		return nil, nil, err
	}

	var bins []*as.Bin
	if len(args[1:])%2 == 0 {
		fields := args[1:]
		bins = make([]*as.Bin, 0, len(fields)/2)
		for i := 0; i < len(fields)/2; i++ {
			bin := as.NewBin(string(fields[i*2]), fields[i*2+1])
			bins = append(bins, bin)
		}
	}

	return key, bins, nil
}

type AsCommandFunc func(c *Client, k *as.Key, w ResponseWriter) error
type AsCommandFuncWithBins func(c *Client, k *as.Key, bins []*as.Bin, w ResponseWriter) error
