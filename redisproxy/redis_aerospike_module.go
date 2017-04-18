package redisproxy

import (
	"errors"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/absolute8511/proxymodule/common"
	as "github.com/aerospike/aerospike-client-go"
	"github.com/aerospike/aerospike-client-go/logger"
)

var (
	ErrKeyInvalid = errors.New("Invalid Aerospike Key")
	ErrCmdParams  = errors.New("command params error")
	ErrFieldValue = errors.New("Invalid field value")
	ErrAuthFailed = errors.New("has no auth to access kv servers, please contact the admin")
)

type AerospikeRedisConf struct {
	AerospikeServers []string
	Timeout          int
	UseWhiteList     bool
}

type AerospikeRedisProxy struct {
	sync.RWMutex
	asClient        *as.Client
	asServers       []string
	conf            *AerospikeRedisConf
	proxyStatistics *AerospikeProxyStatistics
	whiteList       *aerospikeWhiteList
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

	if self.whiteList != nil {
		self.whiteList.Stop()
	}
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

	asClientPolicy := as.NewClientPolicy()
	asClientPolicy.FailIfNotConnected = false
	self.asClient, err = as.NewClientWithPolicyAndHost(asClientPolicy, hosts...)
	if err != nil {
		redisLog.Errorf("failed to init aerospike client: %v", err)
		return err
	} else if !self.asClient.IsConnected() {
		redisLog.Errorf("redis2aerospike proxy can't connect to cluster, please check network condition and seed nodes")
	}
	self.asClient.DefaultPolicy.Timeout = time.Second * time.Duration(int64(self.conf.Timeout))
	self.asClient.DefaultWritePolicy.SendKey = true
	self.asClient.DefaultWritePolicy.Expiration = math.MaxUint32

	if self.conf.UseWhiteList {
		self.whiteList, err = NewAerospikeWhiteList(common.GlobalControlCenter)
		if err != nil {
			redisLog.Errorf("init aerospike access white list failed: %v, all access will be authorized", err)
			self.whiteList = nil
		}
	}

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

		if err := self.aerospikeAccessAuth(c.cmd, k, ArgEx); err != nil {
			return err
		} else {
			return f(c, k, w)
		}
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

		if err := self.aerospikeAccessAuth(c.cmd, k, nil); err != nil {
			return err
		} else {
			return f(c, k, fields, w)
		}
	}
}

//may only need to pass the client argument
func (self *AerospikeRedisProxy) aerospikeAccessAuth(cmd string, key *as.Key, argEx [][]byte) error {
	if self.whiteList == nil {
		return nil
	}

	if !self.whiteList.AuthAccess(key) {
		return ErrAuthFailed

	} else if (cmd == "mget" || cmd == "del") && argEx != nil {
		for _, arg := range argEx {
			if key, err := parserRedisKey(string(arg)); err != nil {
				return err
			} else {
				if !self.whiteList.AuthAccess(key) {
					return ErrAuthFailed
				}
			}
		}
	}

	return nil
}

func (self *AerospikeRedisProxy) CheckUsedAsKVDSModule() bool {
	return true
}

func (self *AerospikeRedisProxy) SetUsedAsKVDSModule() error {
	return nil
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
	router.Register("incr", self.wrapParserRedisKey(self.incrCommand))
	router.Register("incrby", self.wrapParserRedisKey(self.incrbyCommand))
	router.Register("decr", self.wrapParserRedisKey(self.decrCommand))
	router.Register("decrby", self.wrapParserRedisKey(self.decrbyCommand))
	router.Register("hget", self.wrapParserRedisKeyAndField(self.hgetCommand))
	router.Register("hgetall", self.wrapParserRedisKeyAndField(self.hgetallCommand))
	router.Register("hmget", self.wrapParserRedisKeyAndField(self.hmgetCommand))
	router.Register("hmset", self.wrapParserRedisKeyAndField(self.hmsetCommand))
	router.Register("hset", self.wrapParserRedisKeyAndField(self.hsetCommand))
	router.Register("hdel", self.wrapParserRedisKeyAndField(self.hdelCommand))
	router.Register("hexists", self.wrapParserRedisKeyAndField(self.hexistsCommand))
	router.Register("hincrby", self.wrapParserRedisKeyAndField(self.hincrbyCommand))
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
