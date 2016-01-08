package redisproxy

import (
	"errors"
	as "github.com/aerospike/aerospike-client-go"
	"github.com/aerospike/aerospike-client-go/logger"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
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
	asClient  *as.Client
	asServers []string
	conf      *AerospikeRedisConf
}

func CreateRedis2AerospikeProxy() RedisProxyModule {
	return &AerospikeRedisProxy{
		asServers: make([]string, 0),
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
	return nil
}

func (self *AerospikeRedisProxy) RegisterCmd(router *CmdRouter) {
	router.Register("get", wrapParserRedisKey(self.getCommand))
	router.Register("set", wrapParserRedisKey(self.setCommand))
	router.Register("setex", wrapParserRedisKey(self.setexCommand))
	router.Register("exists", wrapParserRedisKey(self.existsCommand))
	router.Register("mget", wrapParserRedisKey(self.mgetCommand))
	router.Register("expire", wrapParserRedisKey(self.expireCommand))
	router.Register("ttl", wrapParserRedisKey(self.ttlCommand))
	router.Register("hget", wrapParserRedisKeyAndField(self.hgetCommand))
	router.Register("hgetall", wrapParserRedisKeyAndField(self.hgetallCommand))
	router.Register("hmget", wrapParserRedisKeyAndField(self.hmgetCommand))
	router.Register("hmset", wrapParserRedisKeyAndField(self.hmsetCommand))
	router.Register("hset", wrapParserRedisKeyAndField(self.hsetCommand))
	router.Register("hdel", wrapParserRedisKeyAndField(self.hdelCommand))
	router.Register("hexists", wrapParserRedisKeyAndField(self.hexistsCommand))
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

func wrapParserRedisKey(f AsCommandFunc) CommandFunc {
	return func(c *Client, w ResponseWriter) error {
		if len(c.Args) == 0 {
			return ErrCmdParams
		}
		k, err := parserRedisKey(string(c.Args[0]))
		if err != nil {
			return err
		}
		return f(c, k, w)
	}
}

func wrapParserRedisKeyAndField(f AsCommandFuncWithBins) CommandFunc {
	return func(c *Client, w ResponseWriter) error {
		if len(c.Args) == 0 {
			return ErrCmdParams
		}
		k, fields, err := parserRedisKeyAndFields(c.Args)
		if err != nil {
			return err
		}
		return f(c, k, fields, w)
	}
}
