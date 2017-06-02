package kvds

import (
	"errors"
	"strings"

	"github.com/absolute8511/proxymodule/common"
)

var (
	log *common.LevelLogger

	ReadCommands  map[string]struct{}
	WriteCommands map[string]struct{}

	unaryCommands  map[string]struct{}
	multiCommands  map[string]struct{}
	kvPairCommands map[string]struct{}
)

var (
	ErrKVDSKeyInvalid = errors.New("the format of the key is invalid for KVDS")
)

const (
	KeySep = ":"
)

func init() {
	ReadCommands = make(map[string]struct{})
	for _, cmd := range []string{
		"get", "mget", "exists", "ttl", "hget",
		"hgetall", "hmget", "hexists", "lrange", "llen",
		"zrangebyscore", "zrange", "zrevrange", "zrevrangebyscore",
		"zcard", "zrank", "zrevrank", "sismember",
		"sinter", "sunion", "sdiff", "smembers", "scard",
		"srandmember", "zcount",

		"hkeys", "hlen", "lindex", "zscore", "zlexcount",
		"zrangebylex", "lttl", "httl", "sttl", "zttl",
	} {
		ReadCommands[cmd] = struct{}{}
	}

	WriteCommands = make(map[string]struct{})
	for _, cmd := range []string{
		"setnx", "del", "set", "setex", "expire",
		"incr", "incrby", "decr", "decrby", "hmset",
		"hset", "hdel", "hincrby", "mset", "rpush", "getset",
		"lpush", "lrem", "sadd", "srem", "sinterstore", "sdiffstore", "sunionstore",
		"spop", "zadd", "zremrangebyscore", "zrem",
		"hclear", "lpop", "lset", "ltrim", "rpop", "lclear", "sclear", "smclear",
		"zincrby", "zremrangebylex", "zclear", "zremrangebyrank",
		"persist", "lpersist", "hpersist", "spersist", "zpersist",
		"expire", "lexpire", "hexpire", "sexpire", "zexpire",
	} {
		WriteCommands[cmd] = struct{}{}
	}

	//category of commands according to the usage of KeyTransfer
	unaryCommands = make(map[string]struct{})
	for _, cmd := range []string{
		"get", "set", "ttl", "hset", "hgetall", "hmget", "hexists",
		"getset", "lrange", "llen", "zrangebyscore", "zrange", "zrevrange",
		"zrevrangebyscore", "zcard", "zrank", "zrevrank", "sismember",
		"smembers", "scard", "srandmember", "zcount",
		"setnx", "setex", "expire", "incr", "incrby", "decr", "decrby",
		"hmset", "hset", "hdel", "hincrby", "hget", "rpush", "getset",
		"lpush", "lrem", "sadd", "srem", "spop", "zadd", "zrem", "zremrangebyscore",
		"hkeys", "hlen", "lindex", "zscore", "zlexcount",
		"zrangebylex",
		"hclear", "lpop", "lset", "ltrim", "rpop", "lclear", "sclear", "smclear",
		"zincrby", "zremrangebylex", "zclear", "zremrangebyrank",
		"lttl", "httl", "sttl", "zttl", "persist", "hpersist", "lpersist", "spersist", "zpersist",
		"hexpire", "lexpire", "sexpire", "zexpire",
	} {
		unaryCommands[cmd] = struct{}{}
	}

	multiCommands = make(map[string]struct{})
	for _, cmd := range []string{
		"exists", "mget", "sinter", "sunion", "sdiff",
		"del", "sinterstore", "sdiffstore", "sunionstore",
	} {
		multiCommands[cmd] = struct{}{}
	}

	kvPairCommands = make(map[string]struct{})
	for _, cmd := range []string{
		"mset",
	} {
		kvPairCommands[cmd] = struct{}{}
	}

}

func SetLogger(l *common.LevelLogger) {
	log = l
}

type KVDSKey struct {
	Namespace  string
	Table      string
	PrimaryKey string
}

func ParseRedisKey(redisKey string) (*KVDSKey, error) {
	fields := strings.SplitN(redisKey, KeySep, 3)
	if len(fields) < 3 {
		return nil, ErrKVDSKeyInvalid
	} else {
		return &KVDSKey{
			Namespace:  fields[0],
			Table:      fields[1],
			PrimaryKey: fields[2],
		}, nil
	}
}

type DummyRespWriter struct {
}

func (w *DummyRespWriter) WriteError(error) error {
	return nil
}

func (w *DummyRespWriter) WriteString(string) error {
	return nil
}

func (w *DummyRespWriter) WriteInteger(int64) error {
	return nil
}

func (w *DummyRespWriter) WriteBulk([]byte) error {
	return nil
}

func (w *DummyRespWriter) WriteArray([]interface{}) error {
	return nil
}

func (w *DummyRespWriter) WriteSliceArray([][]byte) error {
	return nil
}

func (w *DummyRespWriter) WriteRawBytes([]byte) error {
	return nil
}

func (w *DummyRespWriter) Flush() error {
	return nil
}
