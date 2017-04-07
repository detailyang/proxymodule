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

	unaryCommands map[string]struct{}
	multiCommands map[string]struct{}
	hashCommands  map[string]struct{}
)

var (
	ErrKVDSKeyInvalid = errors.New("the format of the key is invalid for KVDS")
)

/*
the supported commands:
	"get", "mget", "setnx", "del",
	"set", "setex", "exists", "expire",
	"ttl", "incr", "incrby", "decr",
	"decrby", "hget", "hgetall", "hmget",
	"hmset", "hset", "hdel", "hexists",
	"hincrby", "mset", "rpush", "getset",
	"lpush", "lrange", "llen", "lrem",
	"zrangebyscore", "zrange", "zrevrange", "zrevrangebyscore",
	"zcard", "zrank", "zrevrank", "sadd",
	"srem", "sismember", "sinterstore", "sdiffstore",
	"sinter", "sunion", "ssize", "keys",
	"sdiff", "smembers", "spop", "scard",
	"srandmember", "zadd", "zremrangebyscore", "zrem",
	"zcount",
*/

func init() {
	ReadCommands = make(map[string]struct{})
	for _, cmd := range []string{
		"get", "mget", "exists", "ttl", "hget",
		"hgetall", "hmget", "hexists", "getset", "lrange", "llen",
		"zrangebyscore", "zrange", "zrevrange", "zrevrangebyscore",
		"zcard", "zrank", "zrevrank", "sismember",
		"sinter", "sunion", "ssize", "keys", "sdiff", "smembers", "scard",
		"srandmember", "zcount",
	} {
		ReadCommands[cmd] = struct{}{}
	}

	WriteCommands = make(map[string]struct{})
	for _, cmd := range []string{
		"setnx", "del", "set", "setex", "expire",
		"incr", "incrby", "decr", "decrby", "hmset",
		"hset", "hdel", "hincrby", "mset", "rpush", "getset",
		"lpush", "lrem", "sadd", "srem", "sinterstore", "sdiffstore",
		"spop", "zadd", "zremrangebyscore", "zrem",
	} {
		WriteCommands[cmd] = struct{}{}
	}

	//category of commands according to the usage of KeyTransfer
	unaryCommands = make(map[string]struct{})
	for _, cmd := range []string{
		"get", "set", "ttl", "hget", "hset", "hgetall", "hmget", "hexists",
		"getset",
	} {
		unaryCommands[cmd] = struct{}{}
	}

	multiCommands = make(map[string]struct{})
	for _, cmd := range []string{
		"exists", "mget",
	} {
		multiCommands[cmd] = struct{}{}
	}

	hashCommands = make(map[string]struct{})
	for _, cmd := range []string{
		"mset",
	} {
		hashCommands[cmd] = struct{}{}
	}

	//part of commands will cause => "liveMigration"
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
	fields := strings.SplitN(redisKey, ":", 3)
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

func (w *DummyRespWriter) Flush() error {
	return nil
}
