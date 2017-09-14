package zrdb

import (
	"bytes"
	"errors"

	"github.com/absolute8511/go-zanredisdb"
)

var (
	ErrKeyInvalid = errors.New("invalid key format for ZanRedisDB")

	KeySep = []byte(":")

	HashCmds = []string{
		"hget", "hgetall", "hkeys", "hexists", "hmget",
		"hlen", "hset", "hmset", "hdel", "hincrby", "hclear",
		"hexpire", "httl", "hpersist",
	}

	ListCmds = []string{
		"lindex", "llen", "lrange", "lpop", "lset",
		"lpush", "ltrim", "rpop", "rpush", "lclear",
		"lexpire", "lttl", "lpersist",
	}
	SetCmds = []string{
		"scard", "sismember", "smembers",
		"sadd", "srem", "sclear", "smclear",
		"sexpire", "sttl", "spersist",
	}

	ZSetCmds = []string{
		"zscore", "zcount", "zcard", "zlexcount", "zrange",
		"zrevrange", "zrangebylex", "zrangebyscore", "zrevrangebyscore",
		"zrank", "zrevrank", "zadd", "zincrby", "zrem", "zremrangebyrank",
		"zremrangebyscore", "zremrangebylex", "zclear",
		"zexpire", "zttl", "zpersist",
	}

	JsonCmds = []string{
		"json.get", "json.keyexists", "json.mkget", "json.type", "json.arrlen", "json.objkeys",
		"json.objlen", "json.set", "json.del", "json.arrappend", "json.arrpop",
	}
)

const (
	DefaultTendInterval = 3
	DefaultReadTimeout  = 3
	DefaultWriteTimeout = 3
	DefaultDialTimeout  = 3

	// max connection number to a back-end node
	DefaultMaxActiveConn = 640

	// max idle connection number to a back-end node
	DefaultMaxIdleConn = 512

	// the idle connection will be closed after 120 seconds
	DefaultIdleTimeout = 120
)

func ParseKey(key []byte) (*zanredisdb.PKey, error) {
	fields := bytes.SplitN(key, KeySep, 3)
	if len(fields) != 3 {
		return nil, ErrKeyInvalid
	} else {
		return zanredisdb.NewPKey(string(fields[0]), string(fields[1]), fields[2]), nil
	}
}
