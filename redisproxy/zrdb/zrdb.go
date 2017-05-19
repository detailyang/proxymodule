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
	}

	ListCmds = []string{
		"lindex", "llen", "lrange", "lpop", "lset",
		"lpush", "ltrim", "rpop", "rpush", "lclear",
	}
	SetCmds = []string{
		"scard", "sismember", "smembers",
		"sadd", "srem", "sclear", "smclear",
	}

	ZSetCmds = []string{
		"zscore", "zcount", "zcard", "zlexcount", "zrange",
		"zrevrange", "zrangebylex", "zrangebyscore", "zrevrangebyscore",
		"zrank", "zrevrank", "zadd", "zincrby", "zrem", "zremrangebyrank",
		"zremrangebyscore", "zremrangebylex", "zclear",
	}
)

const (
	DefaultTendInterval = 3
	DefaultReadTimeout  = 1
	DefaultWriteTimeout = 1
	DefaultDialTimeout  = 2
)

func ParseKey(key []byte) (*zanredisdb.PKey, error) {
	fields := bytes.SplitN(key, KeySep, 3)
	if len(fields) != 3 {
		return nil, ErrKeyInvalid
	} else {
		return zanredisdb.NewPKey(string(fields[0]), string(fields[1]), fields[2]), nil
	}
}
