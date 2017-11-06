package common

var (
	ReadCommands  map[string]struct{}
	WriteCommands map[string]struct{}

	UnaryCommands  map[string]struct{}
	MultiCommands  map[string]struct{}
	KVPairCommands map[string]struct{}
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

		"json.get", "json.keyexists", "json.mkget",
		"json.type", "json.arrlen", "json.objlen",

		"geodist", "geohash", "georadius", "georadiusbymember", "geopos",

		"hgetex",
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

		"json.set", "json.del", "json.arrappend", "json.arrpop",

		"geoadd",

		"hsetex", "hdelex",
	} {
		WriteCommands[cmd] = struct{}{}
	}

	UnaryCommands = make(map[string]struct{})
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

		//Json extension.
		"json.set", "json.del", "json.arrappend", "json.arrpop",
		"json.get", "json.keyexists", "json.type", "json.arrlen", "json.objlen",

		//Geohash commands.
		"geodist", "geohash", "georadius", "georadiusbymember", "geopos", "geoadd",

		"hgetex", "hsetex", "hdelex",
	} {
		UnaryCommands[cmd] = struct{}{}
	}

	MultiCommands = make(map[string]struct{})
	for _, cmd := range []string{
		"exists", "mget", "sinter", "sunion", "sdiff",
		"del", "sinterstore", "sdiffstore", "sunionstore",
		"json.mkget",
	} {
		MultiCommands[cmd] = struct{}{}
	}

	KVPairCommands = make(map[string]struct{})
	for _, cmd := range []string{
		"mset",
	} {
		KVPairCommands[cmd] = struct{}{}
	}
}
