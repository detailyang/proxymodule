package redisproxy

import (
	"encoding/json"
	"fmt"
	"github.com/siddontang/goredis"
	"io/ioutil"
	"strconv"
	"testing"
	"time"
)

const (
	testProxyAddr     = "127.0.0.1:18818"
	testProxyAddrConn = "192.168.66.202:6666"
)

type testLogger struct {
	tl *testing.T
}

func newTestLogger(t *testing.T) *testLogger {
	return &testLogger{tl: t}
}

func (self *testLogger) Flush() {
}

func (self *testLogger) Output(depth int, s string) {
	self.tl.Log(s)
}

func (self *testLogger) OutputErr(depth int, s string) {
	self.tl.Error(s)
}

func startTestRedisProxy(t *testing.T) *RedisProxy {
	redisLog.Logger = newTestLogger(t)
	configFile := "/tmp/redis-proxy-module-test.conf"
	conf := &AerospikeRedisConf{[]string{"192.168.66.202:3000"}, 10}
	d, _ := json.Marshal(conf)
	err := ioutil.WriteFile(configFile, d, 0666)
	if err != nil {
		t.Errorf("write config failed: %v", err)
		return nil
	}
	rp := NewRedisProxy(testProxyAddr, "redis2aerospike", configFile, nil)
	if rp == nil {
		return nil
	}
	go rp.Start()
	return rp
}

func getTestConn(t *testing.T) *goredis.PoolConn {
	time.Sleep(time.Second * 3)
	testClient := goredis.NewClient(testProxyAddrConn, "")
	testClient.SetMaxIdleConns(2)
	conn, err := testClient.Get()
	if err != nil {
		t.Fatalf("err get conn: %v", err)
	}
	return conn
}

func TestRedisKV(t *testing.T) {
	rp := startTestRedisProxy(t)
	if rp == nil {
		t.Fatal("init proxy server failed.")
		return
	}
	defer rp.Stop()
	c := getTestConn(t)
	if c == nil {
		t.Fatal("init proxy connection failed.")
		return
	}

	defer c.Close()
	asKeyPrefix := "test:test-redis-proxy:"
	if ok, err := goredis.String(c.Do("set", asKeyPrefix+"a", "1234")); err != nil {
		t.Fatal(err)
	} else if ok != "OK" {
		t.Fatal(ok)
	}

	if v, err := goredis.String(c.Do("get", asKeyPrefix+"a")); err != nil {
		t.Fatal(err)
	} else if v != "1234" {
		t.Fatal(v)
	}

	if n, err := goredis.Int(c.Do("exists", asKeyPrefix+"a")); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("exists", asKeyPrefix+"empty_key_test")); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("expire", asKeyPrefix+"a", 5)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}
	if n, err := goredis.Int(c.Do("expire", asKeyPrefix+"b", 1)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}
	if n, err := goredis.Int(c.Do("ttl", asKeyPrefix+"a")); err != nil {
		t.Fatal(err)
	} else if n > 5 {
		t.Fatal(n)
	} else if n < 1 {
		t.Fatal(n)
	}

	time.Sleep(6 * time.Second)
	if n, err := goredis.Int(c.Do("ttl", asKeyPrefix+"a")); err != nil {
		t.Fatal(err)
	} else if n > 0 {
		t.Logf("ttl remain: %v", n)
	}

	if n, err := goredis.Int(c.Do("exists", asKeyPrefix+"a")); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("exists", asKeyPrefix+"b")); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}
}

func TestRedisKVM(t *testing.T) {
	rp := startTestRedisProxy(t)

	if rp == nil {
		t.Fatal("init proxy server failed.")
		return
	}
	defer rp.Stop()
	c := getTestConn(t)

	if c == nil {
		t.Fatal("init proxy connection failed.")
		return
	}
	defer c.Close()

	if c == nil {
		t.Fatal("init proxy connection failed.")
		return
	}
	asKeyPrefix := "test:test-redis-proxy:"
	if ok, err := goredis.String(c.Do("set", asKeyPrefix+"a", "1")); err != nil {
		t.Fatal(err)
	} else if ok != "OK" {
		t.Fatal(ok)
	}

	if ok, err := goredis.String(c.Do("set", asKeyPrefix+"b", "2")); err != nil {
		t.Fatal(err)
	} else if ok != "OK" {
		t.Fatal(ok)
	}

	start := time.Now()
	if v, err := goredis.MultiBulk(c.Do("mget", asKeyPrefix+"a", asKeyPrefix+"b", asKeyPrefix+"c")); err != nil {
		t.Fatal(err)
	} else if len(v) != 3 {
		t.Fatal(len(v))
	} else {
		if vv, ok := v[0].([]byte); !ok || string(vv) != "1" {
			t.Fatalf("not 1: %v", string(vv))
		}

		if vv, ok := v[1].([]byte); !ok || string(vv) != "2" {
			t.Fatalf("not 2: %v", string(vv))
		}

		if v[2] != nil {
			t.Fatal("must nil")
		}
		t.Logf("cost %v", time.Since(start))
	}
}

func TestRedisKVErrorParams(t *testing.T) {
	rp := startTestRedisProxy(t)

	if rp == nil {
		t.Fatal("init proxy server failed.")
		return
	}
	defer rp.Stop()
	c := getTestConn(t)

	if c == nil {
		t.Fatal("init proxy connection failed.")
		return
	}
	defer c.Close()

	if c == nil {
		t.Fatal("init proxy connection failed.")
		return
	}
	asKeyPrefix := "test:test-redis-proxy:"
	if _, err := c.Do("get", asKeyPrefix+"a", asKeyPrefix+"b", asKeyPrefix+"c"); err == nil {
		t.Fatalf("invalid err %v", err)
	}

	if _, err := c.Do("exists", asKeyPrefix+"a", asKeyPrefix+"b"); err == nil {
		t.Fatalf("invalid err %v", err)
	}

	if _, err := c.Do("del"); err == nil {
		t.Fatal("invalid err of %v", err)
	}

	if _, err := c.Do("mget"); err == nil {
		t.Fatal("invalid err of %v", err)
	}

	if _, err := c.Do("expire"); err == nil {
		t.Fatal("invalid err of %v", err)
	}

	if _, err := c.Do("expire", asKeyPrefix+"a", "b"); err == nil {
		t.Fatal("invalid err of %v", err)
	}
	if _, err := c.Do("expire", "a", 1); err == nil {
		t.Fatal("invalid err of %v", err)
	}

	if _, err := c.Do("ttl"); err == nil {
		t.Fatal("invalid err of %v", err)
	}
}

func TestRedisHash(t *testing.T) {
	rp := startTestRedisProxy(t)

	if rp == nil {
		t.Fatal("init proxy server failed.")
		return
	}
	defer rp.Stop()
	c := getTestConn(t)

	if c == nil {
		t.Fatal("init proxy connection failed.")
		return
	}
	defer c.Close()

	key := []byte("test:test-redis-proxy:ha")
	defer func() {
		if _, err := goredis.Int(c.Do("expire", key, 1)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
	}()
	if n, err := goredis.Int(c.Do("exists", key)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}
	if n, err := goredis.Int(c.Do("hset", key, 1, 0)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}
	if n, err := goredis.Int(c.Do("exists", key)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("hexists", key, 1)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("hexists", key, -1)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := goredis.Int(c.Do("hget", key, 1)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if _, err := goredis.Int(c.Do("hset", key, 1, 1)); err != nil {
		t.Fatal(err)
	}

	if n, err := goredis.Int(c.Do("hget", key, 1)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

}

func testRedisHashArrayPair(ay []interface{}, checkValues ...int) error {
	if len(ay) != len(checkValues) {
		return fmt.Errorf("invalid return number %d != %d", len(ay), len(checkValues))
	}
	if len(ay)%2 != 0 {
		return fmt.Errorf("invalid return number, should be pair : %v", len(ay))
	}
	// the get all return may be out of ordered in (field value) pair, so we need to check by field map.
	mapValues := make(map[int]int)
	for index, v := range checkValues {
		mapValues[index] = v
	}

	for i := 0; i < len(ay)/2; i = i + 2 {
		if ay[i] == nil && checkValues[i] != 0 {
			return fmt.Errorf("must nil")
		} else if ay[i] != nil {
			field, ok := ay[i].([]byte)
			var vstr string
			if !ok {
				vstr, ok = ay[i].(string)
				if !ok {
					return fmt.Errorf("invalid return data %d %v :%T", i, ay[i], ay[i])
				}
			} else {
				vstr = string(field)
			}

			fieldV, _ := strconv.Atoi(vstr)
			v, ok := ay[i+1].([]byte)
			if !ok {
				vstr, ok = ay[i+1].(string)
				if !ok {
					return fmt.Errorf("invalid return data %d %v :%T", i+1, ay[i+1], ay[i+1])
				}
			} else {
				vstr = string(v)
			}

			d, _ := strconv.Atoi(vstr)

			if d != checkValues[fieldV] {
				return fmt.Errorf("invalid data %d %s != %d", d, vstr, checkValues[fieldV])
			}
		}
	}
	return nil
}

func testRedisHashArray(ay []interface{}, checkValues ...int) error {
	if len(ay) != len(checkValues) {
		return fmt.Errorf("invalid return number %d != %d", len(ay), len(checkValues))
	}

	for i := 0; i < len(ay); i++ {
		if ay[i] == nil && checkValues[i] != 0 {
			return fmt.Errorf("must nil")
		} else if ay[i] != nil {
			v, ok := ay[i].([]byte)
			var vstr string
			if !ok {
				vstr, ok = ay[i].(string)
				if !ok {
					return fmt.Errorf("invalid return data %d %v :%T", i, ay[i], ay[i])
				}
			} else {
				vstr = string(v)
			}

			d, _ := strconv.Atoi(vstr)

			if d != checkValues[i] {
				return fmt.Errorf("invalid data %d %s != %d", i, vstr, checkValues[i])
			}
		}
	}
	return nil
}

func TestRedisHashM(t *testing.T) {
	rp := startTestRedisProxy(t)

	if rp == nil {
		t.Fatal("init proxy server failed.")
		return
	}
	defer rp.Stop()
	c := getTestConn(t)

	if c == nil {
		t.Fatal("init proxy connection failed.")
		return
	}
	defer c.Close()

	key := []byte("test:test-redis-proxy:hb")
	defer func() {
		if _, err := goredis.Int(c.Do("expire", key, 1)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
	}()

	if ok, err := goredis.String(c.Do("hmset", key, 1, 1, 2, 2, 3, 3)); err != nil {
		t.Fatal(err)
	} else if ok != "OK" {
		t.Fatal(ok)
	}

	if v, err := goredis.MultiBulk(c.Do("hmget", key, 1, 2, 3, 4)); err != nil {
		t.Fatal(err)
	} else {
		if err := testRedisHashArray(v, 1, 2, 3, 0); err != nil {
			t.Fatal(err)
		}
	}

	if n, err := goredis.Int(c.Do("hdel", key, 1)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}
	if n, err := goredis.Int(c.Do("hdel", key, 2)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}
	if n, err := goredis.Int(c.Do("hdel", key, 3)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}
	if n, err := goredis.Int(c.Do("hdel", key, 4)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Logf("delete not exist bin : %v\n", n)
	}

	if v, err := goredis.MultiBulk(c.Do("hmget", key, 1, 2, 3, 4)); err != nil {
		t.Fatal(err)
	} else {
		if err := testRedisHashArray(v, 0, 0, 0, 0); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRedisHashGetAll(t *testing.T) {

	rp := startTestRedisProxy(t)

	if rp == nil {
		t.Fatal("init proxy server failed.")
		return
	}
	defer rp.Stop()
	c := getTestConn(t)

	if c == nil {
		t.Fatal("init proxy connection failed.")
		return
	}
	defer c.Close()

	key := []byte("test:test-redis-proxy:hd")
	defer func() {
		if _, err := goredis.Int(c.Do("expire", key, 1)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
	}()

	if ok, err := goredis.String(c.Do("hmset", key, 1, 1, 2, 2, 3, 3)); err != nil {
		t.Fatal(err)
	} else if ok != "OK" {
		t.Fatal(ok)
	}

	if v, err := goredis.MultiBulk(c.Do("hgetall", key)); err != nil {
		t.Fatal(err)
	} else {
		if err := testRedisHashArrayPair(v, 1, 1, 2, 2, 3, 3); err != nil {
			t.Fatal(err)
		}
	}

}

func TestRedisHashErrorParams(t *testing.T) {

	rp := startTestRedisProxy(t)

	if rp == nil {
		t.Fatal("init proxy server failed.")
		return
	}
	defer rp.Stop()
	c := getTestConn(t)

	if c == nil {
		t.Fatal("init proxy connection failed.")
		return
	}
	defer c.Close()

	key := []byte("test:test-redis-proxy:test_herr")
	if _, err := c.Do("hset", key); err == nil {
		t.Fatal("invalid err of %v", err)
	}

	if _, err := c.Do("hget", key); err == nil {
		t.Fatal("invalid err of %v", err)
	}

	if _, err := c.Do("hexists", key); err == nil {
		t.Fatal("invalid err of %v", err)
	}

	if _, err := c.Do("hdel", key); err == nil {
		t.Fatal("invalid err of %v", err)
	}

	if _, err := c.Do("hincrby", key); err == nil {
		t.Fatal("invalid err of %v", err)
	}

	if _, err := c.Do("hmset", key); err == nil {
		t.Fatal("invalid err of %v", err)
	}

	if _, err := c.Do("hmset", key, "f1", "v1", "f2"); err == nil {
		t.Fatal("invalid err of %v", err)
	}

	if _, err := c.Do("hmget", key); err == nil {
		t.Fatal("invalid err of %v", err)
	}

	if _, err := c.Do("hgetall"); err == nil {
		t.Fatal("invalid err of %v", err)
	}
}
