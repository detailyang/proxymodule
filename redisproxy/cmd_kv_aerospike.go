package redisproxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"

	as "github.com/aerospike/aerospike-client-go"
)

const (
	singleBinName = "redisvalue"
	incrMaxRetry  = 3
)

func writeSingleRecord(w ResponseWriter, bins as.BinMap) {
	var resp []byte
	if d, ok := bins[singleBinName]; ok && len(bins) == 1 {
		switch vt := d.(type) {
		case int:
			resp = []byte(strconv.Itoa(vt))
		case int32:
			resp = []byte(strconv.FormatInt(int64(vt), 10))
		case int64:
			resp = []byte(strconv.FormatInt(vt, 10))
		case string:
			resp = []byte(vt)
		case []byte:
			resp = vt
		default:
			resp, _ = json.Marshal(d)
		}
	} else {
		resp, _ = json.Marshal(bins)
	}
	w.WriteBulk(resp)
}

func (self *AerospikeRedisProxy) getCommand(c *Client, key *as.Key, w ResponseWriter) error {
	if len(c.Args) != 1 {
		return ErrCmdParams
	}
	if v, err := self.asClient.Get(nil, key); err != nil {
		return err
	} else {
		if v == nil {
			w.WriteBulk(nil)
		} else {
			if redisLog.Level() > 2 {
				redisLog.Debugf("get %v, %v", v.Key.String(), v.Bins)
			}
			writeSingleRecord(w, v.Bins)
		}
	}
	return nil
}

func (self *AerospikeRedisProxy) delCommand(c *Client, key *as.Key, w ResponseWriter) error {
	keys := make([]*as.Key, 0, len(c.Args))
	keys = append(keys, key)
	for i := 1; i < len(c.Args); i++ {
		k, err := parserRedisKey(string(c.Args[i]))
		if err != nil {
			return err
		}
		keys = append(keys, k)
	}

	touchPolicy := *self.asClient.DefaultWritePolicy
	touchPolicy.Expiration = 1
	var deleted int64
	for _, key := range keys {
		if err := self.asClient.Touch(&touchPolicy, key); err != nil {
			redisLog.Debugf("delCommand expire key:%v failed, error:%s", key, err.Error())
		} else {
			deleted++
		}
	}

	delPolicy := *self.asClient.DefaultWritePolicy
	for _, key := range keys {
		if _, err := self.asClient.Delete(&delPolicy, key); err != nil {
			redisLog.Debugf("delCommand delete key:%v failed, error:%s", key, err.Error())
		}
	}

	w.WriteInteger(deleted)

	return nil
}

func (self *AerospikeRedisProxy) setCommand(c *Client, key *as.Key, w ResponseWriter) error {
	args := c.Args
	if len(args) != 2 {
		return ErrCmdParams
	}

	// set should never expire, default write policy no expire
	bin := as.NewBin(singleBinName, args[1])
	if err := self.asClient.PutBins(nil, key, bin); err != nil {
		return err
	} else {
		w.WriteString("OK")
	}

	return nil
}

func (self *AerospikeRedisProxy) setexCommand(c *Client, key *as.Key, w ResponseWriter) error {
	args := c.Args
	if len(args) != 3 {
		return ErrCmdParams
	}

	duration, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return ErrFieldValue
	}
	if duration < 1 {
		return ErrCmdParams
	}

	policy := *self.asClient.DefaultWritePolicy
	policy.Expiration = uint32(duration)
	bin := as.NewBin(singleBinName, args[2])
	if err := self.asClient.PutBins(&policy, key, bin); err != nil {
		return err
	} else {
		w.WriteString("OK")
	}

	return nil
}

func (self *AerospikeRedisProxy) existsCommand(c *Client, key *as.Key, w ResponseWriter) error {
	if len(c.Args) != 1 {
		return ErrCmdParams
	}
	if ret, err := self.asClient.Exists(nil, key); err != nil {
		return err
	} else {
		n := 0
		if ret {
			n = 1
		}
		w.WriteInteger(int64(n))
	}

	return nil
}

func (self *AerospikeRedisProxy) mgetCommand(c *Client, key *as.Key, w ResponseWriter) error {
	keys := make([]*as.Key, 0, len(c.Args))
	keys = append(keys, key)
	for i := 1; i < len(c.Args); i++ {
		k, err := parserRedisKey(string(c.Args[i]))
		if err != nil {
			return err
		}
		keys = append(keys, k)
	}

	if v, err := self.asClient.BatchGet(nil, keys); err != nil {
		return err
	} else {
		varry := make([][]byte, len(v))
		for i, r := range v {
			if r == nil {
				varry[i] = nil
			} else {
				var jsondata []byte
				if d, ok := r.Bins[singleBinName]; ok && len(r.Bins) == 1 {
					// json marshal will base64 []byte type, so we need handle
					// it special.
					if vt, ok := d.([]byte); ok {
						jsondata = vt
					} else {
						jsondata, _ = json.Marshal(d)
					}
				} else {
					jsondata, _ = json.Marshal(r.Bins)
				}
				if redisLog.Level() > 2 {
					redisLog.Debugf("batch get %v, %v, %v", r.Key.String(), r.Bins, string(jsondata))
				}
				varry[i] = jsondata
			}
		}
		w.WriteSliceArray(varry)
		if redisLog.Level() > 2 {
			redisLog.Debugf("batch get write response done")
		}
	}

	return nil
}

func (self *AerospikeRedisProxy) expireCommand(c *Client, key *as.Key, w ResponseWriter) error {
	args := c.Args
	if len(args) != 2 {
		return ErrCmdParams
	}

	duration, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return ErrFieldValue
	}
	if duration < 1 {
		return ErrCmdParams
	}

	touchPolicy := *self.asClient.DefaultWritePolicy
	touchPolicy.Expiration = uint32(duration)
	if err := self.asClient.Touch(&touchPolicy, key); err != nil {
		redisLog.Debugf("touch failed: %v", err)
		w.WriteInteger(0)
	} else {
		w.WriteInteger(1)
	}

	return nil
}

func (self *AerospikeRedisProxy) ttlCommand(c *Client, key *as.Key, w ResponseWriter) error {

	if len(c.Args) != 1 {
		return ErrCmdParams
	}

	if v, err := self.asClient.GetHeader(nil, key); err != nil {
		w.WriteInteger(-1)
		redisLog.Debugf("touch failed: %v", err)
	} else {
		if v == nil {
			w.WriteInteger(-1)
		} else {
			w.WriteInteger(int64(v.Expiration))
		}
	}

	return nil
}

func (self *AerospikeRedisProxy) infoCommand(c *Client, w ResponseWriter) error {
	var info bytes.Buffer

	info.Write(self.proxyStatistics.GenInfoBytes())

	if self.whiteList != nil {
		info.WriteString("\r\n")
		info.Write(self.whiteList.GenInfoBytes())
	}

	w.WriteBulk(info.Bytes())
	return nil
}

func (self *AerospikeRedisProxy) incrCommand(c *Client, key *as.Key, w ResponseWriter) error {
	args := c.Args
	if len(args) != 1 {
		return ErrCmdParams
	}

	if v, err := self.increase(key, singleBinName, 1); err != nil {
		return fmt.Errorf("incr [%s] execute failed, %s", string(args[0]), err.Error())
	} else {
		w.WriteInteger(v)
	}

	return nil
}

func (self *AerospikeRedisProxy) decrCommand(c *Client, key *as.Key, w ResponseWriter) error {
	args := c.Args
	if len(args) != 1 {
		return ErrCmdParams
	}

	if v, err := self.increase(key, singleBinName, -1); err != nil {
		return fmt.Errorf("decr [%s] execute failed, %s", string(args[0]), err.Error())
	} else {
		w.WriteInteger(v)
	}

	return nil
}

func (self *AerospikeRedisProxy) incrbyCommand(c *Client, key *as.Key, w ResponseWriter) error {
	args := c.Args
	if len(args) != 2 {
		return ErrCmdParams
	}

	increment, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return err
	}

	if v, err := self.increase(key, singleBinName, increment); err != nil {
		return fmt.Errorf("incrby [%s] execute failed, %s", string(args[0]), err.Error())
	} else {
		w.WriteInteger(v)
	}

	return nil
}

func (self *AerospikeRedisProxy) decrbyCommand(c *Client, key *as.Key, w ResponseWriter) error {
	args := c.Args
	if len(args) != 2 {
		return ErrCmdParams
	}

	decrement, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return err
	}

	if v, err := self.increase(key, singleBinName, -decrement); err != nil {
		return fmt.Errorf("decrby [%s] execute failed, %s", string(args[0]), err.Error())
	} else {
		w.WriteInteger(v)
	}

	return nil
}

func (self *AerospikeRedisProxy) increase(key *as.Key, binName string, increment int64) (int64, error) {
	var preVal int64

	incrPolicy := *self.asClient.DefaultWritePolicy
	incrPolicy.GenerationPolicy = as.EXPECT_GEN_EQUAL

	for i := 0; i < incrMaxRetry; i++ {
		if v, err := self.asClient.Get(nil, key, binName); err != nil {
			return 0, err
		} else if v != nil {
			if binValue, ok := v.Bins[binName]; ok {
				switch vt := binValue.(type) {
				case int:
					preVal = int64(vt)
				case int32:
					preVal = int64(vt)
				case int64:
					preVal = vt
				case []byte:
					preVal, err = strconv.ParseInt(string(vt), 10, 64)
				case string:
					preVal, err = strconv.ParseInt(vt, 10, 64)
				default:
					return 0, fmt.Errorf("ERR, value type [%v] is not an integer", reflect.TypeOf(binValue).Name())
				}
			}
			if err != nil {
				return 0, errors.New("ERR, value type is not an integer")
			}
			incrPolicy.Generation = v.Generation
		}

		bin := as.NewBin(binName, []byte(strconv.FormatInt(preVal+increment, 10)))

		if err := self.asClient.PutBins(&incrPolicy, key, bin); err != nil {
			redisLog.Infof("incrCommand execute [%v] failed: %v", *key, err)
		} else {
			return preVal + increment, nil
		}
	}

	return 0, errors.New("ERR, too many simultaneously operations on the key")
}
