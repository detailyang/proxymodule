package redisproxy

import (
	"encoding/json"
	"strconv"

	as "github.com/aerospike/aerospike-client-go"
)

const (
	singleBinName = "redisvalue"
)

func writeSingleRecord(w ResponseWriter, bins as.BinMap) {
	if d, ok := bins[singleBinName]; ok && len(bins) == 1 {
		switch vt := d.(type) {
		case int64:
			w.WriteInteger(vt)
		case string:
			w.WriteString(vt)
		case []byte:
			w.WriteBulk(vt)
		default:
			jsondata, _ := json.Marshal(d)
			w.WriteBulk(jsondata)
		}
	} else {
		jsondata, _ := json.Marshal(bins)
		w.WriteBulk(jsondata)
	}
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

func (self *AerospikeRedisProxy) incrCommand(c *Client) error {
	return nil
}

func (self *AerospikeRedisProxy) decrCommand(c *Client) error {
	return nil
}

func (self *AerospikeRedisProxy) incrbyCommand(c *Client) error {
	return nil
}

func (self *AerospikeRedisProxy) decrbyCommand(c *Client) error {
	return nil
}
