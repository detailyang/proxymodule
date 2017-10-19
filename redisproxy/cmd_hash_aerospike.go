package redisproxy

import (
	"fmt"
	"strconv"

	as "github.com/aerospike/aerospike-client-go"
)

func (self *AerospikeRedisProxy) hsetCommand(c *Client, key *as.Key, bins []*as.Bin, w ResponseWriter) error {
	args := c.Args
	if len(args) != 3 {
		return ErrCmdParams
	}
	if len(bins) != 1 {
		return ErrCmdParams
	}
	if err := self.asClient.PutBins(nil, key, bins...); err != nil {
		return err
	} else {
		w.WriteInteger(1)
	}
	return nil
}

func (self *AerospikeRedisProxy) hsetexCommand(c *Client, key *as.Key, bins []*as.Bin, w ResponseWriter) error {
	args := c.Args
	if len(args) != 4 {
		return ErrCmdParams
	}
	if len(bins) != 1 {
		return ErrCmdParams
	}
	duration, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return ErrCmdParams
	}
	if duration < 1 {
		return ErrCmdParams
	}

	policy := *self.asClient.DefaultWritePolicy
	policy.Expiration = uint32(duration)

	if err := self.asClient.PutBins(&policy, key, bins...); err != nil {
		return err
	} else {
		w.WriteInteger(1)
	}
	return nil
}

func (self *AerospikeRedisProxy) hdelexCommand(c *Client, key *as.Key, bins []*as.Bin, w ResponseWriter) error {
	args := c.Args
	if len(args) != 3 {
		return ErrCmdParams
	}
	delBin := as.NewBin(string(args[2]), nil)

	duration, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return ErrCmdParams
	}
	if duration < 1 {
		return ErrCmdParams
	}

	touchPolicy := *self.asClient.DefaultWritePolicy
	touchPolicy.Expiration = uint32(duration)

	_, err = self.asClient.Operate(&touchPolicy, key, as.PutOp(delBin), as.TouchOp())
	if err != nil {
		w.WriteInteger(0)
	} else {
		w.WriteInteger(1)
	}

	return nil
}

func (self *AerospikeRedisProxy) hgetexCommand(c *Client, key *as.Key, bins []*as.Bin, w ResponseWriter) error {
	args := c.Args
	if len(args) != 3 {
		return ErrCmdParams
	}
	duration, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return ErrCmdParams
	}
	if duration < 1 {
		return ErrCmdParams
	}
	binName := string(args[2])
	touchPolicy := *self.asClient.DefaultWritePolicy
	touchPolicy.Expiration = uint32(duration)

	v, err := self.asClient.Operate(&touchPolicy, key, as.TouchOp(), as.GetOpForBin(binName))
	if err != nil {
		return err
	} else {
		if v == nil {
			w.WriteBulk(nil)
		} else {
			var resp []byte
			d := v.Bins[binName]
			switch vt := d.(type) {
			case string:
				resp = []byte(vt)
			case []byte:
				resp = vt
			case int64:
				resp = []byte(strconv.FormatInt(vt, 10))
			case int32:
				resp = []byte(strconv.FormatInt(int64(vt), 10))
			case int:
				resp = []byte(strconv.Itoa(vt))
			case nil:
				resp = nil
			default:
				return ErrFieldValue
			}
			w.WriteBulk(resp)
		}
	}

	return nil
}

func (self *AerospikeRedisProxy) hgetCommand(c *Client, key *as.Key, bins []*as.Bin, w ResponseWriter) error {
	args := c.Args
	if len(args) != 2 {
		return ErrCmdParams
	}

	binName := string(args[1])
	if v, err := self.asClient.Get(nil, key, binName); err != nil {
		return err
	} else {
		if v == nil {
			w.WriteBulk(nil)
		} else {
			var resp []byte
			d := v.Bins[binName]
			switch vt := d.(type) {
			case string:
				resp = []byte(vt)
			case []byte:
				resp = vt
			case int64:
				resp = []byte(strconv.FormatInt(vt, 10))
			case int32:
				resp = []byte(strconv.FormatInt(int64(vt), 10))
			case int:
				resp = []byte(strconv.Itoa(vt))
			case nil:
				resp = nil
			default:
				return ErrFieldValue
			}
			w.WriteBulk(resp)
		}
	}

	return nil
}

func (self *AerospikeRedisProxy) hexistsCommand(c *Client, key *as.Key, bins []*as.Bin, w ResponseWriter) error {
	args := c.Args
	if len(args) != 2 {
		return ErrCmdParams
	}
	binName := string(args[1])
	if v, err := self.asClient.Get(nil, key, binName); err != nil {
		return err
	} else {
		if v == nil {
			w.WriteInteger(0)
		} else {
			if _, ok := v.Bins[binName]; ok {
				w.WriteInteger(1)
			} else {
				w.WriteInteger(0)
			}
		}
	}
	return nil
}

func (self *AerospikeRedisProxy) hdelCommand(c *Client, key *as.Key, bins []*as.Bin, w ResponseWriter) error {
	args := c.Args
	if len(args) < 2 {
		return ErrCmdParams
	}
	delBins := make([]*as.Bin, 0, len(args[1:]))
	for _, arg := range args[1:] {
		delBin := as.NewBin(string(arg), nil)
		delBins = append(delBins, delBin)
	}

	if err := self.asClient.PutBins(nil, key, delBins...); err != nil {
		w.WriteInteger(0)
	} else {
		w.WriteInteger(int64(len(delBins)))
	}

	return nil
}

func (self *AerospikeRedisProxy) hincrbyCommand(c *Client, key *as.Key, bins []*as.Bin, w ResponseWriter) error {
	args := c.Args
	if len(args) != 3 {
		return ErrCmdParams
	}

	increment, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return err
	}

	if v, err := self.increase(key, string(args[1]), increment); err != nil {
		return fmt.Errorf("hincrby [%s, %s] execute failed, %s", string(args[0]), string(args[1]), err.Error())
	} else {
		w.WriteInteger(v)
	}

	return nil
}

func (self *AerospikeRedisProxy) hmsetCommand(c *Client, key *as.Key, bins []*as.Bin, w ResponseWriter) error {
	args := c.Args
	if len(args) < 3 {
		return ErrCmdParams
	}

	if len(args[1:])%2 != 0 {
		return ErrCmdParams
	}

	if err := self.asClient.PutBins(nil, key, bins...); err != nil {
		return err
	} else {
		w.WriteString("OK")
	}

	return nil
}

func (self *AerospikeRedisProxy) hmgetCommand(c *Client, key *as.Key, bins []*as.Bin, w ResponseWriter) error {
	args := c.Args
	if len(args) < 2 {
		return ErrCmdParams
	}

	args = args[1:]
	binNames := make([]string, 0, len(args))
	for _, arg := range args {
		binNames = append(binNames, string(arg))
	}
	if v, err := self.asClient.Get(nil, key, binNames...); err != nil {
		return err
	} else {
		fields := make([]interface{}, 0, len(binNames))
		for _, field := range binNames {
			if v == nil {
				fields = append(fields, nil)
			} else {
				fields = append(fields, v.Bins[field])
			}
		}
		w.WriteArray(fields)
	}
	return nil
}

func (self *AerospikeRedisProxy) hgetallCommand(c *Client, key *as.Key, bins []*as.Bin, w ResponseWriter) error {
	args := c.Args
	if len(args) != 1 {
		return ErrCmdParams
	}

	if v, err := self.asClient.Get(nil, key); err != nil {
		return err
	} else {
		var kvlist []interface{}
		if v != nil {
			kvlist = make([]interface{}, 0, len(v.Bins)*2)
			for name, bin := range v.Bins {
				kvlist = append(kvlist, name)
				kvlist = append(kvlist, bin)
			}
		}
		w.WriteArray(kvlist)
	}

	return nil
}
