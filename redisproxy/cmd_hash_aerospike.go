package redisproxy

import (
	as "github.com/aerospike/aerospike-client-go"
	"strconv"
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
			return nil
		}
		d := v.Bins[binName]
		switch vt := d.(type) {
		case string:
			w.WriteString(vt)
		case []byte:
			w.WriteBulk(vt)
		case int64:
			w.WriteInteger(vt)
		case int32:
			w.WriteInteger(int64(vt))
		case int:
			w.WriteInteger(int64(vt))
		case nil:
			w.WriteBulk(nil)
		default:
			return ErrFieldValue
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
	if len(args) != 2 {
		return ErrCmdParams
	}
	delBin := as.NewBin(string(args[1]), nil)

	if err := self.asClient.PutBins(nil, key, delBin); err != nil {
		w.WriteInteger(0)
	} else {
		w.WriteInteger(1)
	}

	return nil
}

func (self *AerospikeRedisProxy) hincrbyCommand(c *Client, key *as.Key, bins []*as.Bin, w ResponseWriter) error {
	args := c.Args
	if len(args) != 3 {
		return ErrCmdParams
	}

	delta, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return ErrFieldValue
	}

	w.WriteInteger(int64(delta))
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
