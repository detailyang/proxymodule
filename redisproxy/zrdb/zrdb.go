package zrdb

import (
	"bytes"
	"errors"

	"github.com/absolute8511/go-zanredisdb"
)

var (
	ErrKeyInvalid = errors.New("invalid key format for ZanRedisDB")

	keySep = []byte(":")
)

const (
	DefaultTendInterval = 3
	DefaultReadTimeout  = 1
	DefaultWriteTimeout = 1
	DefaultDialTimeout  = 2
)

func ParseKey(key []byte) (*zanredisdb.PKey, error) {
	fields := bytes.SplitN(key, keySep, 3)
	if len(fields) != 3 {
		return nil, ErrKeyInvalid
	} else {
		return zanredisdb.NewPKey(string(fields[0]), string(fields[1]), fields[2]), nil
	}
}
