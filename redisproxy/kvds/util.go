package kvds

import (
	"errors"
	"strings"

	"github.com/absolute8511/proxymodule/common"
)

var (
	log *common.LevelLogger
)

var (
	ErrKVDSKeyInvalid         = errors.New("the format of the key is invalid for KVDS")
	ErrRWAcrossNamespaceTable = errors.New("can not read-write keys across namespaces or tables")
	ErrCmdParamsLength        = errors.New("command params is empty")
)

const (
	KeySep = ":"
)

func SetLogger(l *common.LevelLogger) {
	log = l
}

type KVDSKey struct {
	Namespace  string
	Table      string
	PrimaryKey string
}

func ParseRedisKey(redisKey string) (*KVDSKey, error) {
	fields := strings.SplitN(redisKey, KeySep, 3)
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

func (w *DummyRespWriter) WriteRawBytes([]byte) error {
	return nil
}

func (w *DummyRespWriter) Flush() error {
	return nil

}

/*
All commands should not read or write keys across namespaces or tables.
*/
func ExtractNamespceTable(cmd string, Args [][]byte) (namespace string, table string, err error) {
	if len(Args) == 0 {
		err = ErrCmdParamsLength
		return
	}
	var key *KVDSKey
	if _, ok := common.UnaryCommands[cmd]; ok {
		if key, err = ParseRedisKey(string(Args[0])); err == nil {
			namespace, table = key.Namespace, key.Table
		}
		return
	}
	for i, Arg := range Args {
		if _, ok := common.KVPairCommands[cmd]; ok && i%2 != 0 {
			continue
		}
		if key, err = ParseRedisKey(string(Arg)); err != nil {
			break
		} else if namespace == "" || table == "" {
			namespace, table = key.Namespace, key.Table
		} else if namespace != key.Namespace || table != key.Table {
			namespace, table = "", ""
			err = ErrRWAcrossNamespaceTable
			break
		}
	}
	return
}
