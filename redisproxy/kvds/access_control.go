package kvds

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"runtime"
	"sync"

	ds "gitlab.qima-inc.com/wangjian/go-dcc-sdk"

	"github.com/absolute8511/proxymodule/common"
)

const (
	dccApp      = "KVDS"
	dccReadKey  = "read.rule"
	dccWriteKey = "write.rule"

	gradationWildCard = "*"
)

var (
	localHost    = "unknown host"
	localIPAddrs = []string{"127.0.0.1"}

	replaceWildCard = []byte("%s")
	keySep          = []byte(":")
)

func init() {
	if name, err := os.Hostname(); err == nil {
		localHost = name
	}

	if inAddrs, err := net.InterfaceAddrs(); err == nil {
		for _, inAddr := range inAddrs {
			localIPAddrs = append(localIPAddrs, inAddr.String())
		}
	}
}

type KeyTransfer struct {
	Table      string
	PrimaryKey string
}

//Transform the format of args according to the cmd
//TODO
//1.should transform all keys used by the commands which can operate multiple keys simultaneously
//2.may use a pool of byte slice to reduce the cost of GC
func (transfer *KeyTransfer) Transform(cmd string, args [][]byte) [][]byte {
	defer func() {
		if e := recover(); e != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			buf = buf[0:n]
			if log != nil {
				log.Errorf("panic at transform key for command:%s, stack:%s", cmd, string(buf))
			}
		}
	}()

	newArgs := make([][]byte, len(args))
	copy(newArgs, args)

	if 0 != len(transfer.Table) || len(transfer.PrimaryKey) != 0 {
		if _, ok := unaryCommands[cmd]; ok {
			newArgs[0] = transfer.transferSingleKey(args[0])
		} else if _, ok := multiCommands[cmd]; ok {
			for i, arg := range args {
				newArgs[i] = transfer.transferSingleKey(arg)
			}
		} else if _, ok := kvPairCommands[cmd]; ok {
			if len(args)%2 == 0 {
				for i := 0; i < len(args)/2; i++ {
					newArgs[i*2] = transfer.transferSingleKey(args[i*2])
				}
			}
		}
	}
	return newArgs
}

func (transfer *KeyTransfer) transferSingleKey(key []byte) []byte {
	fields := bytes.SplitN(key, keySep, 3)
	if len(transfer.Table) > 0 {
		fields[1] = bytes.Replace([]byte(transfer.Table), replaceWildCard, fields[1], -1)
	}
	if len(transfer.PrimaryKey) > 0 {
		fields[2] = bytes.Replace([]byte(transfer.PrimaryKey), replaceWildCard, fields[2], -1)
	}
	return bytes.Join(fields, keySep)
}

type Cluster struct {
	Name        string
	KeyTransfer KeyTransfer
}

func (cluster *Cluster) Empty() bool {
	return cluster.Name == ""
}

type RWBaseRule struct {
	PreCluster Cluster
	CurCluster Cluster
}

type WriteRule struct {
	RWBaseRule
}

type ReadRule struct {
	RWBaseRule

	//the key of the map is designed to be the host name
	Gradation map[string]GradationStrategy
}

//针对单个host的灰度策略
type GradationStrategy struct {
	Percent float64
}

func (self *ReadRule) gradationFilter() *RWBaseRule {
	if len(self.Gradation) == 0 {
		//no gradation strategy
		return &RWBaseRule{
			PreCluster: self.PreCluster,
			CurCluster: self.CurCluster,
		}
	} else {
		var percent float64
		percent = self.Gradation[gradationWildCard].Percent
		if v, ok := self.Gradation[localHost]; ok {
			percent = v.Percent
		}
		if rand.Float64() < percent/100 {
			return &RWBaseRule{
				PreCluster: self.PreCluster,
				CurCluster: self.CurCluster,
			}
		} else {
			return &RWBaseRule{
				CurCluster: self.PreCluster,
			}
		}
	}
}

type AccessControl struct {
	sync.Mutex
	tableReadRule  map[string]*ReadRule
	tableWriteRule map[string]*WriteRule
}

func NewAccessControl(cc *common.ControlCenter) (*AccessControl, error) {
	if cc == nil {
		return nil, errors.New("the control center for proxy modules has not been initialized")
	}

	ac := &AccessControl{
		tableReadRule:  make(map[string]*ReadRule),
		tableWriteRule: make(map[string]*WriteRule),
	}

	cc.Register(dccApp, dccWriteKey, ac.updateRWRule)
	cc.Register(dccApp, dccReadKey, ac.updateRWRule)

	return ac, nil
}

func (ac *AccessControl) updateRWRule(e *common.CCEvent) {
	if e.Err != nil || len(e.Rsp.RspList) != 1 ||
		e.Rsp.RspList[0].Type != ds.ValueTypeCombination {
		return
	}

	ccRsp := e.Rsp.RspList[0]
	if ccRsp.Key == dccReadKey {
		readRule := make(map[string]*ReadRule)
		for _, v := range ccRsp.CombVal {
			rule := &ReadRule{}
			if err := json.Unmarshal([]byte(v.Value), rule); err != nil {
				if log != nil {
					log.Errorf("kvds access control update table read rule err:%s", err.Error())
				}
			} else {
				readRule[v.Key] = rule
				if log != nil {
					log.Infof("add read rule for table:%s ,rule:%v", v.Key, rule)
				}
			}
		}
		ac.Mutex.Lock()
		ac.tableReadRule = readRule
		ac.Mutex.Unlock()
	} else if ccRsp.Key == dccWriteKey {
		writeRule := make(map[string]*WriteRule)
		for _, v := range ccRsp.CombVal {
			rule := &WriteRule{}
			if err := json.Unmarshal([]byte(v.Value), rule); err != nil {
				if log != nil {
					log.Errorf("kvds access control update table write rule err:%s", err.Error())
				}
			} else {
				writeRule[v.Key] = rule
				if log != nil {
					log.Infof("add write rule for table:%s ,rule:%v", v.Key, rule)
				}
			}
		}
		ac.Mutex.Lock()
		ac.tableWriteRule = writeRule
		ac.Mutex.Unlock()
	}
}

func (ac *AccessControl) GetReadRule(key *KVDSKey) (*RWBaseRule, error) {
	ac.Mutex.Lock()
	readRule := ac.tableReadRule
	ac.Mutex.Unlock()

	if rule, ok := readRule[ruleLookupKey(key)]; ok {
		if rule.CurCluster.Empty() && rule.PreCluster.Empty() {
			return nil, fmt.Errorf("no kvds clusters can be used to read table: [%s, %s]",
				key.Namespace, key.Table)
		} else {
			return rule.gradationFilter(), nil
		}
	} else {
		return nil, fmt.Errorf("can not find read rule for table: [%s, %s]",
			key.Namespace, key.Table)
	}
}

func (ac *AccessControl) GetWriteRule(key *KVDSKey) (*RWBaseRule, error) {
	ac.Mutex.Lock()
	writeRule := ac.tableWriteRule
	ac.Mutex.Unlock()

	if rule, ok := writeRule[ruleLookupKey(key)]; ok {
		if rule.CurCluster.Empty() && rule.PreCluster.Empty() {
			return nil, fmt.Errorf("no kvds clusters can be used to write table: [%s, %s]",
				key.Namespace, key.Table)
		} else {
			return &rule.RWBaseRule, nil
		}
	} else {
		return nil, fmt.Errorf("can not find write rule for table: [%s, %s]",
			key.Namespace, key.Table)
	}
}

func ruleLookupKey(key *KVDSKey) string {
	return key.Namespace + ":" + key.Table
}
