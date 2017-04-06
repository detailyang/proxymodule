package redisproxy

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/absolute8511/proxymodule/common"
	as "github.com/aerospike/aerospike-client-go"
	ds "gitlab.qima-inc.com/wangjian/go-dcc-sdk"
)

const (
	dccApp = "tether.kvproxy"
	dccKey = "whitelist.set"
)

func NewAerospikeWhiteList(cc *common.ControlCenter) (*aerospikeWhiteList, error) {
	if cc == nil {
		return nil, errors.New("the control center for proxy modules has not been initialized")
	}

	wl := &aerospikeWhiteList{
		whiteList: make(map[string]struct{}),
	}

	cc.Register(dccApp, dccKey, wl.Update)

	return wl, nil
}

type aerospikeWhiteList struct {
	sync.Mutex
	whiteList map[string]struct{}
}

func (self *aerospikeWhiteList) Update(e *common.CCEvent) {
	if e.Err != nil {
		redisLog.Errorf("white list update for aerospike failed, err: %s", e.Err.Error())
		return
	}

	dccRsp := e.Rsp
	if len(dccRsp.RspList) != 1 || dccRsp.RspList[0].Type != ds.ValueTypeCombination {
		redisLog.Errorf("response format from control center is illegal")
	} else {
		updatedwhiteList := make(map[string]struct{})
		for _, v := range dccRsp.RspList[0].CombVal {
			if v.Value == "1" {
				redisLog.Infof("add set into white list, %s", v.Key)
				updatedwhiteList[v.Key] = struct{}{}
			}
		}

		self.Mutex.Lock()
		self.whiteList = updatedwhiteList
		self.Mutex.Unlock()
	}
}

func (self *aerospikeWhiteList) AuthAccess(key *as.Key) bool {
	defer self.Mutex.Unlock()
	self.Mutex.Lock()

	if len(self.whiteList) == 0 {
		redisLog.Errorf("KVProxy access whitelist is empty, all access are authorized")
		return true
	}

	if _, ok := self.whiteList[key.Namespace()+":"+key.SetName()]; !ok {
		return false
	} else {
		return true
	}
}

func (self *aerospikeWhiteList) GenInfoBytes() []byte {
	var info bytes.Buffer
	info.WriteString("#White List\r\n")

	self.Mutex.Lock()
	defer self.Mutex.Unlock()

	info.WriteString(fmt.Sprintf("list length:%d\r\n", len(self.whiteList)))

	for set, _ := range self.whiteList {
		info.WriteString(set + "\r\n")
	}

	return info.Bytes()
}

func (self *aerospikeWhiteList) Stop() {
}
