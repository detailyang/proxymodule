package redisproxy

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	as "github.com/aerospike/aerospike-client-go"
	ds "gitlab.qima-inc.com/wangjian/go-dcc-sdk"
)

const (
	dccApp = "tether.kvproxy"
	dccKey = "whitelist.set"
)

func NewAerospikeWhiteList(serverAddrs []string, backupFile string) (*aerospikeWhiteList, error) {

	dccServerAddr := serverAddrs[0]
	for _, serverAddr := range serverAddrs[1:] {
		dccServerAddr += "," + serverAddr
	}

	asWhiteList := &aerospikeWhiteList{
		d3client:  ds.NewDccClient(dccServerAddr, backupFile, dccApp),
		whiteList: make(map[string]struct{}),
	}

	dccReq := []*ds.GetRequest{&ds.GetRequest{App: dccApp, Key: dccKey}}

	if dccRsp, err := asWhiteList.d3client.Get(dccReq, asWhiteList.Update); err != nil {
		asWhiteList.d3client.Close()
		return nil, err
	} else {
		if len(dccRsp.RspList) != 1 || dccRsp.RspList[0].Type != ds.ValueTypeCombination {
			return nil, errors.New("DCC server response format is illegal")
		}
		for _, v := range dccRsp.RspList[0].CombVal {
			if v.Value == "1" {
				redisLog.Infof("add set into white list, %s", v.Key)
				asWhiteList.whiteList[v.Key] = struct{}{}
			}
		}
	}

	return asWhiteList, nil
}

type aerospikeWhiteList struct {
	d3client *ds.DccClient

	sync.Mutex

	whiteList map[string]struct{}
}

func (self *aerospikeWhiteList) Update(dccRsp *ds.Response) {

	if len(dccRsp.RspList) != 1 || dccRsp.RspList[0].Type != ds.ValueTypeCombination {
		redisLog.Warningln("response format from DCC is illegal")
	} else {

		updatedwhiteList := make(map[string]struct{})

		for _, v := range dccRsp.RspList[0].CombVal {
			if v.Value == "1" {
				redisLog.Infof("add set into white list, %s", v.Key)
				updatedwhiteList[v.Key] = struct{}{}
			}
		}

		self.Mutex.Lock()
		defer self.Mutex.Unlock()

		self.whiteList = updatedwhiteList

	}

}

func (self *aerospikeWhiteList) AuthAccess(key *as.Key) bool {
	self.Mutex.Lock()
	defer self.Mutex.Unlock()

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
	self.d3client.Close()
}
