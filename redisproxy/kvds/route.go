package kvds

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sync"

	ds "gitlab.qima-inc.com/wangjian/go-dcc-sdk"

	"github.com/absolute8511/proxymodule/common"
)

const (
	dccApp       = "KVDS"
	dccRouteKey  = "route"
	HostWildCard = "*"
)

var (
	keySep = []byte(":")
)

type KeyConvert struct {
	Prefix string `json:"prefix"`
}

//Convert the format of args according to the cmd
//1.should convert all keys used by the commands which may operate multiple keys simultaneously(mget eg.)
//2.may use a pool of byte slice to reduce the cost of GC
func (conv *KeyConvert) Convert(cmd string, args [][]byte) [][]byte {
	if len(conv.Prefix) == 0 {
		return args
	}

	newArgs := make([][]byte, len(args))
	copy(newArgs, args)

	if _, ok := unaryCommands[cmd]; ok {
		newArgs[0] = conv.convSingleK(args[0])
	} else if _, ok := multiCommands[cmd]; ok {
		for i, arg := range args {
			newArgs[i] = conv.convSingleK(arg)
		}
	} else if _, ok := kvPairCommands[cmd]; ok {
		if len(args)%2 == 0 {
			for i := 0; i < len(args)/2; i++ {
				newArgs[i*2] = conv.convSingleK(args[i*2])
			}
		}
	}
	return newArgs
}

func (conv *KeyConvert) convSingleK(key []byte) []byte {
	fields := bytes.SplitN(key, keySep, 3)
	fields[1] = []byte(conv.Prefix)

	return bytes.Join(fields[1:], keySep)
}

type Cluster struct {
	Name       string `json:"name"`
	KeyConvert `json:"key-convert"`
}

func (clster *Cluster) Empty() bool {
	return clster.Name == ""
}

func (clster *Cluster) String() string {
	return fmt.Sprintf("%s, Convert:[ Prefix:%s ]", clster.Name, clster.KeyConvert.Prefix)
}

type Route struct {
	PreCluster Cluster `json:"pre-cluster"`
	CurCluster Cluster `json:"cur-cluster"`

	//the key of the map is designed to be the host name
	Gradation map[string]GradationStrategy `json:"gradation"`
}

func (route *Route) String() string {
	buf := new(bytes.Buffer)
	if !route.PreCluster.Empty() && !route.CurCluster.Empty() {
		buf.WriteString(fmt.Sprintf("current cluster:%s\n", route.CurCluster.String()))
		buf.WriteString(fmt.Sprintf("previous cluster:%s\n", route.PreCluster.String()))
	} else {
		buf.WriteString(fmt.Sprintf("cluster:%s\n", route.CurCluster.String()))
	}

	if len(route.Gradation) > 0 {
		buf.WriteString("Gradation:\n")
		if v, ok := route.Gradation[HostWildCard]; ok {
			buf.WriteString(fmt.Sprintf("default, percent:%f\n", v))
		}
		for host, percent := range route.Gradation {
			if host != HostWildCard {
				buf.WriteString(fmt.Sprintf("host:%s, percent:%f\n", host, percent))
			}
		}
	}

	return buf.String()
}

//针对单个host的灰度策略
type GradationStrategy struct {
	Percent float64 `json:"percent"`
}

type AccessControl struct {
	sync.Mutex
	tblRoute map[string]*Route
}

func NewAccessControl(cc *common.ControlCenter) (*AccessControl, error) {
	if cc == nil {
		return nil, errors.New("the control center for proxy modules has not been initialized")
	}

	ac := &AccessControl{
		tblRoute: make(map[string]*Route),
	}

	cc.Register(dccApp, dccRouteKey, ac.updateTblRoute)

	return ac, nil
}

func (ac *AccessControl) updateTblRoute(e *common.CCEvent) {
	if e.Err != nil || len(e.Rsp.RspList) != 1 ||
		e.Rsp.RspList[0].Type != ds.ValueTypeCombination {
		return
	}

	rsp := e.Rsp.RspList[0]
	tblRoute := make(map[string]*Route)

	for _, v := range rsp.CombVal {
		route := &Route{}
		if err := json.Unmarshal([]byte(v.Value), route); err != nil {
			if log != nil {
				log.Errorf("update table route meta data failed, err:%s", err.Error())
			}
		} else {
			tblRoute[v.Key] = route
			if log != nil {
				log.Infof("route meta data\ntable:%s\n%s", v.Key, route.String())
			}
		}
	}

	ac.Mutex.Lock()
	ac.tblRoute = tblRoute
	ac.Mutex.Unlock()
}

func (ac *AccessControl) GetTableRoute(namespace string, table string) (*Route, error) {
	ac.Mutex.Lock()
	tblRoute := ac.tblRoute
	ac.Mutex.Unlock()

	lookUpKey := routeLookupKey(namespace, table)
	route, ok := tblRoute[lookUpKey]
	if !ok {
		for k, v := range tblRoute {
			if match, err := regexp.MatchString(k, lookUpKey); match && err == nil {
				route = v
				break
			}
		}
	}

	if route == nil {
		return nil, fmt.Errorf("can not find route for table: [%s, %s]", namespace, table)
	} else if route.CurCluster.Empty() && route.PreCluster.Empty() {
		return nil, fmt.Errorf("no cluster can be used to read-write table: [%s, %s]", namespace, table)
	} else {
		return route, nil
	}

}

func routeLookupKey(namespace string, table string) string {
	return namespace + ":" + table
}
