package kvds

import "github.com/absolute8511/proxymodule/common"

type Clusters map[string]*StoreConf

type Conf struct {
	Namespace map[string]Clusters
}

type StoreConf struct {
	Protocol string
	ConfPath string
}

func LoadConfFromDCC(cc *common.ControlCenter) error {
	return nil
}
