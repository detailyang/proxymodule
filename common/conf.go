package common

import (
	"encoding/json"
	"io/ioutil"
	"path/filepath"
)

type ProxyConf struct {
	ModuleName     string
	ProxyType      string
	LocalProxyAddr string
	ModuleConfPath string
}

type ProxyModuleConf struct {
	PidFile        string
	GlogDir        string
	ModuleLogLevel int32
	ProxyConfList  []ProxyConf
}

const (
	ProxyModuleConfFile = "proxymodule.conf"
)

func LoadProxyModuleConfFromFile(confPath string) (ProxyModuleConf, error) {
	var c ProxyModuleConf
	confFile := filepath.Join(confPath, ProxyModuleConfFile)
	bytes, err := ioutil.ReadFile(confFile)
	if err != nil {
		return c, err
	}
	if len(bytes) == 0 {
		return c, nil
	}
	err = json.Unmarshal(bytes, &c)
	for _, e := range c.ProxyConfList {
		e.ModuleConfPath = filepath.Join(confPath, e.ModuleConfPath)
	}
	return c, err
}

func LoadProxyModuleConfFromServer(serverKey string) (ProxyModuleConf, error) {
	var c ProxyModuleConf
	var bytes []byte
	// TODO: load proxy configure from configure server.
	err := json.Unmarshal(bytes, &c)
	if err != nil {
		return c, err
	}
	for _, e := range c.ProxyConfList {
		e.ModuleConfPath = serverKey + "_module_conf/" + e.ModuleConfPath
	}
	return c, err
}

func LoadModuleConfFromFile(fileName string, v interface{}) error {
	bytes, err := ioutil.ReadFile(fileName)
	if err != nil {
		return err
	}
	if len(bytes) == 0 {
		return nil
	}
	err = json.Unmarshal(bytes, v)
	return err
}

func LoadModuleConfFromConfServer(serverKey string, v interface{}) error {
	var bytes []byte
	// TODO: load module configure from configure server.
	if len(bytes) == 0 {
		return nil
	}
	err := json.Unmarshal(bytes, v)
	return err
}
