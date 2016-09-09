package common

type ModuleProxyServer interface {
	Start()
	Stop()
	ProxyStatisticsData() []byte
}
