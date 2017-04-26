package common

type ModuleProxyServer interface {
	Start()
	Stop()
	ProxyStatisticsData() []byte
}

type GraceModuleProxyServer interface {
	ModuleProxyServer
	StopGracefully() (chan struct{}, error)
}
