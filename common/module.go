package common

type ModuleProxyServer interface {
	Start()
	Stop()
	GetStatsData() interface{}
}

type GraceModuleProxyServer interface {
	ModuleProxyServer
	StopGracefully() (chan struct{}, error)
}
