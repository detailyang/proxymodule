package common

import (
	"fmt"
	"github.com/absolute8511/glog"
	"sync/atomic"
)

type Logger interface {
	Output(depth int, s string)
	OutputErr(depth int, s string)
	Flush()
}

type GLogger struct {
}

func (l *GLogger) Output(depth int, s string) {
	glog.InfoDepth(depth, s)
}

func (l *GLogger) OutputErr(depth int, s string) {
	glog.ErrorDepth(depth, s)
}

func (l *GLogger) Flush() {
	glog.Flush()
}

type ProxyLogger struct {
	Logger Logger
	level  int32
}

func NewProxyLogger(l int32, logger Logger) *ProxyLogger {
	return &ProxyLogger{Logger: logger, level: l}
}

func (self *ProxyLogger) Flush() {
	if self.Logger != nil {
		self.Logger.Flush()
	}
}

func (self *ProxyLogger) SetLevel(l int32) {
	atomic.StoreInt32(&self.level, l)
}

func (self *ProxyLogger) Level() int32 {
	return self.level
}

func (self *ProxyLogger) Infof(f string, args ...interface{}) {
	if self.Logger != nil && self.level > 0 {
		self.Logger.Output(2, fmt.Sprintf(f, args...))
	}
}

func (self *ProxyLogger) Debugf(f string, args ...interface{}) {
	if self.Logger != nil && self.level > 1 {
		self.Logger.Output(2, fmt.Sprintf(f, args...))
	}
}

func (self *ProxyLogger) Errorf(f string, args ...interface{}) {
	if self.Logger != nil {
		self.Logger.OutputErr(2, fmt.Sprintf(f, args...))
	}
}
