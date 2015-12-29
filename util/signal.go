package util

import (
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
)

func Trap(cleanup func(), restartFunc func()) {
	c := make(chan os.Signal, 1)
	signals := []os.Signal{os.Interrupt, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGUSR2, syscall.SIGQUIT}
	signal.Notify(c, signals...)
	go func() {
		cleaning := int32(0)
		for sig := range c {
			go func(sig os.Signal) {
				log.Printf("Received signal %v.\n", sig)
				switch sig {
				case os.Interrupt, syscall.SIGTERM:
					log.Printf("starting cleanup...\n")
					if atomic.CompareAndSwapInt32(&cleaning, 0, 1) {
						cleanup()
						//os.Exit(0)
					} else {
						return
					}
				case syscall.SIGUSR2, syscall.SIGHUP:
					log.Printf("restarting ...\n")
					if restartFunc != nil {
						restartFunc()
						return
					}
				case syscall.SIGQUIT:
					log.Fatalln("force quit without clean...\n")
					os.Exit(128 + int(sig.(syscall.Signal)))
				}
			}(sig)
		}
	}()
}
