package util

import (
	"time"
)

type TimerScheduler struct {
	stopChan chan struct{}
}

func NewTimerScheduler() *TimerScheduler {
	return &TimerScheduler{
		stopChan: make(chan struct{}),
	}
}

func (self *TimerScheduler) ScheduleJob(job func(), d time.Duration, repeat bool) chan struct{} {
	cancelChan := make(chan struct{})
	go func() {
		ticker := time.NewTicker(d)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				job()
				if !repeat {
					return
				}
			case <-self.stopChan:
				return
			case <-cancelChan:
				return
			}
		}
	}()
	return cancelChan
}

func (self *TimerScheduler) StopScheduler() {
	close(self.stopChan)
}
