package util

import (
	//"github.com/golang/glog"
	"sync"
)

type ProxyJob struct {
	JobFunc func()
}

type JobDispatcher struct {
	workerQueue chan chan ProxyJob
	quit        chan struct{}
	wg          sync.WaitGroup
}

func NewJobDispatcher(max int, q chan struct{}) *JobDispatcher {
	jd := &JobDispatcher{
		workerQueue: make(chan chan ProxyJob, max),
		quit:        q,
	}
	for i := 0; i < 4; i++ {
		jd.wg.Add(1)
		jobQueue := make(chan ProxyJob, 1)
		jd.workerQueue <- jobQueue
		go func() {
			defer jd.wg.Done()
			jobWorker(jd.workerQueue, jobQueue, jd.quit)
		}()
	}
	return jd
}

func (self *JobDispatcher) DispatchJob(j ProxyJob) {
	select {
	case jobQueue := <-self.workerQueue:
		jobQueue <- j
	case <-self.quit:
		return
	default:
		//if glog.V(1) {
		//	glog.Info("===== No available worker. Starting new")
		//}
		self.wg.Add(1)
		jobQueue := make(chan ProxyJob, 1)
		jobQueue <- j
		go func() {
			defer self.wg.Done()
			jobWorker(self.workerQueue, jobQueue, self.quit)
		}()
	}
}

func (self *JobDispatcher) WaitFinish() {
	self.wg.Wait()
	close(self.workerQueue)
}

func jobWorker(
	workerQueue chan chan ProxyJob,
	jobQueue chan ProxyJob,
	closeChan chan struct{}) {

	//if glog.V(1) {
	//	defer func() {
	//		glog.Infof("A process goroutine exit: %v", GoroutineID())
	//	}()
	//	glog.Infof("A new process goroutine started: %v", GoroutineID())
	//}
	for {
		select {
		case <-closeChan:
			return
		case proxyJob := <-jobQueue:
			if proxyJob.JobFunc != nil {
				proxyJob.JobFunc()
			}
			select {
			case workerQueue <- jobQueue:
			default:
				//glog.Infof("==== worker number is large than worker queue just remove this worker!!")
				return
			}
		}
	}
}
