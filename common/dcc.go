package common

import (
	"sync"

	ds "gitlab.qima-inc.com/wangjian/go-dcc-sdk"
)

var (
	GlobalControlCenter *ControlCenter
)

type CCEvent struct {
	Rsp *ds.Response
	Err error
}

type EventHandler func(*CCEvent)

type ControlCenter struct {
	d3client *ds.DccClient
	wg       sync.WaitGroup
	chans    []chan *CCEvent
}

func NewControlCenter(serverAddrs string, backupFile string, ccTag string, ccEnv string) *ControlCenter {
	cc := &ControlCenter{
		d3client: ds.NewDccClient(serverAddrs, backupFile, ccTag),
	}

	ds.SetEnv(ccEnv)

	return cc
}

func (cc *ControlCenter) Register(app string, key string, handler EventHandler) error {
	notifyC := make(chan *CCEvent)
	cc.chans = append(cc.chans, notifyC)

	cc.wg.Add(1)
	go func() {
		defer cc.wg.Done()
		for e := range notifyC {
			handler(e)
		}
	}()

	dccReq := []*ds.GetRequest{&ds.GetRequest{App: app, Key: key}}

	dispatcher := func(rsp *ds.Response) {
		notifyC <- &CCEvent{Rsp: rsp, Err: nil}
	}

	if dccRsp, err := cc.d3client.Get(dccReq, dispatcher); err != nil {
		notifyC <- &CCEvent{Err: err}
	} else {
		notifyC <- &CCEvent{Rsp: dccRsp, Err: nil}
	}

	return nil
}

func (cc *ControlCenter) Close() {
	cc.d3client.Close()
	for _, c := range cc.chans {
		close(c)
	}
	cc.wg.Wait()
}
