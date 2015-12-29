package util

import (
	"bytes"
	"container/list"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DEFAULT_BUF_BYTES = 32
)

// buffer pool for only consume by the producer order.
// producer get buffer from empty list and fill data then push to data list.
// consumer get the data from data list and push back to empty list
type FixedBuffer struct {
	buf  []byte
	used int32
}

func NewFixedBuffer(size int) *FixedBuffer {
	return &FixedBuffer{
		buf:  make([]byte, size),
		used: 0,
	}
}

func (self *FixedBuffer) UseBuffer(n int32) {
	atomic.StoreInt32(&self.used, n)
}

func (self *FixedBuffer) GetUsed() int32 {
	return atomic.LoadInt32(&self.used)
}

func (self *FixedBuffer) GetUsedBuffer() []byte {
	return self.buf[0:self.GetUsed()]
}

func (self *FixedBuffer) GetRawBuffer() []byte {
	return self.buf
}

func GoroutineID() string {
	buf := make([]byte, 15)
	buf = buf[:runtime.Stack(buf, false)]
	return string(bytes.Split(buf, []byte(" "))[1])
}

type FixedBufferPool struct {
	sync.Mutex
	emptyList chan *FixedBuffer
	// the buffer size for each buffer in the pool
	bufSize   int
	bufCount  int
	capacity  int
	trackList map[*FixedBuffer]string
}

func NewFixedBufferPool(bs, ca int, trackEnable bool) *FixedBufferPool {
	b := &FixedBufferPool{
		bufSize:   bs,
		emptyList: make(chan *FixedBuffer, ca),
	}
	if trackEnable {
		b.trackList = make(map[*FixedBuffer]string)
	}
	return b
}

func (self *FixedBufferPool) GetBuffer() *FixedBuffer {
	var b *FixedBuffer
	select {
	case b = <-self.emptyList:
	default:
		self.Lock()
		if self.bufCount >= cap(self.emptyList) {
			// wait for free buffer
			//for k, gid := range self.trackList {
			//glog.Infof("used buffer by goroutine %v : %v", gid, k.GetUsedBuffer())
			//}
			self.Unlock()
			b = <-self.emptyList
			// got free buffer
		} else {
			b = NewFixedBuffer(self.bufSize)
			self.bufCount++
			self.Unlock()
		}
	}
	if self.trackList != nil {
		self.Lock()
		self.trackList[b] = GoroutineID()
		self.Unlock()
	}
	return b
}

func (self *FixedBufferPool) PutBuffer(b *FixedBuffer) {
	if b == nil {
		return
	}
	b.UseBuffer(0)
	if self.trackList != nil {
		self.Lock()
		if _, ok := self.trackList[b]; !ok {
			//glog.Infof("put buffer: %v used by %v.", b.GetUsedBuffer(), gid)
			self.Unlock()
			return
		}
		delete(self.trackList, b)
		self.Unlock()
	}
	select {
	case self.emptyList <- b:
	default:
		//glog.Warningln("put buffer blocked.")
		self.emptyList <- b
		//glog.Warningln("put buffer unblocked")
	}
}

func (self *FixedBufferPool) Close() {
	close(self.emptyList)
}

const (
	BUF_CHAN_NUM = 8
)

type ByteBufferPool struct {
	chanList []chan []byte
	bsize    int
	next     uint32
}

func NewByteBufferPool(maxSize int, bufSize int) *ByteBufferPool {
	b := &ByteBufferPool{
		chanList: make([]chan []byte, BUF_CHAN_NUM),
		bsize:    bufSize,
		next:     0,
	}
	for i := 0; i < len(b.chanList); i++ {
		b.chanList[i] = make(chan []byte, maxSize)
	}
	return b
}

func (self *ByteBufferPool) Get() []byte {
	self.next++
	select {
	case b := <-self.chanList[self.next%BUF_CHAN_NUM]:
		return b
	default:
		return make([]byte, self.bsize)
	}
}

func (self *ByteBufferPool) Put(b []byte) {
	select {
	case self.chanList[self.next%BUF_CHAN_NUM] <- b:
	default:
		// buffer did not go back into pool
	}
}

type queuedItem struct {
	timestamp time.Time
	data      []byte
}

type UnboundBufferPool struct {
	getList  []chan []byte
	giveList []chan []byte
	next     uint32
	bufSize  int
}

func NewUnboundBufferPool(bs int) *UnboundBufferPool {
	p := &UnboundBufferPool{
		getList:  make([]chan []byte, BUF_CHAN_NUM),
		giveList: make([]chan []byte, BUF_CHAN_NUM),
		next:     0,
		bufSize:  bs,
	}
	for i := 0; i < len(p.getList); i++ {
		p.getList[i] = make(chan []byte)
		p.giveList[i] = make(chan []byte)

		go func(get, give chan []byte) {
			bufList := list.New()
			timeout := time.NewTimer(time.Minute)
			for {
				if bufList.Len() == 0 {
					bufList.PushFront(queuedItem{timestamp: time.Now(), data: make([]byte, 0, bs)})
				}
				e := bufList.Front()
				timeout.Reset(time.Minute)
				select {
				case b := <-give:
					bufList.PushFront(queuedItem{timestamp: time.Now(), data: b})
				case get <- e.Value.(queuedItem).data:
					p.next += 1
					bufList.Remove(e)
				case <-timeout.C:
					e := bufList.Front()
					if e != nil {
						n := e.Next()
						if time.Since(e.Value.(queuedItem).timestamp) > time.Minute {
							bufList.Remove(e)
							e.Value = nil
						}
						e = n
					}
				}
			}
		}(p.getList[i], p.giveList[i])
	}
	return p
}

func (self *UnboundBufferPool) Get() []byte {
	i := self.next
	b := <-self.getList[int(i)%BUF_CHAN_NUM]
	if len(b) > 0 {
		return b[0:0]
	}
	return b
}

//func (self *UnboundBufferPool) GetLarge(capacity int) []byte {
//	b := self.Get()
//	if cap(b) >= capacity {
//		return b
//	}
//	return b
//}

func (self *UnboundBufferPool) Put(b []byte) {
	if b == nil {
		return
	}
	i := self.next
	self.giveList[int(i)%BUF_CHAN_NUM] <- b
}
