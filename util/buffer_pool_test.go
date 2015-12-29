package util

import (
	"flag"
	"reflect"
	"sync"
	"testing"
	"time"
	"unsafe"
)

func TestFixedBufferPool(t *testing.T) {
	flag.Set("alsologtostderr", "true")
	bufsize := 10
	pool := NewFixedBufferPool(bufsize, 1, true)
	b := pool.GetBuffer()

	if len(b.buf) != bufsize {
		t.Errorf("buf size error")
	}
	if b.GetUsed() != 0 {
		t.Errorf("buf should be unused")
	}
	var b2 *FixedBuffer
	var getAgain time.Time
	go func() {
		b2 = pool.GetBuffer()
		getAgain = time.Now()
	}()
	start := time.Now()
	var end time.Time
	go func() {
		time.Sleep(100 * time.Millisecond)
		pool.PutBuffer(b)
		end = time.Now()
	}()
	time.Sleep(2 * time.Second)

	if b != b2 {
		t.Errorf("the same buffer in pool should be the same")
	}
	pool.PutBuffer(b2)
	start2 := time.Now()
	var end2 time.Time
	go func() {
		b = pool.GetBuffer()
		end2 = time.Now()
	}()
	diff := end.Sub(start)

	time.Sleep(2 * time.Second)
	pool.Close()
	if diff > time.Second {
		t.Errorf("Put Buffer should be immediatlly")
	}
	if getAgain.Before(end) {
		t.Errorf("Get Buffer again should be after the PutBuffer.")
	}
	diff = end2.Sub(start2)
	if diff > time.Second {
		t.Errorf("Get Buffer should be immediatlly")
	}

	pool = NewFixedBufferPool(bufsize, bufsize, true)
	tmp := make([]*FixedBuffer, bufsize+1)
	pool.PutBuffer(NewFixedBuffer(bufsize))
	go func() {
		time.Sleep(time.Second * 2)
		for _, d := range tmp {
			pool.PutBuffer(d)
		}
	}()
	for i := 0; i < bufsize+1; i++ {
		tmp[i] = pool.GetBuffer()
	}

	pool.Close()
}

func TestByteBufferPool(t *testing.T) {
	p := NewByteBufferPool(10, 32)
	b := p.Get()
	if len(b) != 32 {
		t.Errorf("byte pool length wrong.")
	}
	p.Put(b)
	for i := 0; i < 10*20; i++ {
		p.Put(make([]byte, 32))
	}
	for i := 0; i < 10*20; i++ {
		b = p.Get()
		if b == nil {
			t.Error("nil bufffer from pool")
		}
	}
}

func TestUnboundBufferPool(t *testing.T) {
	p := NewUnboundBufferPool(32)

	b1 := p.Get()
	b1 = append(b1, 'a')
	p.Put(b1)
	b2 := p.Get()
	p.Put(b2)
	b1 = p.Get()
	b2 = p.Get()
	p1 := (*reflect.SliceHeader)(unsafe.Pointer(&b1)).Data
	p2 := (*reflect.SliceHeader)(unsafe.Pointer(&b2)).Data
	if b1 == nil || b2 == nil || p1 == p2 {
		t.Errorf("should not be equal ")
	}
}

func applyUnboundBufferPoolBench(b *testing.B, n int) {
	p := NewUnboundBufferPool(n)
	var wg sync.WaitGroup
	wg.Add(10)
	b.ResetTimer()
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			buf := make([][]byte, 20)
			for j := 0; j < b.N; j++ {
				for k := 0; k < 100; k++ {
					tmp := buf[k%len(buf)]
					if tmp != nil {
						p.Put(tmp)
					}

					buf[k%len(buf)] = p.Get()
				}
			}
		}()
	}
	wg.Wait()
}

func BenchmarkUnboundBufferPoolWith32(b *testing.B) {
	applyUnboundBufferPoolBench(b, 32)
}

func BenchmarkUnboundBufferPoolWith320(b *testing.B) {
	applyUnboundBufferPoolBench(b, 320)
}

func BenchmarkUnboundBufferPoolWith3200(b *testing.B) {
	applyUnboundBufferPoolBench(b, 3200)
}
func applyByteBufferPoolBench(b *testing.B, n int) {
	p := NewByteBufferPool(100, n)

	var wg sync.WaitGroup
	wg.Add(10)
	b.ResetTimer()
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			buf := make([][]byte, 20)
			for j := 0; j < b.N; j++ {
				for k := 0; k < 100; k++ {
					tmp := buf[k%len(buf)]
					if tmp != nil {
						p.Put(tmp)
					}
					buf[k%len(buf)] = p.Get()
				}
			}
		}()
	}
	wg.Wait()

}

func BenchmarkBufferGetWithByteBufferPoolWith32(b *testing.B) {
	applyByteBufferPoolBench(b, 32)
}
func BenchmarkBufferGetWithByteBufferPoolWith320(b *testing.B) {
	applyByteBufferPoolBench(b, 320)
}
func BenchmarkBufferGetWithByteBufferPoolWith3200(b *testing.B) {
	applyByteBufferPoolBench(b, 3200)
}

func applyMakeBufBench(b *testing.B, n int) {
	var wg sync.WaitGroup
	wg.Add(10)
	b.ResetTimer()
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			buf := make([][]byte, 20)
			for j := 0; j < b.N; j++ {
				for k := 0; k < 100; k++ {
					buf[k%len(buf)] = make([]byte, 0, n)
				}
			}
		}()
	}
	wg.Wait()
}

func BenchmarkBufferGetWithMake32(b *testing.B) {
	applyMakeBufBench(b, 32)
}
func BenchmarkBufferGetWithMake320(b *testing.B) {
	applyMakeBufBench(b, 320)
}
func BenchmarkBufferGetWithMake3200(b *testing.B) {
	applyMakeBufBench(b, 3200)
}
