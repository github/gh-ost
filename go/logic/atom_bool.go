package logic

import "sync/atomic"

type AtomicBool struct {
	flag int64
}

func (a *AtomicBool) Set(value bool) {
	var i int64 = 0
	if value {
		i = 1
	}
	atomic.StoreInt64(&(a.flag), int64(i))
}

func (a *AtomicBool) Get() bool {
	return atomic.LoadInt64(&(a.flag)) != 0
}
