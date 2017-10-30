// Copyright 2014 Wandoujia Inc. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package atomic2

import "sync/atomic"

type Int64 struct {
	v int64
}

func (a *Int64) Get() int64 {
	return atomic.LoadInt64(&a.v)
}

func (a *Int64) Set(v int64) {
	atomic.StoreInt64(&a.v, v)
}

func (a *Int64) CompareAndSwap(o, n int64) bool {
	return atomic.CompareAndSwapInt64(&a.v, o, n)
}

func (a *Int64) Swap(v int64) int64 {
	return atomic.SwapInt64(&a.v, v)
}

func (a *Int64) Add(v int64) int64 {
	return atomic.AddInt64(&a.v, v)
}

func (a *Int64) Sub(v int64) int64 {
	return a.Add(-v)
}

func (a *Int64) Incr() int64 {
	return a.Add(1)
}

func (a *Int64) Decr() int64 {
	return a.Add(-1)
}
