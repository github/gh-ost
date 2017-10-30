// Copyright 2014 Wandoujia Inc. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package errors

import (
	"container/list"
	"errors"
	"sync"
)

var ErrAnonError = errors.New("anonymous error")

type ErrorList struct {
	mu sync.Mutex
	el list.List
}

func (q *ErrorList) First() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if e := q.el.Front(); e != nil {
		return e.Value.(error)
	}
	return nil
}

func (q *ErrorList) Errors() []error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if n := q.el.Len(); n != 0 {
		array := make([]error, 0, n)
		for e := q.el.Front(); e != nil; e = e.Next() {
			array = append(array, e.Value.(error))
		}
		return array
	}
	return nil
}

func (q *ErrorList) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.el.Len()
}

func (q *ErrorList) PushBack(err error) {
	if err == nil {
		err = Trace(ErrAnonError)
	}
	q.mu.Lock()
	q.el.PushBack(err)
	q.mu.Unlock()
}

func (q *ErrorList) Reset() {
	q.mu.Lock()
	q.el.Init()
	q.mu.Unlock()
}
