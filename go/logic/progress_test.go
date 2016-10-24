/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"testing"
	"time"

	"github.com/outbrain/golib/log"
	test "github.com/outbrain/golib/tests"
)

func init() {
	log.SetLevel(log.ERROR)
}

func TestNewProgressHistory(t *testing.T) {
	progressHistory := NewProgressHistory()
	test.S(t).ExpectTrue(progressHistory.lastProgressState == nil)
}

func TestMarkState(t *testing.T) {
	{
		progressHistory := NewProgressHistory()
		_, err := progressHistory.markState(0, 0)
		test.S(t).ExpectNotNil(err)
	}
	{
		progressHistory := NewProgressHistory()
		_, err := progressHistory.markState(0, 0.01)
		test.S(t).ExpectNotNil(err)
	}
	{
		progressHistory := NewProgressHistory()
		_, err := progressHistory.markState(0, 50)
		test.S(t).ExpectNotNil(err)
	}
	{
		progressHistory := NewProgressHistory()
		_, err := progressHistory.markState(time.Hour, 50)
		test.S(t).ExpectNil(err)
	}
}
