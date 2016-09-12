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
	test.S(t).ExpectEquals(len(progressHistory.history), 0)
}

func TestMarkState(t *testing.T) {
	{
		progressHistory := NewProgressHistory()
		test.S(t).ExpectEquals(len(progressHistory.history), 0)
	}
	{
		progressHistory := NewProgressHistory()
		progressHistory.markState()
		progressHistory.markState()
		progressHistory.markState()
		progressHistory.markState()
		progressHistory.markState()
		test.S(t).ExpectEquals(len(progressHistory.history), 5)
	}
	{
		progressHistory := NewProgressHistory()
		progressHistory.markState()
		progressHistory.markState()
		progressHistory.history[0].mark = time.Now().Add(-2 * time.Hour)
		progressHistory.markState()
		progressHistory.markState()
		progressHistory.markState()
		test.S(t).ExpectEquals(len(progressHistory.history), 4)
	}
}

func TestOldestMark(t *testing.T) {
	{
		progressHistory := NewProgressHistory()
		oldestState := progressHistory.oldestState()
		test.S(t).ExpectTrue(oldestState == nil)
		oldestMark := progressHistory.oldestMark()
		test.S(t).ExpectTrue(oldestMark.IsZero())
	}
}
