/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"fmt"
	"testing"
	"time"

	"github.com/outbrain/golib/log"
	test "github.com/outbrain/golib/tests"
)

func init() {
	log.SetLevel(log.ERROR)
}

func TestReadChangelogState(t *testing.T) {
	waitForEventsUpToLockStartTime := time.Now()
	allEventsUpToLockProcessedChallenge := fmt.Sprintf("%s:%d", string(AllEventsUpToLockProcessed), waitForEventsUpToLockStartTime.UnixNano())

	state := ReadChangelogState(allEventsUpToLockProcessedChallenge)
	test.S(t).ExpectEquals(string(state), string(AllEventsUpToLockProcessed))
}
