/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"fmt"
	"sync"
	"time"
)

const minProgressRatio float64 = 0.001
const alpha float64 = 0.25

type ProgressState struct {
	elapsedTime           time.Duration
	progressRatio         float64
	expectedTotalDuration time.Duration
}

func NewProgressState(elapsedTime time.Duration, progressRatio float64, expectedTotalDuration time.Duration) *ProgressState {
	result := &ProgressState{
		elapsedTime:           elapsedTime,
		progressRatio:         progressRatio,
		expectedTotalDuration: expectedTotalDuration,
	}
	return result
}

type ProgressHistory struct {
	lastProgressState *ProgressState
	historyMutex      *sync.Mutex
	eta               time.Time
}

func NewProgressHistory() *ProgressHistory {
	result := &ProgressHistory{
		lastProgressState: nil,
		historyMutex:      &sync.Mutex{},
	}
	return result
}

func (this *ProgressHistory) markState(elapsedTime time.Duration, progressRatio float64) (expectedTotalDuration time.Duration, err error) {
	if progressRatio < minProgressRatio || elapsedTime == 0 {
		return expectedTotalDuration, fmt.Errorf("eta n/a")
	}

	this.historyMutex.Lock()
	defer this.historyMutex.Unlock()

	newExpectedTotalDuration := float64(elapsedTime.Nanoseconds()) / progressRatio
	if this.lastProgressState != nil {
		newExpectedTotalDuration = alpha*float64(this.lastProgressState.expectedTotalDuration.Nanoseconds()) + (1.0-alpha)*newExpectedTotalDuration
	}
	expectedTotalDuration = time.Duration(int64(newExpectedTotalDuration))
	this.lastProgressState = NewProgressState(elapsedTime, progressRatio, expectedTotalDuration)
	this.eta = time.Now().Add(expectedTotalDuration - elapsedTime)
	return expectedTotalDuration, nil
}

func (this *ProgressHistory) GetETA() time.Time {
	this.historyMutex.Lock()
	defer this.historyMutex.Unlock()

	return this.eta
}
