/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"sync"
	"time"

	"github.com/github/gh-ost/go/base"
)

const maxHistoryDuration time.Duration = time.Hour

type ProgressState struct {
	mark       time.Time
	rowsCopied int64
}

func NewProgressState(rowsCopied int64) *ProgressState {
	result := &ProgressState{
		mark:       time.Now(),
		rowsCopied: rowsCopied,
	}
	return result
}

type ProgressHistory struct {
	migrationContext *base.MigrationContext
	history          [](*ProgressState)
	historyMutex     *sync.Mutex
}

func NewProgressHistory() *ProgressHistory {
	result := &ProgressHistory{
		history:          make([](*ProgressState), 0),
		historyMutex:     &sync.Mutex{},
		migrationContext: base.GetMigrationContext(),
	}
	return result
}

func (this *ProgressHistory) oldestState() *ProgressState {
	if len(this.history) == 0 {
		return nil
	}
	return this.history[0]
}

func (this *ProgressHistory) newestState() *ProgressState {
	if len(this.history) == 0 {
		return nil
	}
	return this.history[len(this.history)-1]
}

func (this *ProgressHistory) oldestMark() (mark time.Time) {
	if oldest := this.oldestState(); oldest != nil {
		return oldest.mark
	}
	return mark
}

func (this *ProgressHistory) markState() {
	this.historyMutex.Lock()
	defer this.historyMutex.Unlock()

	state := NewProgressState(this.migrationContext.GetTotalRowsCopied())
	this.history = append(this.history, state)
	for time.Since(this.oldestMark()) > maxHistoryDuration {
		if len(this.history) == 0 {
			return
		}
		this.history = this.history[1:]
	}
}

// hasEnoughData tells us whether there's at all enough information to predict an ETA
// this function is not concurrent-safe
func (this *ProgressHistory) hasEnoughData() bool {
	oldest := this.oldestState()
	if oldest == nil {
		return false
	}
	newest := this.newestState()

	if !oldest.mark.Before(newest.mark) {
		// single point in time; cannot extrapolate
		return false
	}
	if oldest.rowsCopied == newest.rowsCopied {
		// Nothing really happened; cannot extrapolate
		return false
	}
	return true
}

func (this *ProgressHistory) getETA() (eta time.Time) {
	if !this.hasEnoughData() {
		return eta
	}
	return eta
}
