/*
   Copyright 2025 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/sql"
)

// rangeResult is the outcome of a single parallel chunk INSERT, held until its
// iteration can be committed contiguously (see advanceFrontier).
type rangeResult struct {
	rangeMin     *sql.ColumnValues
	rangeMax     *sql.ColumnValues
	rowsAffected int64
}

// Blocks the calling goroutine until gh-ost's own binlog
// applier HeartbeatLag drops below ParallelCopyMaxHeartbeatLagThresholdMillies. It is a
// no-op when the threshold is zero (disabled), when no heartbeat has been seen yet, or
// when ctx is cancelled.
//
// HeartbeatLag (time since gh-ost last processed its own changelog heartbeat) is the
// right signal here: replica replication lag is already covered by the general throttle;
// this check specifically targets the case where the replica is fine but gh-ost's own
// binlog applier is falling behind due to pressure.
func (mgtr *Migrator) throttleOnHeartbeatLag(ctx context.Context) {
	threshold := atomic.LoadInt64(&mgtr.migrationContext.ParallelCopyMaxHeartbeatLagThresholdMillies)
	if threshold <= 0 {
		return
	}
	for {
		lastHeartbeat := mgtr.migrationContext.GetLastHeartbeatOnChangelogTime()
		if lastHeartbeat.IsZero() {
			return // no heartbeat received yet; don't block on stale zero value
		}
		if time.Since(lastHeartbeat) <= time.Duration(threshold)*time.Millisecond {
			return
		}
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// copyRowsParallel is the --parallel-copy consumer side. iterateChunks remains the single
// producer, enqueuing copy-task closures onto copyRowsQueue exactly as in serial mode; this
// function starts CopyWorkers consumer goroutines that pull those closures and run them
// concurrently. Each closure serializes its own boundary SELECT under parallelSelectMutex
// (so chunk ranges are still produced sequentially) and only the chunk INSERT runs in
// parallel; advanceFrontier then commits global state in contiguous iteration order so a
// crash/resume with checkpoints never leaves un-copied holes.
//
// Completion: the producer (iterateChunks) returns once the scan is exhausted; we then stop
// the workers, wait for every in-flight INSERT to finish, and signal clean completion to
// rowCopyComplete exactly once. A fatal error in any closure is signaled by the closure
// itself (it holds iterateChunks' terminateRowIteration), which aborts the context; in that
// case we do not signal a (false) clean completion.
func (mgtr *Migrator) copyRowsParallel() {
	mgtr.resetParallelState(mgtr.migrationContext.GetIteration())

	workers := int(mgtr.migrationContext.ParallelCopyWorkers)
	if workers < 1 {
		workers = 1
	}
	mgtr.migrationContext.Log.Infof("Parallel row copy: starting %d workers", workers)

	ctx := mgtr.migrationContext.GetContext()
	workersDone := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case copyRowsFunc := <-mgtr.copyRowsQueue:
					// Parallel-copy lag throttle: back off before the global throttle
					// kicks in, giving the binlog DML applier priority over row copy.
					mgtr.throttleOnHeartbeatLag(ctx)
					// Throttle before issuing the chunk, mirroring executeWriteFuncs.
					mgtr.throttler.throttle(nil)
					copyRowsStartTime := time.Now()
					// Errors are routed to rowCopyComplete inside the closure (via
					// terminateRowIteration); the ctx.Done() case below then unwinds us.
					if err := copyRowsFunc(); err != nil {
						return // closure already signaled via terminateRowIteration; skip nice-ratio sleep
					}
					if niceRatio := mgtr.migrationContext.GetNiceRatio(); niceRatio > 0 {
						copyRowsDuration := time.Since(copyRowsStartTime)
						sleepTimeNanosecondFloat64 := niceRatio * float64(copyRowsDuration.Nanoseconds())
						sleepTime := time.Duration(int64(sleepTimeNanosecondFloat64)) * time.Nanosecond
						time.Sleep(sleepTime)
					}
				case <-workersDone:
					return
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	// Run the producer to completion. It enqueues one closure per chunk onto copyRowsQueue
	// (an unbuffered channel, so every closure is handed to a worker before the next is
	// produced) and returns once the boundary scan is exhausted or the migration aborts.
	_ = mgtr.iterateChunks()

	// Producer done: tell idle workers to stop, then wait for any in-flight INSERTs.
	close(workersDone)
	wg.Wait()

	if err := mgtr.checkAbort(); err != nil {
		// The error was already signaled by the failing closure; don't signal completion.
		return
	}

	if atomic.LoadInt64(&mgtr.rowCopyCompleteFlag) == 0 {
		_ = base.SendWithContext(ctx, mgtr.rowCopyComplete, nil)
	}
}

// resetParallelState initializes the iteration-keyed pending table and the next-to-commit
// cursor for a parallel row-copy.
func (mgtr *Migrator) resetParallelState(firstIteration int64) {
	mgtr.parallelFrontierMutex.Lock()
	defer mgtr.parallelFrontierMutex.Unlock()
	mgtr.parallelPending = make(map[int64]*rangeResult)
	mgtr.parallelNextCommit = firstIteration
	atomic.StoreInt64(&mgtr.parallelDispatchSeq, firstIteration)
}

// advanceFrontier records a just-completed chunk INSERT (keyed by its iteration)
// and then commits every chunk that is now contiguously complete, in iteration
// order. "Committing" a chunk means folding its rows into TotalRowsCopied and
// advancing the checkpoint frontier (LastIterationRange{Min,Max}Values) to that
// chunk's range.
//
// Because parallel workers finish out of order, a chunk that completes before its
// predecessors is parked in parallelPending and only committed once the gap below
// it is filled. This guarantees the persisted frontier never advances past an
// un-written chunk, so a crash/resume can re-copy forward from the frontier without
// leaving holes.
func (mgtr *Migrator) advanceFrontier(iteration int64, rangeMin, rangeMax *sql.ColumnValues, rowsAffected int64) {
	mgtr.parallelFrontierMutex.Lock()
	defer mgtr.parallelFrontierMutex.Unlock()

	mgtr.parallelPending[iteration] = &rangeResult{
		rangeMin:     rangeMin,
		rangeMax:     rangeMax,
		rowsAffected: rowsAffected,
	}

	for {
		result, ok := mgtr.parallelPending[mgtr.parallelNextCommit]
		if !ok {
			break
		}
		delete(mgtr.parallelPending, mgtr.parallelNextCommit)
		mgtr.parallelNextCommit++
		atomic.AddInt64(&mgtr.migrationContext.Iteration, 1)
		atomic.AddInt64(&mgtr.migrationContext.TotalRowsCopied, result.rowsAffected)

		mgtr.applier.LastIterationRangeMutex.Lock()
		mgtr.applier.LastIterationRangeMinValues = result.rangeMin
		mgtr.applier.LastIterationRangeMaxValues = result.rangeMax
		mgtr.applier.LastIterationRangeMutex.Unlock()
	}
}
