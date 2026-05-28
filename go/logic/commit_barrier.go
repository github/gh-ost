/*
   Copyright 2025 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"context"
	"sync"
	"sync/atomic"
)

const seqUninit int64 = 0

// commitBarrier implements MTS LOGICAL_CLOCK dependency tracking using a
// gap-free Low Water Mark (LWM), mirroring MySQL 8.0's GAQ scheduling.
//
// LWM invariant: all transactions with sequence_number <= lwm have completed.
// This is equivalent to MySQL's find_lwm() + move_queue_head() in rpl_rli_pdb.cc.
//
// Reference: MySQL 8.0 sql/rpl_mta_submode.cc
//   - waitForDependency  corresponds to wait_for_last_committed_trx()
//   - commit             corresponds to Worker commit + GAQ LWM advancement
//   - waitForAllWorkers  corresponds to wait_for_workers_to_finish()
type commitBarrier struct {
	mu           sync.Mutex
	lwm          int64           // gap-free low water mark: all seq <= lwm are complete
	pending      map[int64]bool  // sequences > lwm that committed but aren't consecutive yet
	delegatedJobs atomic.Int64   // jobs dispatched but not yet completed
	cond         *sync.Cond
}

func newCommitBarrier() *commitBarrier {
	cb := &commitBarrier{
		lwm:     seqUninit,
		pending: make(map[int64]bool),
	}
	cb.cond = sync.NewCond(&cb.mu)
	return cb
}

// clockLeq implements MySQL's clock_leq: SEQ_UNINIT (0) is treated as the
// minimum value in the clock domain.
func clockLeq(a, b int64) bool {
	if a == seqUninit {
		return true
	}
	if b == seqUninit {
		return false
	}
	return a <= b
}

// waitForDependency blocks until lwm >= lastCommitted.
//
// This matches MySQL's wait_for_last_committed_trx() which waits until the
// low water mark advances past the parent transaction:
//
//	while (!clock_leq(last_committed_arg, estimate_lwm_timestamp()))
//	    wait(logical_clock_cond)
//
// Cross-table dependencies (parentSeenOnStream == false) are treated as
// satisfied, matching MySQL's SEQ_UNINIT handling where undefined parents
// don't block scheduling.
func (cb *commitBarrier) waitForDependency(ctx context.Context, lastCommitted int64, parentSeenOnStream bool) {
	if lastCommitted == seqUninit || !parentSeenOnStream {
		return
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()
	for !clockLeq(lastCommitted, cb.lwm) {
		if ctx.Err() != nil {
			return
		}
		cb.cond.Wait()
	}
}

// commit records a transaction as complete and advances the LWM
// as far as possible through consecutive completed sequences.
//
// This matches MySQL's move_queue_head() which dequeues jobs from
// the GAQ head only while they are consecutively done:
//
//	while (!empty()) {
//	    if (ptr_g->done == 0) break;   // gap — stop advancing
//	    de_queue(&g);                   // remove from queue
//	    lwm = g;                        // advance LWM
//	}
func (cb *commitBarrier) commit(sequenceNumber int64) {
	if sequenceNumber == seqUninit {
		return
	}
	cb.mu.Lock()
	cb.pending[sequenceNumber] = true
	// Advance LWM through consecutive committed sequences
	for cb.pending[cb.lwm+1] {
		delete(cb.pending, cb.lwm+1)
		cb.lwm++
	}
	cb.mu.Unlock()
	cb.cond.Broadcast()
}

// waitForAllWorkers blocks until all delegated jobs have completed (delegatedJobs == 0).
//
// Corresponds to MySQL: wait_for_workers_to_finish()
func (cb *commitBarrier) waitForAllWorkers(ctx context.Context) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	for cb.delegatedJobs.Load() > 0 {
		if ctx.Err() != nil {
			return
		}
		cb.cond.Wait()
	}
}

// addDelegatedJob increments the delegated job counter.
func (cb *commitBarrier) addDelegatedJob() {
	cb.delegatedJobs.Add(1)
}

// completeDelegatedJob decrements the delegated job counter and broadcasts wake signal.
func (cb *commitBarrier) completeDelegatedJob() {
	cb.delegatedJobs.Add(-1)
	cb.cond.Broadcast()
}

// getLWM returns the current gap-free low water mark (thread-safe).
func (cb *commitBarrier) getLWM() int64 {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.lwm
}
