/*
   Copyright 2025 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"context"
	"sort"
	"sync"
)

const seqUninit int64 = 0

// commitBarrier implements MTS LOGICAL_CLOCK dependency tracking for gh-ost.
//
// It mirrors MySQL 8.0's GAQ low-water-mark (LWM) scheduling, with one crucial
// adaptation: gh-ost only streams the binlog rows of the single migrated table,
// so the GTID sequence_number values it observes are SPARSE. The server assigns
// sequence_number across all transactions on all tables, so consecutive
// transactions on our table might be numbered 5, 9, 14, ... with large gaps for
// other tables' transactions that we never see.
//
// A naive gap-free LWM over the global sequence space (advance only lwm+1) would
// stall permanently at the first gap, because the missing sequence numbers
// belong to other tables and are never committed in our barrier. Instead, the
// LWM advances over the *dispatched* (seen) subsequence: the coordinator records
// every transaction it dispatches via addDelegatedJob(seq) in binlog order, and
// the LWM advances to the highest dispatched sequence whose predecessors in that
// subsequence have all committed.
//
// Invariant: all dispatched (seen) transactions with sequence_number <= lwm have
// been applied. This is exactly what wait_for_last_committed_trx needs, restricted
// to the transactions gh-ost actually applies.
//
// Concurrency: every field is guarded by mu. delegatedJobs MUST be mutated under
// mu so its change and the corresponding cond.Broadcast() are serialized with
// waiters in waitForAllWorkers; otherwise a decrement-to-zero racing with a
// waiter's predicate check would lose the wakeup and deadlock the coordinator.
type commitBarrier struct {
	mu            sync.Mutex
	dispatched    []int64        // seen sequence numbers in dispatch (increasing) order
	head          int            // index into dispatched: entries before head are folded into lwm
	committed     map[int64]bool // committed sequences not yet folded into lwm
	lwm           int64          // all dispatched seq <= lwm are committed
	delegatedJobs int64          // jobs dispatched but not yet completed
	cond          *sync.Cond
}

func newCommitBarrier() *commitBarrier {
	cb := &commitBarrier{
		lwm:       seqUninit,
		committed: make(map[int64]bool),
	}
	cb.cond = sync.NewCond(&cb.mu)
	return cb
}

// addDelegatedJob records a dispatched transaction. seq must be the transaction's
// sequence_number, supplied in binlog (increasing) order. A zero seq (logical
// timestamps unavailable) is accepted for job accounting but not tracked for
// dependency ordering.
func (cb *commitBarrier) addDelegatedJob(seq int64) {
	cb.mu.Lock()
	if seq != seqUninit {
		cb.dispatched = append(cb.dispatched, seq)
	}
	cb.delegatedJobs++
	cb.mu.Unlock()
}

// commit records a transaction as applied and advances the LWM over the
// dispatched subsequence as far as consecutively-committed entries allow.
func (cb *commitBarrier) commit(sequenceNumber int64) {
	if sequenceNumber == seqUninit {
		return
	}
	cb.mu.Lock()
	cb.committed[sequenceNumber] = true
	for cb.head < len(cb.dispatched) && cb.committed[cb.dispatched[cb.head]] {
		seq := cb.dispatched[cb.head]
		delete(cb.committed, seq)
		cb.lwm = seq
		cb.head++
	}
	cb.compactLocked()
	cb.cond.Broadcast()
	cb.mu.Unlock()
}

// compactLocked reclaims the folded prefix of the dispatched slice so it does
// not grow unbounded over a long migration. Caller must hold mu.
func (cb *commitBarrier) compactLocked() {
	const compactThreshold = 4096
	if cb.head < compactThreshold || cb.head < len(cb.dispatched)/2 {
		return
	}
	remaining := len(cb.dispatched) - cb.head
	copy(cb.dispatched, cb.dispatched[cb.head:])
	cb.dispatched = cb.dispatched[:remaining]
	cb.head = 0
}

// isPendingDispatchedLocked reports whether seq was dispatched on this table's
// stream and has not yet been folded into the LWM. dispatched[head:] is sorted
// ascending, so we binary-search it. Caller must hold mu.
func (cb *commitBarrier) isPendingDispatchedLocked(seq int64) bool {
	tail := cb.dispatched[cb.head:]
	i := sort.Search(len(tail), func(i int) bool { return tail[i] >= seq })
	return i < len(tail) && tail[i] == seq
}

// waitForDependency blocks until every seen transaction with sequence_number <=
// lastCommitted has been applied (lwm >= lastCommitted).
//
// "Seen" is determined from the dispatched set itself, atomically under the lock:
//   - lastCommitted <= lwm: the parent (and all earlier seen transactions) have
//     already completed. Return immediately.
//   - lastCommitted is a dispatched-but-not-yet-folded sequence: wait until the
//     LWM advances past it.
//   - lastCommitted was never dispatched on this table's stream: it is a
//     cross-table dependency (the parent did not touch the migrated table), so
//     gh-ost never applies it and must not wait — return immediately.
//
// Because the coordinator processes transactions in binlog order and a child's
// lastCommitted is always strictly less than its own sequence_number, any parent
// that lives on this stream has already been dispatched by the time we get here;
// "not dispatched" therefore reliably means a cross-table parent. This avoids the
// observe/dispatch ordering hazard of tracking "seen" separately from the barrier.
func (cb *commitBarrier) waitForDependency(ctx context.Context, lastCommitted int64) {
	if lastCommitted == seqUninit {
		return
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if lastCommitted <= cb.lwm {
		return
	}
	if !cb.isPendingDispatchedLocked(lastCommitted) {
		return
	}
	cb.waitWhileLocked(ctx, func() bool { return cb.lwm < lastCommitted })
}

// waitForAllWorkers blocks until all delegated jobs have completed
// (delegatedJobs == 0). Corresponds to MySQL: wait_for_workers_to_finish()
func (cb *commitBarrier) waitForAllWorkers(ctx context.Context) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.waitWhileLocked(ctx, func() bool { return cb.delegatedJobs > 0 })
}

// waitWhileLocked repeatedly waits on cond until either cond is met or ctx is cancelled.
// It spawns a short-lived goroutine per Wait() call that broadcasts on context cancellation,
// ensuring the caller never blocks past ctx cancellation.
// Caller must hold mu.
func (cb *commitBarrier) waitWhileLocked(ctx context.Context, condMet func() bool) {
	for condMet() {
		if ctx.Err() != nil {
			return
		}
		done := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				cb.cond.Broadcast()
			case <-done:
			}
		}()
		cb.cond.Wait()
		close(done)
	}
}

// completeDelegatedJob decrements the delegated job counter and wakes any
// waiter. The decrement and broadcast happen under mu so the wakeup cannot be
// lost against a concurrent waitForAllWorkers predicate check.
func (cb *commitBarrier) completeDelegatedJob() {
	cb.mu.Lock()
	cb.delegatedJobs--
	cb.cond.Broadcast()
	cb.mu.Unlock()
}

// reset clears the barrier state for a new logical-clock epoch (e.g. when
// sequence numbers restart after binlog rotation). Callers MUST ensure all
// delegated jobs have drained (via waitForAllWorkers) before resetting; this is
// done in-place to avoid racing with workers holding a reference to the barrier.
func (cb *commitBarrier) reset() {
	cb.mu.Lock()
	cb.lwm = seqUninit
	cb.dispatched = cb.dispatched[:0]
	cb.head = 0
	cb.committed = make(map[int64]bool)
	cb.delegatedJobs = 0
	cb.mu.Unlock()
}

// getLWM returns the current low water mark over the dispatched subsequence
// (thread-safe).
func (cb *commitBarrier) getLWM() int64 {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.lwm
}

// delegatedJobCount returns the number of in-flight delegated jobs (thread-safe).
func (cb *commitBarrier) delegatedJobCount() int64 {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.delegatedJobs
}
