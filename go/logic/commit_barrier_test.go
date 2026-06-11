/*
   Copyright 2025 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// dispatch records a set of sequence numbers as dispatched (seen) jobs, in the
// order given. The LWM only advances over dispatched sequences, mirroring how
// the coordinator registers every transaction via addDelegatedJob before a
// worker commits it.
func dispatch(cb *commitBarrier, seqs ...int64) {
	for _, s := range seqs {
		cb.addDelegatedJob(s)
	}
}

// --- LWM advancement tests ---

func TestCommitBarrier_LWMAdvancesConsecutively(t *testing.T) {
	cb := newCommitBarrier()
	require.Equal(t, int64(0), cb.getLWM())
	dispatch(cb, 1, 2, 3)

	cb.commit(1)
	require.Equal(t, int64(1), cb.getLWM())

	cb.commit(2)
	require.Equal(t, int64(2), cb.getLWM())

	cb.commit(3)
	require.Equal(t, int64(3), cb.getLWM())
}

func TestCommitBarrier_LWMStopsAtGap(t *testing.T) {
	cb := newCommitBarrier()
	dispatch(cb, 1, 2, 3)

	cb.commit(1)
	require.Equal(t, int64(1), cb.getLWM())

	// Commit 3 before 2: LWM stays at 1 because 2 (earlier in dispatch order)
	// is not yet committed.
	cb.commit(3)
	require.Equal(t, int64(1), cb.getLWM())

	// Now commit 2 — LWM should jump to 3.
	cb.commit(2)
	require.Equal(t, int64(3), cb.getLWM())
}

func TestCommitBarrier_OutOfOrderCommitFillsGap(t *testing.T) {
	cb := newCommitBarrier()
	dispatch(cb, 1, 2, 3, 4, 5)

	cb.commit(5)
	require.Equal(t, int64(0), cb.getLWM())
	cb.commit(4)
	require.Equal(t, int64(0), cb.getLWM())
	cb.commit(3)
	require.Equal(t, int64(0), cb.getLWM())
	cb.commit(2)
	require.Equal(t, int64(0), cb.getLWM())

	// Filling the front: commit 1 should advance LWM all the way to 5.
	cb.commit(1)
	require.Equal(t, int64(5), cb.getLWM())
}

// TestCommitBarrier_SparseSequencesAdvance is the regression test for the
// production deadlock: gh-ost only sees its own table's binlog rows, so the
// observed sequence numbers are sparse (other tables' transactions create
// gaps). A gap-free LWM over the global sequence space would stall at the first
// gap; the dispatched-subsequence LWM must advance normally.
func TestCommitBarrier_SparseSequencesAdvance(t *testing.T) {
	cb := newCommitBarrier()
	// Sequences observed for our table, with large gaps from other tables.
	seen := []int64{5, 9, 14, 22, 100, 537}
	dispatch(cb, seen...)

	for _, s := range seen {
		cb.commit(s)
	}
	require.Equal(t, int64(537), cb.getLWM())

	// A later transaction depends on a seen parent (lc=100). It must not block,
	// because every seen sequence <= 100 has committed even though global
	// sequences 1..99 (other tables) were never observed.
	done := make(chan struct{})
	go func() {
		cb.waitForDependency(context.Background(), 100)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("waitForDependency stalled on sparse sequence space")
	}
}

// TestCommitBarrier_SparseDependencyBlocksUntilSeenParent verifies the ordering
// guarantee still holds with sparse sequences: a child depending on a seen
// parent waits until that parent (and all earlier seen transactions) commit.
func TestCommitBarrier_SparseDependencyBlocksUntilSeenParent(t *testing.T) {
	cb := newCommitBarrier()
	dispatch(cb, 5, 9, 14)
	ctx := context.Background()

	// Commit 5 and 14, but NOT 9 (earlier in dispatch order than 14).
	cb.commit(5)
	cb.commit(14)
	require.Equal(t, int64(5), cb.getLWM())

	done := make(chan struct{})
	go func() {
		// Child depends on seen parent seq=9.
		cb.waitForDependency(ctx, 9)
		close(done)
	}()
	require.Eventually(t, func() bool {
		select {
		case <-done:
			return false
		default:
			return true
		}
	}, 200*time.Millisecond, 10*time.Millisecond)

	cb.commit(9)
	require.Equal(t, int64(14), cb.getLWM())
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("waitForDependency should unblock once seen parent committed")
	}
}

func TestCommitBarrier_CommitZeroIsNoop(t *testing.T) {
	cb := newCommitBarrier()
	cb.commit(0)
	require.Equal(t, int64(0), cb.getLWM())
}

// --- waitForDependency tests ---

func TestCommitBarrier_WaitForDependencyAlreadySatisfied(t *testing.T) {
	cb := newCommitBarrier()
	dispatch(cb, 1, 2, 3, 4, 5)
	for i := int64(1); i <= 5; i++ {
		cb.commit(i)
	}
	require.Equal(t, int64(5), cb.getLWM())

	// LWM is 5, lastCommitted=5 should return immediately.
	cb.waitForDependency(context.Background(), 5)
}

func TestCommitBarrier_WaitForDependencyBlocksUntilLWMAdvances(t *testing.T) {
	cb := newCommitBarrier()
	dispatch(cb, 1, 2, 3)
	ctx := context.Background()

	done := make(chan struct{})
	go func() {
		cb.waitForDependency(ctx, 3)
		close(done)
	}()

	require.Eventually(t, func() bool {
		select {
		case <-done:
			return false
		default:
			return true
		}
	}, 200*time.Millisecond, 10*time.Millisecond)

	cb.commit(1)
	cb.commit(2)
	cb.commit(3)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("waitForDependency did not unblock after LWM advanced")
	}
}

func TestCommitBarrier_WaitForDependencyZeroIsNoop(t *testing.T) {
	cb := newCommitBarrier()
	cb.waitForDependency(context.Background(), 0)
}

func TestCommitBarrier_WaitForDependencyCrossTableIsNoop(t *testing.T) {
	cb := newCommitBarrier()
	ctx := context.Background()

	// Dispatch only sequences on this table's stream; 99 is never dispatched, so
	// it is a cross-table parent and must be treated as satisfied immediately.
	dispatch(cb, 5, 9, 14)
	cb.commit(5)
	cb.commit(9)
	cb.commit(14)

	done := make(chan struct{})
	go func() {
		cb.waitForDependency(ctx, 99)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("waitForDependency blocked on unseen cross-table parent")
	}
}

// TestCommitBarrier_WaitForDependencyUndispatchedGapIsNoop is the regression
// guard for the coordinator hang: a lastCommitted value that lies BETWEEN two
// dispatched sequences but was itself never dispatched (its parent transaction
// touched another table) must be treated as a satisfied cross-table dependency,
// never blocking. Previously a separate "seen" set could disagree with the
// barrier's dispatched set and deadlock the coordinator here.
func TestCommitBarrier_WaitForDependencyUndispatchedGapIsNoop(t *testing.T) {
	cb := newCommitBarrier()
	ctx := context.Background()

	// We saw transactions 5 and 10 on our stream; 7 was on another table.
	dispatch(cb, 5, 10)
	cb.commit(5) // lwm = 5; 10 still pending

	done := make(chan struct{})
	go func() {
		// 7 > lwm(5) and 7 is not a pending dispatched seq -> cross-table -> noop.
		cb.waitForDependency(ctx, 7)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("waitForDependency deadlocked on an undispatched gap sequence")
	}
}

// --- waitForAllWorkers tests ---

func TestCommitBarrier_WaitForAllWorkers(t *testing.T) {
	cb := newCommitBarrier()
	ctx := context.Background()

	cb.addDelegatedJob(1)
	cb.addDelegatedJob(2)

	done := make(chan struct{})
	go func() {
		cb.waitForAllWorkers(ctx)
		close(done)
	}()

	require.Eventually(t, func() bool {
		select {
		case <-done:
			return false
		default:
			return true
		}
	}, 200*time.Millisecond, 10*time.Millisecond)

	cb.completeDelegatedJob()
	cb.completeDelegatedJob()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("waitForAllWorkers did not unblock")
	}
}

// TestCommitBarrier_WaitForAllWorkersNoLostWakeup stresses the window between a
// waiter's predicate check and its park inside cond.Wait(). Before the fix,
// delegatedJobs was mutated outside the lock, so a decrement-to-zero racing
// with the waiter could lose the broadcast and deadlock the coordinator at a
// group boundary. Each iteration must terminate; a hang means the regression
// is back.
func TestCommitBarrier_WaitForAllWorkersNoLostWakeup(t *testing.T) {
	ctx := context.Background()
	for iter := 0; iter < 2000; iter++ {
		cb := newCommitBarrier()
		cb.addDelegatedJob(1)

		go cb.completeDelegatedJob()

		done := make(chan struct{})
		go func() {
			cb.waitForAllWorkers(ctx)
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("waitForAllWorkers lost wakeup on iteration %d", iter)
		}
	}
}

// --- Concurrent tests ---

func TestCommitBarrier_ConcurrentCommitsAdvanceLWM(t *testing.T) {
	cb := newCommitBarrier()
	var wg sync.WaitGroup
	for i := int64(1); i <= 20; i++ {
		wg.Add(1)
		cb.addDelegatedJob(i)
		go func(seq int64) {
			defer wg.Done()
			cb.commit(seq)
			cb.completeDelegatedJob()
		}(i)
	}
	wg.Wait()
	require.Equal(t, int64(20), cb.getLWM())
}

// TestCommitBarrier_Reset verifies epoch reset clears all state.
func TestCommitBarrier_Reset(t *testing.T) {
	cb := newCommitBarrier()
	dispatch(cb, 1, 2, 3)
	cb.commit(1)
	cb.commit(2)
	require.Equal(t, int64(2), cb.getLWM())

	cb.reset()
	require.Equal(t, int64(0), cb.getLWM())
	require.Equal(t, int64(0), cb.delegatedJobCount())

	// New epoch starts numbering from 1 again.
	dispatch(cb, 1, 2)
	cb.commit(1)
	require.Equal(t, int64(1), cb.getLWM())
}

// --- PR #1454 regression tests ---
// Scenarios from meiji163's and dnovitski's investigation:
// https://github.com/github/gh-ost/pull/1454#issuecomment-2798932454
// https://github.com/github/gh-ost/pull/1454#issuecomment-4340103322

func TestCommitBarrier_PR1454_OutOfOrderDoesNotShortCircuit(t *testing.T) {
	// Scenario:
	//   trx A: seq=89058, lc=89053  (modifies row id=5025)
	//   trx B: seq=89065, lc=89062  (modifies row id=5025)
	//
	// B must wait until LWM >= 89062, which guarantees A (89058 <= 89062) is
	// also complete. Per-sequence membership of just lc would be insufficient.
	cb := newCommitBarrier()

	for i := int64(1); i <= 89062; i++ {
		cb.addDelegatedJob(i)
	}
	for i := int64(1); i <= 89053; i++ {
		cb.commit(i)
	}
	require.Equal(t, int64(89053), cb.getLWM())

	// Commit A (89058) out of order, then fill the gap up to 89062.
	cb.commit(89058)
	require.Equal(t, int64(89053), cb.getLWM())
	for _, s := range []int64{89054, 89055, 89056, 89057, 89059, 89060, 89061, 89062} {
		cb.commit(s)
	}
	require.Equal(t, int64(89062), cb.getLWM())

	// trx B (lc=89062) can proceed: 89058 has completed (it's <= LWM).
	cb.waitForDependency(context.Background(), 89062)
}

func TestCommitBarrier_PR1454_WaitBlocksWhenLWMBelowLastCommitted(t *testing.T) {
	cb := newCommitBarrier()
	ctx := context.Background()

	for i := int64(1); i <= 15; i++ {
		cb.addDelegatedJob(i)
	}
	for i := int64(1); i <= 10; i++ {
		cb.commit(i)
	}
	require.Equal(t, int64(10), cb.getLWM())

	done := make(chan struct{})
	go func() {
		cb.waitForDependency(ctx, 15)
		close(done)
	}()

	require.Eventually(t, func() bool {
		select {
		case <-done:
			return false
		default:
			return true
		}
	}, 200*time.Millisecond, 10*time.Millisecond)

	for i := int64(11); i <= 15; i++ {
		cb.commit(i)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("waitForDependency should have unblocked once LWM reached 15")
	}
	require.Equal(t, int64(15), cb.getLWM())
}

func TestCommitBarrier_PR1454_BinlogRotationResetsLWM(t *testing.T) {
	// After binlog rotation, sequence numbers restart from 1. The coordinator
	// drains workers and calls reset() so the LWM restarts at 0.
	cb := newCommitBarrier()
	for i := int64(1); i <= 65553; i++ {
		cb.addDelegatedJob(i)
		cb.commit(i)
	}
	require.Equal(t, int64(65553), cb.getLWM())

	cb.reset()
	require.Equal(t, int64(0), cb.getLWM())

	dispatch(cb, 1, 2, 3, 4, 5)
	cb.commit(1)
	require.Equal(t, int64(1), cb.getLWM())

	ctx := context.Background()
	done := make(chan struct{})
	go func() {
		cb.waitForDependency(ctx, 5)
		close(done)
	}()

	require.Eventually(t, func() bool {
		select {
		case <-done:
			return false
		default:
			return true
		}
	}, 200*time.Millisecond, 10*time.Millisecond)

	for i := int64(2); i <= 5; i++ {
		cb.commit(i)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("should unblock after LWM advances in new epoch")
	}
}

func TestCommitBarrier_PR1454_ErrorDoesNotMarkCommitted(t *testing.T) {
	// If a worker fails, its sequence should NOT advance the LWM past it, so a
	// dependent transaction keeps waiting until the failed one is retried.
	cb := newCommitBarrier()
	dispatch(cb, 1, 2, 3, 4)

	cb.commit(1)
	cb.commit(2)
	require.Equal(t, int64(2), cb.getLWM())

	// seq=4 commits but can't advance LWM past 2 (seq=3 still pending).
	cb.commit(4)
	require.Equal(t, int64(2), cb.getLWM())

	ctx := context.Background()
	done := make(chan struct{})
	go func() {
		cb.waitForDependency(ctx, 3)
		close(done)
	}()

	require.Eventually(t, func() bool {
		select {
		case <-done:
			return false
		default:
			return true
		}
	}, 200*time.Millisecond, 10*time.Millisecond)

	// Retry seq=3 successfully.
	cb.commit(3)
	require.Equal(t, int64(4), cb.getLWM())

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("waiter should unblock once failed transaction is retried")
	}
}
