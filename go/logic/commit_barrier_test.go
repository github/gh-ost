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

// --- LWM advancement tests ---

func TestCommitBarrier_LWMAdvancesConsecutively(t *testing.T) {
	cb := newCommitBarrier()
	require.Equal(t, int64(0), cb.getLWM())

	cb.commit(1)
	require.Equal(t, int64(1), cb.getLWM())

	cb.commit(2)
	require.Equal(t, int64(2), cb.getLWM())

	cb.commit(3)
	require.Equal(t, int64(3), cb.getLWM())
}

func TestCommitBarrier_LWMStopsAtGap(t *testing.T) {
	cb := newCommitBarrier()

	// Commit 1, skip 2, commit 3
	cb.commit(1)
	require.Equal(t, int64(1), cb.getLWM())

	cb.commit(3)
	// LWM stays at 1 because 2 is missing
	require.Equal(t, int64(1), cb.getLWM())

	// Now commit 2 — LWM should jump to 3
	cb.commit(2)
	require.Equal(t, int64(3), cb.getLWM())
}

func TestCommitBarrier_OutOfOrderCommitFillsGap(t *testing.T) {
	cb := newCommitBarrier()
	// Commit in reverse: 5, 4, 3, 2, 1
	cb.commit(5)
	require.Equal(t, int64(0), cb.getLWM())

	cb.commit(4)
	require.Equal(t, int64(0), cb.getLWM())

	cb.commit(3)
	require.Equal(t, int64(0), cb.getLWM())

	cb.commit(2)
	require.Equal(t, int64(0), cb.getLWM())

	// Filling gap: commit 1 should advance LWM all the way to 5
	cb.commit(1)
	require.Equal(t, int64(5), cb.getLWM())
}

func TestCommitBarrier_CommitZeroIsNoop(t *testing.T) {
	cb := newCommitBarrier()
	cb.commit(0)
	require.Equal(t, int64(0), cb.getLWM())
}

// --- waitForDependency tests ---

func TestCommitBarrier_WaitForDependencyAlreadySatisfied(t *testing.T) {
	cb := newCommitBarrier()
	for i := int64(1); i <= 5; i++ {
		cb.commit(i)
	}
	require.Equal(t, int64(5), cb.getLWM())

	ctx := context.Background()
	// LWM is 5, lastCommitted=5 should return immediately
	cb.waitForDependency(ctx, 5, true)
}

func TestCommitBarrier_WaitForDependencyBlocksUntilLWMAdvances(t *testing.T) {
	cb := newCommitBarrier()
	ctx := context.Background()

	done := make(chan struct{})
	go func() {
		cb.waitForDependency(ctx, 3, true)
		close(done)
	}()

	// Should block because LWM is 0
	require.Eventually(t, func() bool {
		select {
		case <-done:
			return false
		default:
			return true
		}
	}, 200*time.Millisecond, 10*time.Millisecond)

	// Commit 1, 2, 3 consecutively — LWM advances to 3
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
	cb.waitForDependency(context.Background(), 0, true)
}

func TestCommitBarrier_WaitForDependencyCrossTableIsNoop(t *testing.T) {
	cb := newCommitBarrier()
	ctx := context.Background()

	done := make(chan struct{})
	go func() {
		// parentSeenOnStream=false: cross-table dependency, treated as satisfied
		cb.waitForDependency(ctx, 99, false)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("waitForDependency blocked on unseen cross-table parent")
	}
}

// --- waitForAllWorkers tests ---

func TestCommitBarrier_WaitForAllWorkers(t *testing.T) {
	cb := newCommitBarrier()
	ctx := context.Background()

	cb.addDelegatedJob()
	cb.addDelegatedJob()

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

// --- Concurrent tests ---

func TestCommitBarrier_ConcurrentCommitsAdvanceLWM(t *testing.T) {
	cb := newCommitBarrier()
	var wg sync.WaitGroup
	for i := int64(1); i <= 20; i++ {
		wg.Add(1)
		cb.addDelegatedJob()
		go func(seq int64) {
			defer wg.Done()
			cb.commit(seq)
			cb.completeDelegatedJob()
		}(i)
	}
	wg.Wait()
	require.Equal(t, int64(20), cb.getLWM())
}

// --- PR #1454 regression tests ---
// These test scenarios from meiji163's and dnovitski's investigation:
// https://github.com/github/gh-ost/pull/1454#issuecomment-2798932454
// https://github.com/github/gh-ost/pull/1454#issuecomment-4340103322

func TestCommitBarrier_PR1454_OutOfOrderDoesNotShortCircuit(t *testing.T) {
	// Core bug: old code checked committed[lastCommitted] (per-sequence)
	// instead of lwm >= lastCommitted (gap-free LWM).
	//
	// Scenario from meiji163's analysis:
	//   trx A: seq=89058, lc=89053  (modifies row id=5025)
	//   trx B: seq=89065, lc=89062  (modifies row id=5025)
	//
	// B must wait until LWM >= 89062, which guarantees A is also complete.

	cb := newCommitBarrier()

	// Commit prefix 1..89053
	for i := int64(1); i <= 89053; i++ {
		cb.commit(i)
	}
	require.Equal(t, int64(89053), cb.getLWM())

	// trx A dispatched (seq=89058) and a few others up to 89062
	cb.commit(89058)
	cb.commit(89054)
	cb.commit(89055)
	cb.commit(89056)
	cb.commit(89057)
	cb.commit(89059)
	cb.commit(89060)
	cb.commit(89061)
	cb.commit(89062)

	// LWM should now be 89062 (all consecutive from 1)
	require.Equal(t, int64(89062), cb.getLWM())

	// trx B (lc=89062) can proceed because LWM >= 89062
	// This is correct: 89058 has also completed (it's <= LWM)
	ctx := context.Background()
	cb.waitForDependency(ctx, 89062, true)
}

func TestCommitBarrier_PR1454_WaitBlocksWhenLWMBelowLastCommitted(t *testing.T) {
	// LWM is below lastCommitted — must block until it catches up
	cb := newCommitBarrier()
	ctx := context.Background()

	for i := int64(1); i <= 10; i++ {
		cb.commit(i)
	}
	require.Equal(t, int64(10), cb.getLWM())

	done := make(chan struct{})
	go func() {
		cb.waitForDependency(ctx, 15, true)
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
	// After binlog rotation, sequence numbers restart from 1.
	// The coordinator creates a new commitBarrier with LWM=0.
	cb := newCommitBarrier()

	for i := int64(1); i <= 65553; i++ {
		cb.commit(i)
	}
	require.Equal(t, int64(65553), cb.getLWM())

	// Simulate rotation: fresh barrier
	cb2 := newCommitBarrier()
	require.Equal(t, int64(0), cb2.getLWM())

	cb2.commit(1)
	require.Equal(t, int64(1), cb2.getLWM())

	ctx := context.Background()
	done := make(chan struct{})
	go func() {
		cb2.waitForDependency(ctx, 5, true)
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
		cb2.commit(i)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("should unblock after LWM advances in new epoch")
	}
}

func TestCommitBarrier_PR1454_ErrorDoesNotMarkCommitted(t *testing.T) {
	// If a worker fails, its sequence should NOT be committed.
	// This prevents the dependency tracker from believing a failed
	// transaction completed (Bug 2 from dnovitski's analysis).
	cb := newCommitBarrier()

	cb.commit(1)
	cb.commit(2)
	require.Equal(t, int64(2), cb.getLWM())

	// seq=3 is dispatched but worker fails — do NOT call commit(3)
	require.Equal(t, int64(2), cb.getLWM())

	// seq=4 commits but can't advance LWM past 2 (gap at 3)
	cb.commit(4)
	require.Equal(t, int64(2), cb.getLWM())

	ctx := context.Background()
	done := make(chan struct{})
	go func() {
		cb.waitForDependency(ctx, 3, true)
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

	// Retry seq=3 successfully
	cb.commit(3)
	require.Equal(t, int64(4), cb.getLWM())

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("waiter should unblock once failed transaction is retried")
	}
}
