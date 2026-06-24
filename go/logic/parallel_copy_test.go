/*
   Copyright 2025 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/sql"
)

// newFrontierTestMigrator builds a Migrator with just enough state to exercise
// advanceFrontier: a migration context (for TotalRowsCopied) and an applier (for the
// LastIterationRange* frontier + its mutex). No DB connection is needed.
func newFrontierTestMigrator(firstIteration int64) *Migrator {
	migrationContext := base.NewMigrationContext()
	migrationContext.Iteration = firstIteration
	mgtr := &Migrator{
		migrationContext: migrationContext,
		applier:          &Applier{migrationContext: migrationContext},
	}
	mgtr.resetParallelState(firstIteration)
	return mgtr
}

// distinctRange returns a fresh ColumnValues pointer so committed ranges can be told
// apart by identity in assertions.
func distinctRange() *sql.ColumnValues {
	return sql.NewColumnValues(1)
}

// TestFrontierInOrderCommit: chunks completing in iteration order each commit
// immediately, summing rows and advancing the frontier to the latest range.
func TestFrontierInOrderCommit(t *testing.T) {
	mgtr := newFrontierTestMigrator(0)
	r0, r1, r2 := distinctRange(), distinctRange(), distinctRange()

	mgtr.advanceFrontier(0, distinctRange(), r0, 10)
	require.Equal(t, int64(10), mgtr.migrationContext.GetTotalRowsCopied())
	require.Same(t, r0, mgtr.applier.LastIterationRangeMaxValues)

	mgtr.advanceFrontier(1, distinctRange(), r1, 20)
	mgtr.advanceFrontier(2, distinctRange(), r2, 30)

	require.Equal(t, int64(60), mgtr.migrationContext.GetTotalRowsCopied())
	require.Same(t, r2, mgtr.applier.LastIterationRangeMaxValues)
	require.Equal(t, int64(3), mgtr.parallelCopyNextCommit)
	require.Empty(t, mgtr.parallelCopyPending)
}

// TestFrontierOutOfOrderHoldAndRelease: a later chunk that finishes before its
// predecessors is held until the gap below it is filled, then released in order.
func TestFrontierOutOfOrderHoldAndRelease(t *testing.T) {
	mgtr := newFrontierTestMigrator(0)
	r0, r1, r2 := distinctRange(), distinctRange(), distinctRange()

	// iteration 2 finishes first: must NOT advance the frontier or rows.
	mgtr.advanceFrontier(2, distinctRange(), r2, 30)
	require.Equal(t, int64(0), mgtr.migrationContext.GetTotalRowsCopied())
	require.Nil(t, mgtr.applier.LastIterationRangeMaxValues)
	require.Equal(t, int64(0), mgtr.parallelCopyNextCommit)
	require.Len(t, mgtr.parallelCopyPending, 1)

	// iteration 0 commits only itself (1 still missing).
	mgtr.advanceFrontier(0, distinctRange(), r0, 10)
	require.Equal(t, int64(10), mgtr.migrationContext.GetTotalRowsCopied())
	require.Same(t, r0, mgtr.applier.LastIterationRangeMaxValues)
	require.Equal(t, int64(1), mgtr.parallelCopyNextCommit)

	// iteration 1 fills the gap and releases 1 then 2.
	mgtr.advanceFrontier(1, distinctRange(), r1, 20)
	require.Equal(t, int64(60), mgtr.migrationContext.GetTotalRowsCopied())
	require.Same(t, r2, mgtr.applier.LastIterationRangeMaxValues)
	require.Equal(t, int64(3), mgtr.parallelCopyNextCommit)
	require.Empty(t, mgtr.parallelCopyPending)
}

// TestFrontierGapSafety: with a gap open, the persisted frontier must never reflect
// a chunk above the gap, even after several higher chunks complete.
func TestFrontierGapSafety(t *testing.T) {
	mgtr := newFrontierTestMigrator(0)
	r0 := distinctRange()

	mgtr.advanceFrontier(0, distinctRange(), r0, 10) // commits 0
	// 2,3,4 complete while 1 is still in flight: all held.
	mgtr.advanceFrontier(2, distinctRange(), distinctRange(), 30)
	mgtr.advanceFrontier(3, distinctRange(), distinctRange(), 40)
	mgtr.advanceFrontier(4, distinctRange(), distinctRange(), 50)

	// Frontier and rows must still reflect only the contiguous prefix [0].
	require.Equal(t, int64(10), mgtr.migrationContext.GetTotalRowsCopied())
	require.Same(t, r0, mgtr.applier.LastIterationRangeMaxValues)
	require.Equal(t, int64(1), mgtr.parallelCopyNextCommit)
	require.Len(t, mgtr.parallelCopyPending, 3)
}

// TestFrontierResumeBase: when resuming, the next-commit cursor starts at the
// checkpoint iteration, and contiguous commit works from that base.
func TestFrontierResumeBase(t *testing.T) {
	mgtr := newFrontierTestMigrator(100)
	r100, r101, r102 := distinctRange(), distinctRange(), distinctRange()

	mgtr.advanceFrontier(100, distinctRange(), r100, 5)
	require.Equal(t, int64(101), mgtr.parallelCopyNextCommit)
	require.Same(t, r100, mgtr.applier.LastIterationRangeMaxValues)

	// 102 out of order is held; 101 then releases 101 and 102.
	mgtr.advanceFrontier(102, distinctRange(), r102, 7)
	require.Same(t, r100, mgtr.applier.LastIterationRangeMaxValues)
	mgtr.advanceFrontier(101, distinctRange(), r101, 6)

	require.Equal(t, int64(18), mgtr.migrationContext.GetTotalRowsCopied())
	require.Same(t, r102, mgtr.applier.LastIterationRangeMaxValues)
	require.Equal(t, int64(103), mgtr.parallelCopyNextCommit)
	require.Empty(t, mgtr.parallelCopyPending)
}
