/*
   Copyright 2025 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMTSSchedule_FirstEventIsNewGroup(t *testing.T) {
	s := newMTSScheduleState()
	isNew, err := s.evaluateTransaction(6, 5)
	require.NoError(t, err)
	require.True(t, isNew)
}

func TestMTSSchedule_SequenceUninitIsNewGroup(t *testing.T) {
	s := newMTSScheduleState()
	_, _ = s.evaluateTransaction(1, 0)
	isNew, err := s.evaluateTransaction(0, 0)
	require.NoError(t, err)
	require.True(t, isNew)
}

func TestMTSSchedule_LastCommittedUninitIsNewGroup(t *testing.T) {
	s := newMTSScheduleState()
	_, _ = s.evaluateTransaction(1, 0)
	isNew, err := s.evaluateTransaction(2, 0)
	require.NoError(t, err)
	require.True(t, isNew)
}

func TestMTSSchedule_GapSuccessorIsNewGroup(t *testing.T) {
	s := newMTSScheduleState()
	_, _ = s.evaluateTransaction(5, 4)
	isNew, err := s.evaluateTransaction(8, 7)
	require.NoError(t, err)
	require.True(t, isNew)
}

func TestMTSSchedule_NormalParallelNotNewGroup(t *testing.T) {
	s := newMTSScheduleState()
	_, _ = s.evaluateTransaction(5, 4)
	isNew, err := s.evaluateTransaction(6, 5)
	require.NoError(t, err)
	require.False(t, isNew)
}

func TestMTSSchedule_SameSequenceNumberTwiceNoError(t *testing.T) {
	s := newMTSScheduleState()
	_, err := s.evaluateTransaction(5, 4)
	require.NoError(t, err)
	// Simulates mis-invocation per row within one transaction; must not trip seq check.
	_, err = s.evaluateTransaction(5, 4)
	require.NoError(t, err)
}

func TestMTSSchedule_SequenceLeqLastCommittedErrors(t *testing.T) {
	s := newMTSScheduleState()
	_, err := s.evaluateTransaction(5, 6)
	require.Error(t, err)
	require.Contains(t, err.Error(), "inconsistent timestamps")
}

func TestMTSSchedule_SequenceResetStartsNewEpoch(t *testing.T) {
	s := newMTSScheduleState()
	_, err := s.evaluateTransaction(534998, 534997)
	require.NoError(t, err)

	isNew, err := s.evaluateTransaction(1, 0)
	require.NoError(t, err)
	require.True(t, isNew)
	require.True(t, s.consumeEpochReset())
}
