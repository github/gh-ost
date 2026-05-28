/*
   Copyright 2025 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import "fmt"

// mtsScheduleState tracks LOGICAL_CLOCK scheduling state across transactions.
// MySQL validates and schedules per GTID transaction, not per row.
type mtsScheduleState struct {
	lastTrxSeqNum int64
	lastSeqNum    int64
	firstEvent    bool
	epochReset    bool
	seenSequence  map[int64]struct{} // sequence numbers observed on this table's binlog stream
}

func newMTSScheduleState() *mtsScheduleState {
	return &mtsScheduleState{
		firstEvent:   true,
		seenSequence: make(map[int64]struct{}),
	}
}

func (s *mtsScheduleState) observeTransaction(sequenceNum int64) {
	if sequenceNum == 0 {
		return
	}
	s.seenSequence[sequenceNum] = struct{}{}
}

func (s *mtsScheduleState) hasSeenSequence(sequenceNum int64) bool {
	_, ok := s.seenSequence[sequenceNum]
	return ok
}

func (s *mtsScheduleState) consumeEpochReset() bool {
	if !s.epochReset {
		return false
	}
	s.epochReset = false
	return true
}

func (s *mtsScheduleState) resetEpoch() {
	s.lastTrxSeqNum = 0
	s.lastSeqNum = 0
	s.firstEvent = true
	s.seenSequence = make(map[int64]struct{})
	s.epochReset = true
}

// evaluateTransaction returns whether the transaction starts a new serialized group
// and updates internal state. Call once per transaction (after row grouping).
func (s *mtsScheduleState) evaluateTransaction(currentSeqNum, currentLastCommitted int64) (isNewGroup bool, err error) {
	if currentSeqNum != 0 && currentSeqNum != s.lastTrxSeqNum {
		if currentLastCommitted != 0 && currentSeqNum <= currentLastCommitted {
			return false, fmt.Errorf("inconsistent timestamps: seq=%d <= lc=%d", currentSeqNum, currentLastCommitted)
		}
		if s.lastTrxSeqNum != 0 && currentSeqNum <= s.lastTrxSeqNum {
			// Logical clock counter wrapped or restarted; begin a new epoch.
			s.resetEpoch()
		} else {
			s.lastTrxSeqNum = currentSeqNum
		}
	}

	gapSuccessor := s.lastSeqNum != 0 && currentSeqNum != s.lastSeqNum && currentSeqNum > s.lastSeqNum+1
	isNewGroup = s.firstEvent ||
		currentSeqNum == 0 ||
		currentLastCommitted == 0 ||
		gapSuccessor ||
		(s.lastSeqNum == 0 && currentSeqNum != 0)

	s.firstEvent = false
	s.lastSeqNum = currentSeqNum
	return isNewGroup, nil
}
