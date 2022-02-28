/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package mysql

import (
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
)

// GTIDBinlogCoordinates described binary log coordinates in the form of a binlog file & log position.
type GTIDBinlogCoordinates struct {
	Set *gomysql.MysqlGTIDSet
}

// ParseGTIDSetBinlogCoordinates parses a MySQL GTID set into a *GTIDBinlogCoordinates struct.
func ParseGTIDSetBinlogCoordinates(gtidSet string) (*GTIDBinlogCoordinates, error) {
	set, err := gomysql.ParseMysqlGTIDSet(gtidSet)
	return &GTIDBinlogCoordinates{set.(*gomysql.MysqlGTIDSet)}, err
}

// DisplayString returns a user-friendly string representation of these coordinates
func (this *GTIDBinlogCoordinates) DisplayString() string {
	return this.Set.String()
}

// String returns a user-friendly string representation of these coordinates
func (this GTIDBinlogCoordinates) String() string {
	return this.DisplayString()
}

// Equals tests equality of this coordinate and another one.
func (this *GTIDBinlogCoordinates) Equals(other *GTIDBinlogCoordinates) bool {
	if other == nil {
		return false
	}
	return other.Set != nil && this.Set.Equal(other.Set)
}

// IsEmpty returns true if the GTID set is empty, unnamed
func (this *GTIDBinlogCoordinates) IsEmpty() bool {
	return this.Set == nil
}

// SmallerThan returns true if this coordinate is strictly smaller than the other.
func (this *GTIDBinlogCoordinates) SmallerThan(other *GTIDBinlogCoordinates) bool {
	// if GTID SIDs are equal we compare the interval stop points
	// if GTID SIDs differ we have to assume there is a new/larger event
	if other.Set == nil || other.Set.Sets == nil {
		return false
	}
	if len(this.Set.Sets) < len(other.Set.Sets) {
		return true
	}
	for sid, otherSet := range other.Set.Sets {
		thisSet, ok := this.Set.Sets[sid]
		if !ok {
			return true // 'this' is missing an SID
		}
		if len(thisSet.Intervals) < len(otherSet.Intervals) {
			return true // 'this' has fewer intervals
		}
		for i, otherInterval := range otherSet.Intervals {
			if len(thisSet.Intervals)-1 > i {
				return true
			}
			thisInterval := thisSet.Intervals[i]
			if thisInterval.Start < otherInterval.Start || thisInterval.Stop < otherInterval.Stop {
				return true
			}
		}
	}
	return false
}

// SmallerThanOrEquals returns true if this coordinate is the same or equal to the other one.
// We do NOT compare the type so we can not use this.Equals()
func (this *GTIDBinlogCoordinates) SmallerThanOrEquals(other *GTIDBinlogCoordinates) bool {
	if this.SmallerThan(other) {
		return true
	}
	return other.Set != nil && this.Set.Equal(other.Set)
}
