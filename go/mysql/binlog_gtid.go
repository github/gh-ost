/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package mysql

import (
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
)

// GTIDBinlogCoordinates describe binary log coordinates in MySQL GTID format.
type GTIDBinlogCoordinates struct {
	GTIDSet *gomysql.MysqlGTIDSet
	UUIDSet *gomysql.UUIDSet
}

// NewGTIDBinlogCoordinates parses a MySQL GTID set into a *GTIDBinlogCoordinates struct.
func NewGTIDBinlogCoordinates(gtidSet string) (*GTIDBinlogCoordinates, error) {
	set, err := gomysql.ParseMysqlGTIDSet(gtidSet)
	return &GTIDBinlogCoordinates{
		GTIDSet: set.(*gomysql.MysqlGTIDSet),
	}, err
}

// DisplayString returns a user-friendly string representation of these current UUID set or the full GTID set.
func (coord *GTIDBinlogCoordinates) DisplayString() string {
	if coord.UUIDSet != nil {
		return coord.UUIDSet.String()
	}
	return coord.String()
}

// String returns a user-friendly string representation of these full GTID set.
func (coord GTIDBinlogCoordinates) String() string {
	return coord.GTIDSet.String()
}

// Equals tests equality of this coordinate and another one.
func (coord *GTIDBinlogCoordinates) Equals(other BinlogCoordinates) bool {
	if other == nil || coord.IsEmpty() || other.IsEmpty() {
		return false
	}

	otherCoords, ok := other.(*GTIDBinlogCoordinates)
	if !ok {
		return false
	}

	return coord.GTIDSet.Equal(otherCoords.GTIDSet)
}

// IsEmpty returns true if the GTID set is empty.
func (coord *GTIDBinlogCoordinates) IsEmpty() bool {
	return coord.GTIDSet == nil
}

// SmallerThan returns true if this coordinate is strictly smaller than the other.
func (coord *GTIDBinlogCoordinates) SmallerThan(other BinlogCoordinates) bool {
	if other == nil || coord.IsEmpty() || other.IsEmpty() {
		return false
	}
	otherCoords, ok := other.(*GTIDBinlogCoordinates)
	if !ok {
		return false
	}

	// if 'coord' does not contain the same sets we assume we are behind 'other'.
	// there are probably edge cases where this isn't true
	return !coord.GTIDSet.Contain(otherCoords.GTIDSet)
}

// SmallerThanOrEquals returns true if this coordinate is the same or equal to the other one.
func (coord *GTIDBinlogCoordinates) SmallerThanOrEquals(other BinlogCoordinates) bool {
	return coord.Equals(other) || coord.SmallerThan(other)
}

func (coord *GTIDBinlogCoordinates) Clone() BinlogCoordinates {
	out := &GTIDBinlogCoordinates{}
	if coord.GTIDSet != nil {
		out.GTIDSet = coord.GTIDSet.Clone().(*gomysql.MysqlGTIDSet)
	}
	if coord.UUIDSet != nil {
		out.UUIDSet = coord.UUIDSet.Clone()
	}
	return out
}
