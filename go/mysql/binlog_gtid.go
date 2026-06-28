/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package mysql

import (
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
)

// Re-exported go-mysql flavor identifiers so the rest of gh-ost doesn't have to
// import go-mysql directly to talk about flavors.
const (
	MySQLFlavor   = gomysql.MySQLFlavor
	MariaDBFlavor = gomysql.MariaDBFlavor
)

// FlavorFor returns the go-mysql flavor identifier for the given server version
// string. It is used to parse GTID sets and to configure the binlog syncer in
// the correct (MySQL vs MariaDB) GTID dialect.
func FlavorFor(mysqlVersion string) string {
	if IsMariaDB(mysqlVersion) {
		return MariaDBFlavor
	}
	return MySQLFlavor
}

// GTIDBinlogCoordinates describe binary log coordinates as a GTID set. The
// underlying set is either a MySQL or a MariaDB GTID set depending on the
// flavor it was parsed with; all operations go through the gomysql.GTIDSet
// interface so the two flavors are handled uniformly.
type GTIDBinlogCoordinates struct {
	GTIDSet gomysql.GTIDSet
}

// NewGTIDBinlogCoordinates parses a GTID set string (in the given flavor's
// dialect) into a *GTIDBinlogCoordinates struct.
func NewGTIDBinlogCoordinates(flavor, gtidSet string) (*GTIDBinlogCoordinates, error) {
	set, err := gomysql.ParseGTIDSet(flavor, gtidSet)
	if err != nil {
		return nil, err
	}
	return &GTIDBinlogCoordinates{GTIDSet: set}, nil
}

// DisplayString returns a user-friendly string representation of these current UUID set or the full GTID set.
func (coord *GTIDBinlogCoordinates) DisplayString() string {
	return coord.String()
}

// String returns a user-friendly string representation of these full GTID set.
func (coord GTIDBinlogCoordinates) String() string {
	if coord.GTIDSet == nil {
		return ""
	}
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
		out.GTIDSet = coord.GTIDSet.Clone()
	}
	return out
}
