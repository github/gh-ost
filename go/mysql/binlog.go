/*
   Copyright 2015 Shlomi Noach, courtesy Booking.com
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package mysql

import (
	"fmt"
	"strconv"
	"strings"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
)

// BinlogCoordinates described binary log coordinates in the form of a GTID set and/or log file & log position.
type BinlogCoordinates struct {
	GTIDSet   gomysql.GTIDSet
	LogFile   string
	LogPos    int64
	EventSize int64
}

// ParseFileBinlogCoordinates parses a log file/position string into a *BinlogCoordinates struct.
func ParseFileBinlogCoordinates(logFileLogPos string) (*BinlogCoordinates, error) {
	tokens := strings.SplitN(logFileLogPos, ":", 2)
	if len(tokens) != 2 {
		return nil, fmt.Errorf("ParseFileBinlogCoordinates: Cannot parse BinlogCoordinates from %s. Expected format is file:pos", logFileLogPos)
	}

	if logPos, err := strconv.ParseInt(tokens[1], 10, 0); err != nil {
		return nil, fmt.Errorf("ParseFileBinlogCoordinates: invalid pos: %s", tokens[1])
	} else {
		return &BinlogCoordinates{LogFile: tokens[0], LogPos: logPos}, nil
	}
}

// ParseGTIDSetBinlogCoordinates parses a MySQL GTID set into a *BinlogCoordinates struct.
func ParseGTIDSetBinlogCoordinates(gtidSet string) (*BinlogCoordinates, error) {
	set, err := gomysql.ParseMysqlGTIDSet(gtidSet)
	return &BinlogCoordinates{GTIDSet: set}, err
}

// DisplayString returns a user-friendly string representation of these coordinates
func (this *BinlogCoordinates) DisplayString() string {
	if this.GTIDSet != nil {
		return this.GTIDSet.String()
	}
	return fmt.Sprintf("%s:%d", this.LogFile, this.LogPos)
}

// String returns a user-friendly string representation of these coordinates
func (this BinlogCoordinates) String() string {
	return this.DisplayString()
}

// Equals tests equality of this coordinate and another one.
func (this *BinlogCoordinates) Equals(other *BinlogCoordinates) bool {
	if other == nil {
		return false
	}
	if this.GTIDSet != nil && !this.GTIDSet.Equal(other.GTIDSet) {
		return false
	}
	return this.LogFile == other.LogFile && this.LogPos == other.LogPos
}

// IsEmpty returns true if the log file and GTID set is empty, unnamed
func (this *BinlogCoordinates) IsEmpty() bool {
	return this.LogFile == "" && this.GTIDSet == nil
}

// SmallerThan returns true if this coordinate is strictly smaller than the other.
func (this *BinlogCoordinates) SmallerThan(other *BinlogCoordinates) bool {
	if this.LogFile < other.LogFile {
		return true
	}
	if this.LogFile == other.LogFile && this.LogPos < other.LogPos {
		return true
	}
	return false
}

// SmallerThanOrEquals returns true if this coordinate is the same or equal to the other one.
// We do NOT compare the type so we can not use this.Equals()
func (this *BinlogCoordinates) SmallerThanOrEquals(other *BinlogCoordinates) bool {
	if this.SmallerThan(other) {
		return true
	}
	if this.GTIDSet != nil && !this.GTIDSet.Equal(other.GTIDSet) {
		return false
	} else if other.GTIDSet != nil {
		return false
	}
	return this.LogFile == other.LogFile && this.LogPos == other.LogPos // No Type comparison
}

// IsLogPosOverflowBeyond4Bytes returns true if the coordinate endpos is overflow beyond 4 bytes.
// The binlog event end_log_pos field type is defined as uint32, 4 bytes.
// https://github.com/go-mysql-org/go-mysql/blob/master/replication/event.go
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_replication_binlog_event.html#sect_protocol_replication_binlog_event_header
// Issue: https://github.com/github/gh-ost/issues/1366
func (this *BinlogCoordinates) IsLogPosOverflowBeyond4Bytes(preCoordinate *BinlogCoordinates) bool {
	if preCoordinate == nil {
		return false
	}
	if preCoordinate.IsEmpty() {
		return false
	}

	if this.LogFile != preCoordinate.LogFile {
		return false
	}

	if preCoordinate.LogPos+this.EventSize >= 1<<32 {
		// Unexpected rows event, the previous binlog log_pos + current binlog event_size is overflow 4 bytes
		return true
	}
	return false
}
