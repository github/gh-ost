/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package binlog

import (
	"fmt"

	"github.com/github/gh-ost/go/mysql"

	gomysql "github.com/siddontang/go-mysql/mysql"
)

// BinlogEntry describes an entry in the binary log
type BinlogEntry struct {
	Coordinates mysql.BinlogCoordinates
	DmlEvent    *BinlogDMLEvent
}

// NewBinlogEntry creates an empty, ready to go BinlogEntry object
func NewBinlogEntry(logFile string, logPos uint64, gtidSet gomysql.GTIDSet) *BinlogEntry {
	binlogEntry := &BinlogEntry{
		Coordinates: mysql.BinlogCoordinates{LogFile: logFile, LogPos: int64(logPos), ExecutedGTIDSet: gtidSet},
	}
	return binlogEntry
}

// NewBinlogEntryAt creates an empty, ready to go BinlogEntry object
func NewBinlogEntryAt(coordinates mysql.BinlogCoordinates) *BinlogEntry {
	binlogEntry := &BinlogEntry{
		Coordinates: coordinates,
	}
	return binlogEntry
}

// Duplicate creates and returns a new binlog entry, with some of the attributes pre-assigned
func (this *BinlogEntry) Duplicate() *BinlogEntry {
	return NewBinlogEntry(this.Coordinates.LogFile, uint64(this.Coordinates.LogPos), this.Coordinates.ExecutedGTIDSet)
}

// String() returns a string representation of this binlog entry
func (this *BinlogEntry) String() string {
	return fmt.Sprintf("[BinlogEntry at %+v; dml:%+v]", this.Coordinates, this.DmlEvent)
}
