/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package binlog

import (
	"fmt"

	"github.com/github/gh-ost/go/mysql"
)

// BinlogEntry describes an entry in the binary log
type BinlogEntry struct {
	Coordinates mysql.BinlogCoordinates
	DmlEvent    *BinlogDMLEvent
}

// NewBinlogEntryAt creates an empty, ready to go BinlogEntry object
func NewBinlogEntryAt(coordinates mysql.BinlogCoordinates) *BinlogEntry {
	binlogEntry := &BinlogEntry{
		Coordinates: coordinates,
	}
	return binlogEntry
}

// String() returns a string representation of this binlog entry
func (this *BinlogEntry) String() string {
	return fmt.Sprintf("[BinlogEntry at %+v; dml:%+v]", this.Coordinates, this.DmlEvent)
}
