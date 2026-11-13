/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package binlog

import (
	"fmt"
	"time"

	"github.com/github/gh-ost/go/mysql"
)

// BinlogEntry describes an entry in the binary log
type BinlogEntry struct {
	Coordinates mysql.BinlogCoordinates
	// Timestamp is the wall-clock time recorded in the binlog event header of the
	// event that produced this entry. For a source-position probe (DmlEvent == nil)
	// it is the local time before querying the source, not the time of delivery.
	Timestamp time.Time
	DmlEvent  *BinlogDMLEvent
}

// NewBinlogEntryAt creates an empty, ready to go BinlogEntry object
func NewBinlogEntryAt(coordinates mysql.BinlogCoordinates) *BinlogEntry {
	binlogEntry := &BinlogEntry{
		Coordinates: coordinates,
	}
	return binlogEntry
}

// String() returns a string representation of this binlog entry
func (ble *BinlogEntry) String() string {
	return fmt.Sprintf("[BinlogEntry at %+v; dml:%+v]", ble.Coordinates, ble.DmlEvent)
}
