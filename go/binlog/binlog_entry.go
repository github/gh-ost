/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-osc/blob/master/LICENSE
*/

package binlog

// BinlogEntry describes an entry in the binary log
type BinlogEntry struct {
	LogPos            uint64
	EndLogPos         uint64
	StatementType     string // INSERT, UPDATE, DELETE
	DatabaseName      string
	TableName         string
	PositionalColumns map[uint64]interface{}
}

// NewBinlogEntry creates an empty, ready to go BinlogEntry object
func NewBinlogEntry() *BinlogEntry {
	binlogEntry := &BinlogEntry{}
	binlogEntry.PositionalColumns = make(map[uint64]interface{})
	return binlogEntry
}

// Duplicate creates and returns a new binlog entry, with some of the attributes pre-assigned
func (this *BinlogEntry) Duplicate() *BinlogEntry {
	binlogEntry := NewBinlogEntry()
	binlogEntry.LogPos = this.LogPos
	binlogEntry.EndLogPos = this.EndLogPos
	return binlogEntry
}
