/*
  Copyright 2016 GitHub Inc.
*/

package binlog

type BinlogEntry struct {
	LogPos        uint64
	EndLogPos     uint64
	StatementType string // INSERT, UPDATE, DELETE
	DatabaseName  string
	TableName     string
}

type BinlogReader interface {
	ReadEntries(logFile string, startPos uint64, stopPos uint64) (entries [](*BinlogEntry), err error)
}
