/*
  Copyright 2016 GitHub Inc.
*/

package binlog

type BinlogEntry struct {
}

type BinlogReader interface {
	ReadEntries(logFile string, startPos uint, endPos uint) (entries [](*BinlogEntry), err error)
}
