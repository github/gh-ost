/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-osc/blob/master/LICENSE
*/

package binlog

// BinlogReader is a general interface whose implementations can choose their methods of reading
// a binary log file and parsing it into binlog entries
type BinlogReader interface {
	ReadEntries(logFile string, startPos uint64, stopPos uint64) (entries [](*BinlogEntry), err error)
}
