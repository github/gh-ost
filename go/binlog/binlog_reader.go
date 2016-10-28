/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package binlog

// BinlogReader is a general interface whose implementations can choose their methods of reading
// a binary log file and parsing it into binlog entries
type BinlogReader interface {
	StreamEvents(canStopStreaming func() bool, entriesChannel chan<- *BinlogEntry) error
	Reconnect() error
}
