/*
  Copyright 2016 GitHub Inc.
*/

package binlog

type MySQLBinlogReader struct {
}

func (this *MySQLBinlogReader) ReadEntries(logFile string, startPos uint, endPos uint) (entries [](*BinlogEntry), err error) {
	return entries, err
}
