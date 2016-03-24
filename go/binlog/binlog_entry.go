/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-osc/blob/master/LICENSE
*/

package binlog

type BinlogEntry struct {
	LogPos        uint64
	EndLogPos     uint64
	StatementType string // INSERT, UPDATE, DELETE
	DatabaseName  string
	TableName     string
}
