/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-osc/blob/master/LICENSE
*/

package mysql

import ()

// BinlogEvent is a binary log event entry, with data
type BinlogEvent struct {
	TableName    string
	DatabaseName string
}
