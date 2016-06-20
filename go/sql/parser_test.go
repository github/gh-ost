/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package sql

import (
	"testing"

	"github.com/outbrain/golib/log"
	test "github.com/outbrain/golib/tests"
)

func init() {
	log.SetLevel(log.ERROR)
}

func TestParseAlterStatement(t *testing.T) {
	statement := "add column t int, engine=innodb"
	parser := NewParser()
	err := parser.ParseAlterStatement(statement)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectFalse(parser.HasNonTrivialRenames())
}

func TestParseAlterStatementTrivialRename(t *testing.T) {
	statement := "add column t int, change ts ts timestamp, engine=innodb"
	parser := NewParser()
	err := parser.ParseAlterStatement(statement)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectFalse(parser.HasNonTrivialRenames())
	test.S(t).ExpectEquals(len(parser.columnRenameMap), 1)
	test.S(t).ExpectEquals(parser.columnRenameMap["ts"], "ts")
}

func TestParseAlterStatementTrivialRenames(t *testing.T) {
	statement := "add column t int, change ts ts timestamp, CHANGE f `f` float, engine=innodb"
	parser := NewParser()
	err := parser.ParseAlterStatement(statement)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectFalse(parser.HasNonTrivialRenames())
	test.S(t).ExpectEquals(len(parser.columnRenameMap), 2)
	test.S(t).ExpectEquals(parser.columnRenameMap["ts"], "ts")
	test.S(t).ExpectEquals(parser.columnRenameMap["f"], "f")
}

func TestParseAlterStatementNonTrivial(t *testing.T) {
	statements := []string{
		`add column b bigint, change f fl float, change i count int, engine=innodb`,
		"add column b bigint, change column `f` fl float, change `i` `count` int, engine=innodb",
		"add column b bigint, change column `f` fl float, change `i` `count` int, change ts ts timestamp, engine=innodb",
		`change
		  f fl float,
			CHANGE COLUMN i
			  count int, engine=innodb`,
	}

	for _, statement := range statements {
		parser := NewParser()
		err := parser.ParseAlterStatement(statement)
		test.S(t).ExpectNil(err)
		renames := parser.GetNonTrivialRenames()
		test.S(t).ExpectEquals(len(renames), 2)
		test.S(t).ExpectEquals(renames["i"], "count")
		test.S(t).ExpectEquals(renames["f"], "fl")
	}
}
