/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package sql

import (
	"reflect"
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

func TestTokenizeAlterStatement(t *testing.T) {
	parser := NewParser()
	{
		alterStatement := "add column t int"
		tokens, _ := parser.tokenizeAlterStatement(alterStatement)
		test.S(t).ExpectTrue(reflect.DeepEqual(tokens, []string{"add column t int"}))
	}
	{
		alterStatement := "add column t int, change column i int"
		tokens, _ := parser.tokenizeAlterStatement(alterStatement)
		test.S(t).ExpectTrue(reflect.DeepEqual(tokens, []string{"add column t int", "change column i int"}))
	}
	{
		alterStatement := "add column t int, change column i int 'some comment'"
		tokens, _ := parser.tokenizeAlterStatement(alterStatement)
		test.S(t).ExpectTrue(reflect.DeepEqual(tokens, []string{"add column t int", "change column i int 'some comment'"}))
	}
	{
		alterStatement := "add column t int, change column i int 'some comment, with comma'"
		tokens, _ := parser.tokenizeAlterStatement(alterStatement)
		test.S(t).ExpectTrue(reflect.DeepEqual(tokens, []string{"add column t int", "change column i int 'some comment, with comma'"}))
	}
	{
		alterStatement := "add column t int, add column d decimal(10,2)"
		tokens, _ := parser.tokenizeAlterStatement(alterStatement)
		test.S(t).ExpectTrue(reflect.DeepEqual(tokens, []string{"add column t int", "add column d decimal(10,2)"}))
	}
	{
		alterStatement := "add column t int, add column e enum('a','b','c')"
		tokens, _ := parser.tokenizeAlterStatement(alterStatement)
		test.S(t).ExpectTrue(reflect.DeepEqual(tokens, []string{"add column t int", "add column e enum('a','b','c')"}))
	}
	{
		alterStatement := "add column t int(11), add column e enum('a','b','c')"
		tokens, _ := parser.tokenizeAlterStatement(alterStatement)
		test.S(t).ExpectTrue(reflect.DeepEqual(tokens, []string{"add column t int(11)", "add column e enum('a','b','c')"}))
	}
}

func TestSanitizeQuotesFromAlterStatement(t *testing.T) {
	parser := NewParser()
	{
		alterStatement := "add column e enum('a','b','c')"
		strippedStatement := parser.sanitizeQuotesFromAlterStatement(alterStatement)
		test.S(t).ExpectEquals(strippedStatement, "add column e enum('','','')")
	}
	{
		alterStatement := "change column i int 'some comment, with comma'"
		strippedStatement := parser.sanitizeQuotesFromAlterStatement(alterStatement)
		test.S(t).ExpectEquals(strippedStatement, "change column i int ''")
	}
}

func TestParseAlterStatementDroppedColumns(t *testing.T) {

	{
		parser := NewParser()
		statement := "drop column b"
		err := parser.ParseAlterStatement(statement)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(len(parser.droppedColumns), 1)
		test.S(t).ExpectTrue(parser.droppedColumns["b"])
	}
	{
		parser := NewParser()
		statement := "drop column b, drop key c_idx, drop column `d`"
		err := parser.ParseAlterStatement(statement)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(len(parser.droppedColumns), 2)
		test.S(t).ExpectTrue(parser.droppedColumns["b"])
		test.S(t).ExpectTrue(parser.droppedColumns["d"])
	}
	{
		parser := NewParser()
		statement := "drop column b, drop key c_idx, drop column `d`, drop `e`, drop primary key, drop foreign key fk_1"
		err := parser.ParseAlterStatement(statement)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(len(parser.droppedColumns), 3)
		test.S(t).ExpectTrue(parser.droppedColumns["b"])
		test.S(t).ExpectTrue(parser.droppedColumns["d"])
		test.S(t).ExpectTrue(parser.droppedColumns["e"])
	}
	{
		parser := NewParser()
		statement := "drop column b, drop bad statement, add column i int"
		err := parser.ParseAlterStatement(statement)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(len(parser.droppedColumns), 1)
		test.S(t).ExpectTrue(parser.droppedColumns["b"])
	}
}

func TestParseAlterStatementRenameTable(t *testing.T) {

	{
		parser := NewParser()
		statement := "drop column b"
		err := parser.ParseAlterStatement(statement)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectFalse(parser.isRenameTable)
	}
	{
		parser := NewParser()
		statement := "rename as something_else"
		err := parser.ParseAlterStatement(statement)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectTrue(parser.isRenameTable)
	}
	{
		parser := NewParser()
		statement := "drop column b, rename as something_else"
		err := parser.ParseAlterStatement(statement)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectTrue(parser.isRenameTable)
	}
	{
		parser := NewParser()
		statement := "engine=innodb rename as something_else"
		err := parser.ParseAlterStatement(statement)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectTrue(parser.isRenameTable)
	}
	{
		parser := NewParser()
		statement := "rename as something_else, engine=innodb"
		err := parser.ParseAlterStatement(statement)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectTrue(parser.isRenameTable)
	}
}
