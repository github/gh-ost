/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package sql

import (
	"reflect"
	"testing"

	"github.com/openark/golib/log"
	test "github.com/openark/golib/tests"
)

func init() {
	log.SetLevel(log.ERROR)
}

func TestParseAlterStatement(t *testing.T) {
	statement := "add column t int, engine=innodb"
	parser := NewAlterTableParser()
	err := parser.ParseAlterStatement(statement)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(parser.alterStatementOptions, statement)
	test.S(t).ExpectFalse(parser.HasNonTrivialRenames())
	test.S(t).ExpectFalse(parser.IsAutoIncrementDefined())
}

func TestParseAlterStatementTrivialRename(t *testing.T) {
	statement := "add column t int, change ts ts timestamp, engine=innodb"
	parser := NewAlterTableParser()
	err := parser.ParseAlterStatement(statement)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(parser.alterStatementOptions, statement)
	test.S(t).ExpectFalse(parser.HasNonTrivialRenames())
	test.S(t).ExpectFalse(parser.IsAutoIncrementDefined())
	test.S(t).ExpectEquals(len(parser.columnRenameMap), 1)
	test.S(t).ExpectEquals(parser.columnRenameMap["ts"], "ts")
}

func TestParseAlterStatementWithAutoIncrement(t *testing.T) {
	statements := []string{
		"auto_increment=7",
		"auto_increment = 7",
		"AUTO_INCREMENT = 71",
		"add column t int, change ts ts timestamp, auto_increment=7 engine=innodb",
		"add column t int, change ts ts timestamp, auto_increment =7 engine=innodb",
		"add column t int, change ts ts timestamp, AUTO_INCREMENT = 7 engine=innodb",
		"add column t int, change ts ts timestamp, engine=innodb auto_increment=73425",
	}
	for _, statement := range statements {
		parser := NewAlterTableParser()
		err := parser.ParseAlterStatement(statement)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(parser.alterStatementOptions, statement)
		test.S(t).ExpectTrue(parser.IsAutoIncrementDefined())
	}
}

func TestParseAlterStatementTrivialRenames(t *testing.T) {
	statement := "add column t int, change ts ts timestamp, CHANGE f `f` float, engine=innodb"
	parser := NewAlterTableParser()
	err := parser.ParseAlterStatement(statement)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(parser.alterStatementOptions, statement)
	test.S(t).ExpectFalse(parser.HasNonTrivialRenames())
	test.S(t).ExpectFalse(parser.IsAutoIncrementDefined())
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
		parser := NewAlterTableParser()
		err := parser.ParseAlterStatement(statement)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectFalse(parser.IsAutoIncrementDefined())
		test.S(t).ExpectEquals(parser.alterStatementOptions, statement)
		renames := parser.GetNonTrivialRenames()
		test.S(t).ExpectEquals(len(renames), 2)
		test.S(t).ExpectEquals(renames["i"], "count")
		test.S(t).ExpectEquals(renames["f"], "fl")
	}
}

func TestTokenizeAlterStatement(t *testing.T) {
	parser := NewAlterTableParser()
	{
		alterStatement := "add column t int"
		tokens := parser.tokenizeAlterStatement(alterStatement)
		test.S(t).ExpectTrue(reflect.DeepEqual(tokens, []string{"add column t int"}))
	}
	{
		alterStatement := "add column t int, change column i int"
		tokens := parser.tokenizeAlterStatement(alterStatement)
		test.S(t).ExpectTrue(reflect.DeepEqual(tokens, []string{"add column t int", "change column i int"}))
	}
	{
		alterStatement := "add column t int, change column i int 'some comment'"
		tokens := parser.tokenizeAlterStatement(alterStatement)
		test.S(t).ExpectTrue(reflect.DeepEqual(tokens, []string{"add column t int", "change column i int 'some comment'"}))
	}
	{
		alterStatement := "add column t int, change column i int 'some comment, with comma'"
		tokens := parser.tokenizeAlterStatement(alterStatement)
		test.S(t).ExpectTrue(reflect.DeepEqual(tokens, []string{"add column t int", "change column i int 'some comment, with comma'"}))
	}
	{
		alterStatement := "add column t int, add column d decimal(10,2)"
		tokens := parser.tokenizeAlterStatement(alterStatement)
		test.S(t).ExpectTrue(reflect.DeepEqual(tokens, []string{"add column t int", "add column d decimal(10,2)"}))
	}
	{
		alterStatement := "add column t int, add column e enum('a','b','c')"
		tokens := parser.tokenizeAlterStatement(alterStatement)
		test.S(t).ExpectTrue(reflect.DeepEqual(tokens, []string{"add column t int", "add column e enum('a','b','c')"}))
	}
	{
		alterStatement := "add column t int(11), add column e enum('a','b','c')"
		tokens := parser.tokenizeAlterStatement(alterStatement)
		test.S(t).ExpectTrue(reflect.DeepEqual(tokens, []string{"add column t int(11)", "add column e enum('a','b','c')"}))
	}
}

func TestSanitizeQuotesFromAlterStatement(t *testing.T) {
	parser := NewAlterTableParser()
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
		parser := NewAlterTableParser()
		statement := "drop column b"
		err := parser.ParseAlterStatement(statement)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(len(parser.droppedColumns), 1)
		test.S(t).ExpectTrue(parser.droppedColumns["b"])
	}
	{
		parser := NewAlterTableParser()
		statement := "drop column b, drop key c_idx, drop column `d`"
		err := parser.ParseAlterStatement(statement)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(parser.alterStatementOptions, statement)
		test.S(t).ExpectEquals(len(parser.droppedColumns), 2)
		test.S(t).ExpectTrue(parser.droppedColumns["b"])
		test.S(t).ExpectTrue(parser.droppedColumns["d"])
	}
	{
		parser := NewAlterTableParser()
		statement := "drop column b, drop key c_idx, drop column `d`, drop `e`, drop primary key, drop foreign key fk_1"
		err := parser.ParseAlterStatement(statement)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(len(parser.droppedColumns), 3)
		test.S(t).ExpectTrue(parser.droppedColumns["b"])
		test.S(t).ExpectTrue(parser.droppedColumns["d"])
		test.S(t).ExpectTrue(parser.droppedColumns["e"])
	}
	{
		parser := NewAlterTableParser()
		statement := "drop column b, drop bad statement, add column i int"
		err := parser.ParseAlterStatement(statement)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(len(parser.droppedColumns), 1)
		test.S(t).ExpectTrue(parser.droppedColumns["b"])
	}
}

func TestParseAlterStatementRenameTable(t *testing.T) {
	{
		parser := NewAlterTableParser()
		statement := "drop column b"
		err := parser.ParseAlterStatement(statement)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectFalse(parser.isRenameTable)
	}
	{
		parser := NewAlterTableParser()
		statement := "rename as something_else"
		err := parser.ParseAlterStatement(statement)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectTrue(parser.isRenameTable)
	}
	{
		parser := NewAlterTableParser()
		statement := "drop column b, rename as something_else"
		err := parser.ParseAlterStatement(statement)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(parser.alterStatementOptions, statement)
		test.S(t).ExpectTrue(parser.isRenameTable)
	}
	{
		parser := NewAlterTableParser()
		statement := "engine=innodb rename as something_else"
		err := parser.ParseAlterStatement(statement)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectTrue(parser.isRenameTable)
	}
	{
		parser := NewAlterTableParser()
		statement := "rename as something_else, engine=innodb"
		err := parser.ParseAlterStatement(statement)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectTrue(parser.isRenameTable)
	}
}

func TestParseAlterStatementExplicitTable(t *testing.T) {
	{
		parser := NewAlterTableParser()
		statement := "drop column b"
		err := parser.ParseAlterStatement(statement)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(parser.explicitSchema, "")
		test.S(t).ExpectEquals(parser.explicitTable, "")
		test.S(t).ExpectEquals(parser.alterStatementOptions, "drop column b")
		test.S(t).ExpectTrue(reflect.DeepEqual(parser.alterTokens, []string{"drop column b"}))
	}
	{
		parser := NewAlterTableParser()
		statement := "alter table tbl drop column b"
		err := parser.ParseAlterStatement(statement)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(parser.explicitSchema, "")
		test.S(t).ExpectEquals(parser.explicitTable, "tbl")
		test.S(t).ExpectEquals(parser.alterStatementOptions, "drop column b")
		test.S(t).ExpectTrue(reflect.DeepEqual(parser.alterTokens, []string{"drop column b"}))
	}
	{
		parser := NewAlterTableParser()
		statement := "alter table `tbl` drop column b"
		err := parser.ParseAlterStatement(statement)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(parser.explicitSchema, "")
		test.S(t).ExpectEquals(parser.explicitTable, "tbl")
		test.S(t).ExpectEquals(parser.alterStatementOptions, "drop column b")
		test.S(t).ExpectTrue(reflect.DeepEqual(parser.alterTokens, []string{"drop column b"}))
	}
	{
		parser := NewAlterTableParser()
		statement := "alter table `scm with spaces`.`tbl` drop column b"
		err := parser.ParseAlterStatement(statement)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(parser.explicitSchema, "scm with spaces")
		test.S(t).ExpectEquals(parser.explicitTable, "tbl")
		test.S(t).ExpectEquals(parser.alterStatementOptions, "drop column b")
		test.S(t).ExpectTrue(reflect.DeepEqual(parser.alterTokens, []string{"drop column b"}))
	}
	{
		parser := NewAlterTableParser()
		statement := "alter table `scm`.`tbl with spaces` drop column b"
		err := parser.ParseAlterStatement(statement)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(parser.explicitSchema, "scm")
		test.S(t).ExpectEquals(parser.explicitTable, "tbl with spaces")
		test.S(t).ExpectEquals(parser.alterStatementOptions, "drop column b")
		test.S(t).ExpectTrue(reflect.DeepEqual(parser.alterTokens, []string{"drop column b"}))
	}
	{
		parser := NewAlterTableParser()
		statement := "alter table `scm`.tbl drop column b"
		err := parser.ParseAlterStatement(statement)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(parser.explicitSchema, "scm")
		test.S(t).ExpectEquals(parser.explicitTable, "tbl")
		test.S(t).ExpectEquals(parser.alterStatementOptions, "drop column b")
		test.S(t).ExpectTrue(reflect.DeepEqual(parser.alterTokens, []string{"drop column b"}))
	}
	{
		parser := NewAlterTableParser()
		statement := "alter table scm.`tbl` drop column b"
		err := parser.ParseAlterStatement(statement)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(parser.explicitSchema, "scm")
		test.S(t).ExpectEquals(parser.explicitTable, "tbl")
		test.S(t).ExpectEquals(parser.alterStatementOptions, "drop column b")
		test.S(t).ExpectTrue(reflect.DeepEqual(parser.alterTokens, []string{"drop column b"}))
	}
	{
		parser := NewAlterTableParser()
		statement := "alter table scm.tbl drop column b"
		err := parser.ParseAlterStatement(statement)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(parser.explicitSchema, "scm")
		test.S(t).ExpectEquals(parser.explicitTable, "tbl")
		test.S(t).ExpectEquals(parser.alterStatementOptions, "drop column b")
		test.S(t).ExpectTrue(reflect.DeepEqual(parser.alterTokens, []string{"drop column b"}))
	}
	{
		parser := NewAlterTableParser()
		statement := "alter table scm.tbl drop column b, add index idx(i)"
		err := parser.ParseAlterStatement(statement)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(parser.explicitSchema, "scm")
		test.S(t).ExpectEquals(parser.explicitTable, "tbl")
		test.S(t).ExpectEquals(parser.alterStatementOptions, "drop column b, add index idx(i)")
		test.S(t).ExpectTrue(reflect.DeepEqual(parser.alterTokens, []string{"drop column b", "add index idx(i)"}))
	}
}

func TestParseEnumValues(t *testing.T) {
	{
		s := "enum('red','green','blue','orange')"
		values := ParseEnumValues(s)
		test.S(t).ExpectEquals(values, "'red','green','blue','orange'")
	}
	{
		s := "('red','green','blue','orange')"
		values := ParseEnumValues(s)
		test.S(t).ExpectEquals(values, "('red','green','blue','orange')")
	}
	{
		s := "zzz"
		values := ParseEnumValues(s)
		test.S(t).ExpectEquals(values, "zzz")
	}
}
