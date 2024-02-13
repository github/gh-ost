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

func TestParseStatement(t *testing.T) {
	statement := "add column t int, engine=innodb"
	parser := NewAlterTableParser(statement)
	err := parser.ParseStatement()
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(parser.GetOptions(), statement)
	test.S(t).ExpectFalse(parser.HasNonTrivialRenames())
	test.S(t).ExpectFalse(parser.IsAutoIncrementDefined())
}

func TestParseStatementTrivialRename(t *testing.T) {
	statement := "add column t int, change ts ts timestamp, engine=innodb"
	parser := NewAlterTableParser(statement)
	err := parser.ParseStatement()
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(parser.GetOptions(), statement)
	test.S(t).ExpectFalse(parser.HasNonTrivialRenames())
	test.S(t).ExpectFalse(parser.IsAutoIncrementDefined())
	p := parser.(*AlterTableParser)
	test.S(t).ExpectEquals(len(p.columnRenameMap), 1)
	test.S(t).ExpectEquals(p.columnRenameMap["ts"], "ts")
}

func TestParseStatementWithAutoIncrement(t *testing.T) {
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
		parser := NewAlterTableParser(statement)
		err := parser.ParseStatement()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(parser.GetOptions(), statement)
		test.S(t).ExpectTrue(parser.IsAutoIncrementDefined())
	}
}

func TestParseStatementTrivialRenames(t *testing.T) {
	statement := "add column t int, change ts ts timestamp, CHANGE f `f` float, engine=innodb"
	parser := NewAlterTableParser(statement)
	err := parser.ParseStatement()
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(parser.GetOptions(), statement)
	test.S(t).ExpectFalse(parser.HasNonTrivialRenames())
	test.S(t).ExpectFalse(parser.IsAutoIncrementDefined())
	p := parser.(*AlterTableParser)
	test.S(t).ExpectEquals(len(p.columnRenameMap), 2)
	test.S(t).ExpectEquals(p.columnRenameMap["ts"], "ts")
	test.S(t).ExpectEquals(p.columnRenameMap["f"], "f")
}

func TestParseStatementNonTrivial(t *testing.T) {
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
		parser := NewAlterTableParser(statement)
		err := parser.ParseStatement()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectFalse(parser.IsAutoIncrementDefined())
		test.S(t).ExpectEquals(parser.GetOptions(), statement)
		renames := parser.GetNonTrivialRenames()
		test.S(t).ExpectEquals(len(renames), 2)
		test.S(t).ExpectEquals(renames["i"], "count")
		test.S(t).ExpectEquals(renames["f"], "fl")
	}
}

func TestTokenizeAlterStatement(t *testing.T) {
	{
		alterStatement := "add column t int"
		tokens := tokenizeStatement(alterStatement)
		test.S(t).ExpectTrue(reflect.DeepEqual(tokens, []string{"add column t int"}))
	}
	{
		alterStatement := "add column t int, change column i int"
		tokens := tokenizeStatement(alterStatement)
		test.S(t).ExpectTrue(reflect.DeepEqual(tokens, []string{"add column t int", "change column i int"}))
	}
	{
		alterStatement := "add column t int, change column i int 'some comment'"
		tokens := tokenizeStatement(alterStatement)
		test.S(t).ExpectTrue(reflect.DeepEqual(tokens, []string{"add column t int", "change column i int 'some comment'"}))
	}
	{
		alterStatement := "add column t int, change column i int 'some comment, with comma'"
		tokens := tokenizeStatement(alterStatement)
		test.S(t).ExpectTrue(reflect.DeepEqual(tokens, []string{"add column t int", "change column i int 'some comment, with comma'"}))
	}
	{
		alterStatement := "add column t int, add column d decimal(10,2)"
		tokens := tokenizeStatement(alterStatement)
		test.S(t).ExpectTrue(reflect.DeepEqual(tokens, []string{"add column t int", "add column d decimal(10,2)"}))
	}
	{
		alterStatement := "add column t int, add column e enum('a','b','c')"
		tokens := tokenizeStatement(alterStatement)
		test.S(t).ExpectTrue(reflect.DeepEqual(tokens, []string{"add column t int", "add column e enum('a','b','c')"}))
	}
	{
		alterStatement := "add column t int(11), add column e enum('a','b','c')"
		tokens := tokenizeStatement(alterStatement)
		test.S(t).ExpectTrue(reflect.DeepEqual(tokens, []string{"add column t int(11)", "add column e enum('a','b','c')"}))
	}
}

func TestSanitizeQuotesFromAlterStatement(t *testing.T) {
	{
		alterStatement := "add column e enum('a','b','c')"
		strippedStatement := sanitizeQuotesFromToken(alterStatement)
		test.S(t).ExpectEquals(strippedStatement, "add column e enum('','','')")
	}
	{
		alterStatement := "change column i int 'some comment, with comma'"
		strippedStatement := sanitizeQuotesFromToken(alterStatement)
		test.S(t).ExpectEquals(strippedStatement, "change column i int ''")
	}
}

func TestParseStatementDroppedColumns(t *testing.T) {
	{
		statement := "drop column b"
		parser := NewAlterTableParser(statement)
		err := parser.ParseStatement()
		test.S(t).ExpectNil(err)
		p := parser.(*AlterTableParser)
		test.S(t).ExpectEquals(len(p.droppedColumns), 1)
		test.S(t).ExpectTrue(p.droppedColumns["b"])
	}
	{
		statement := "drop column b, drop key c_idx, drop column `d`"
		parser := NewAlterTableParser(statement)
		err := parser.ParseStatement()
		test.S(t).ExpectNil(err)
		p := parser.(*AlterTableParser)
		test.S(t).ExpectEquals(parser.GetOptions(), statement)
		test.S(t).ExpectEquals(len(p.droppedColumns), 2)
		test.S(t).ExpectTrue(p.droppedColumns["b"])
		test.S(t).ExpectTrue(p.droppedColumns["d"])
	}
	{
		statement := "drop column b, drop key c_idx, drop column `d`, drop `e`, drop primary key, drop foreign key fk_1"
		parser := NewAlterTableParser(statement)
		err := parser.ParseStatement()
		test.S(t).ExpectNil(err)
		p := parser.(*AlterTableParser)
		test.S(t).ExpectEquals(len(p.droppedColumns), 3)
		test.S(t).ExpectTrue(p.droppedColumns["b"])
		test.S(t).ExpectTrue(p.droppedColumns["d"])
		test.S(t).ExpectTrue(p.droppedColumns["e"])
	}
	{
		statement := "drop column b, drop bad statement, add column i int"
		parser := NewAlterTableParser(statement)
		err := parser.ParseStatement()
		test.S(t).ExpectNil(err)
		p := parser.(*AlterTableParser)
		test.S(t).ExpectEquals(len(p.droppedColumns), 1)
		test.S(t).ExpectTrue(p.droppedColumns["b"])
	}
}

func TestParseStatementRenameTable(t *testing.T) {
	{
		statement := "drop column b"
		parser := NewAlterTableParser(statement)
		err := parser.ParseStatement()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectFalse(parser.IsRenameTable())
	}
	{
		statement := "rename as something_else"
		parser := NewAlterTableParser(statement)
		err := parser.ParseStatement()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectTrue(parser.IsRenameTable())
	}
	{
		statement := "drop column b, rename as something_else"
		parser := NewAlterTableParser(statement)
		err := parser.ParseStatement()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(parser.GetOptions(), statement)
		test.S(t).ExpectTrue(parser.IsRenameTable())
	}
	{
		statement := "engine=innodb rename as something_else"
		parser := NewAlterTableParser(statement)
		err := parser.ParseStatement()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectTrue(parser.IsRenameTable())
	}
	{
		statement := "rename as something_else, engine=innodb"
		parser := NewAlterTableParser(statement)
		err := parser.ParseStatement()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectTrue(parser.IsRenameTable())
	}
}

func TestParseStatementExplicitTable(t *testing.T) {
	{
		statement := "drop column b"
		parser := NewAlterTableParser(statement)
		err := parser.ParseStatement()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(parser.GetExplicitSchema(), "")
		test.S(t).ExpectEquals(parser.GetExplicitTable(), "")
		test.S(t).ExpectEquals(parser.GetOptions(), "drop column b")
		p := parser.(*AlterTableParser)
		test.S(t).ExpectTrue(reflect.DeepEqual(p.alterTokens, []string{"drop column b"}))
	}
	{
		statement := "alter table tbl drop column b"
		parser := NewAlterTableParser(statement)
		err := parser.ParseStatement()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(parser.GetExplicitSchema(), "")
		test.S(t).ExpectEquals(parser.GetExplicitTable(), "tbl")
		test.S(t).ExpectEquals(parser.GetOptions(), "drop column b")
		p := parser.(*AlterTableParser)
		test.S(t).ExpectTrue(reflect.DeepEqual(p.alterTokens, []string{"drop column b"}))
	}
	{
		statement := "alter table `tbl` drop column b"
		parser := NewAlterTableParser(statement)
		err := parser.ParseStatement()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(parser.GetExplicitSchema(), "")
		test.S(t).ExpectEquals(parser.GetExplicitTable(), "tbl")
		test.S(t).ExpectEquals(parser.GetOptions(), "drop column b")
		p := parser.(*AlterTableParser)
		test.S(t).ExpectTrue(reflect.DeepEqual(p.alterTokens, []string{"drop column b"}))
	}
	{
		statement := "alter table `scm with spaces`.`tbl` drop column b"
		parser := NewAlterTableParser(statement)
		err := parser.ParseStatement()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(parser.GetExplicitSchema(), "scm with spaces")
		test.S(t).ExpectEquals(parser.GetExplicitTable(), "tbl")
		test.S(t).ExpectEquals(parser.GetOptions(), "drop column b")
		p := parser.(*AlterTableParser)
		test.S(t).ExpectTrue(reflect.DeepEqual(p.alterTokens, []string{"drop column b"}))
	}
	{
		statement := "alter table `scm`.`tbl with spaces` drop column b"
		parser := NewAlterTableParser(statement)
		err := parser.ParseStatement()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(parser.GetExplicitSchema(), "scm")
		test.S(t).ExpectEquals(parser.GetExplicitTable(), "tbl with spaces")
		test.S(t).ExpectEquals(parser.GetOptions(), "drop column b")
		p := parser.(*AlterTableParser)
		test.S(t).ExpectTrue(reflect.DeepEqual(p.alterTokens, []string{"drop column b"}))
	}
	{
		statement := "alter table `scm`.tbl drop column b"
		parser := NewAlterTableParser(statement)
		err := parser.ParseStatement()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(parser.GetExplicitSchema(), "scm")
		test.S(t).ExpectEquals(parser.GetExplicitTable(), "tbl")
		test.S(t).ExpectEquals(parser.GetOptions(), "drop column b")
		p := parser.(*AlterTableParser)
		test.S(t).ExpectTrue(reflect.DeepEqual(p.alterTokens, []string{"drop column b"}))
	}
	{
		statement := "alter table scm.`tbl` drop column b"
		parser := NewAlterTableParser(statement)
		err := parser.ParseStatement()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(parser.GetExplicitSchema(), "scm")
		test.S(t).ExpectEquals(parser.GetExplicitTable(), "tbl")
		test.S(t).ExpectEquals(parser.GetOptions(), "drop column b")
		p := parser.(*AlterTableParser)
		test.S(t).ExpectTrue(reflect.DeepEqual(p.alterTokens, []string{"drop column b"}))
	}
	{
		statement := "alter table scm.tbl drop column b"
		parser := NewAlterTableParser(statement)
		err := parser.ParseStatement()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(parser.GetExplicitSchema(), "scm")
		test.S(t).ExpectEquals(parser.GetExplicitTable(), "tbl")
		test.S(t).ExpectEquals(parser.GetOptions(), "drop column b")
		p := parser.(*AlterTableParser)
		test.S(t).ExpectTrue(reflect.DeepEqual(p.alterTokens, []string{"drop column b"}))
	}
	{
		statement := "alter table scm.tbl drop column b, add index idx(i)"
		parser := NewAlterTableParser(statement)
		err := parser.ParseStatement()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(parser.GetExplicitSchema(), "scm")
		test.S(t).ExpectEquals(parser.GetExplicitTable(), "tbl")
		test.S(t).ExpectEquals(parser.GetOptions(), "drop column b, add index idx(i)")
		p := parser.(*AlterTableParser)
		test.S(t).ExpectTrue(reflect.DeepEqual(p.alterTokens, []string{"drop column b", "add index idx(i)"}))
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
