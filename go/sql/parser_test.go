/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package sql

import (
	"testing"

	"github.com/openark/golib/log"
	"github.com/stretchr/testify/require"
)

func init() {
	log.SetLevel(log.ERROR)
}

func TestParseAlterStatement(t *testing.T) {
	statement := "add column t int, engine=innodb"
	parser := NewAlterTableParser()
	err := parser.ParseAlterStatement(statement)
	require.NoError(t, err)
	require.Equal(t, statement, parser.alterStatementOptions)
	require.False(t, parser.HasNonTrivialRenames())
	require.False(t, parser.IsAutoIncrementDefined())
}

func TestParseAlterStatementrivialRename(t *testing.T) {
	statement := "add column t int, change ts ts timestamp, engine=innodb"
	parser := NewAlterTableParser()
	err := parser.ParseAlterStatement(statement)
	require.NoError(t, err)
	require.Equal(t, statement, parser.alterStatementOptions)
	require.False(t, parser.HasNonTrivialRenames())
	require.False(t, parser.IsAutoIncrementDefined())
	require.Len(t, parser.columnRenameMap, 1)
	require.Equal(t, "ts", parser.columnRenameMap["ts"])
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
		require.NoError(t, err)
		require.Equal(t, statement, parser.alterStatementOptions)
		require.True(t, parser.IsAutoIncrementDefined())
	}
}

func TestParseAlterStatementrivialRenames(t *testing.T) {
	statement := "add column t int, change ts ts timestamp, CHANGE f `f` float, engine=innodb"
	parser := NewAlterTableParser()
	err := parser.ParseAlterStatement(statement)
	require.NoError(t, err)
	require.Equal(t, statement, parser.alterStatementOptions)
	require.False(t, parser.HasNonTrivialRenames())
	require.False(t, parser.IsAutoIncrementDefined())
	require.Len(t, parser.columnRenameMap, 2)
	require.Equal(t, "ts", parser.columnRenameMap["ts"])
	require.Equal(t, "f", parser.columnRenameMap["f"])
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
		require.NoError(t, err)
		require.False(t, parser.IsAutoIncrementDefined())
		require.Equal(t, statement, parser.alterStatementOptions)
		renames := parser.GetNonTrivialRenames()
		require.Len(t, renames, 2)
		require.Equal(t, "count", renames["i"])
		require.Equal(t, "fl", renames["f"])
	}
}

func TestTokenizeAlterStatement(t *testing.T) {
	parser := NewAlterTableParser()
	{
		alterStatement := "add column t int"
		tokens := parser.tokenizeAlterStatement(alterStatement)
		require.Equal(t, []string{"add column t int"}, tokens)
	}
	{
		alterStatement := "add column t int, change column i int"
		tokens := parser.tokenizeAlterStatement(alterStatement)
		require.Equal(t, []string{"add column t int", "change column i int"}, tokens)
	}
	{
		alterStatement := "add column t int, change column i int 'some comment'"
		tokens := parser.tokenizeAlterStatement(alterStatement)
		require.Equal(t, []string{"add column t int", "change column i int 'some comment'"}, tokens)
	}
	{
		alterStatement := "add column t int, change column i int 'some comment, with comma'"
		tokens := parser.tokenizeAlterStatement(alterStatement)
		require.Equal(t, []string{"add column t int", "change column i int 'some comment, with comma'"}, tokens)
	}
	{
		alterStatement := "add column t int, add column d decimal(10,2)"
		tokens := parser.tokenizeAlterStatement(alterStatement)
		require.Equal(t, []string{"add column t int", "add column d decimal(10,2)"}, tokens)
	}
	{
		alterStatement := "add column t int, add column e enum('a','b','c')"
		tokens := parser.tokenizeAlterStatement(alterStatement)
		require.Equal(t, []string{"add column t int", "add column e enum('a','b','c')"}, tokens)
	}
	{
		alterStatement := "add column t int(11), add column e enum('a','b','c')"
		tokens := parser.tokenizeAlterStatement(alterStatement)
		require.Equal(t, []string{"add column t int(11)", "add column e enum('a','b','c')"}, tokens)
	}
}

func TestSanitizeQuotesFromAlterStatement(t *testing.T) {
	parser := NewAlterTableParser()
	{
		alterStatement := "add column e enum('a','b','c')"
		strippedStatement := parser.sanitizeQuotesFromAlterStatement(alterStatement)
		require.Equal(t, "add column e enum('','','')", strippedStatement)
	}
	{
		alterStatement := "change column i int 'some comment, with comma'"
		strippedStatement := parser.sanitizeQuotesFromAlterStatement(alterStatement)
		require.Equal(t, "change column i int ''", strippedStatement)
	}
}

func TestParseAlterStatementDroppedColumns(t *testing.T) {
	{
		parser := NewAlterTableParser()
		statement := "drop column b"
		err := parser.ParseAlterStatement(statement)
		require.NoError(t, err)
		require.Len(t, parser.droppedColumns, 1)
		require.True(t, parser.droppedColumns["b"])
	}
	{
		parser := NewAlterTableParser()
		statement := "drop column b, drop key c_idx, drop column `d`"
		err := parser.ParseAlterStatement(statement)
		require.NoError(t, err)
		require.Equal(t, statement, parser.alterStatementOptions)
		require.Len(t, parser.droppedColumns, 2)
		require.True(t, parser.droppedColumns["b"])
		require.True(t, parser.droppedColumns["d"])
	}
	{
		parser := NewAlterTableParser()
		statement := "drop column b, drop key c_idx, drop column `d`, drop `e`, drop primary key, drop foreign key fk_1"
		err := parser.ParseAlterStatement(statement)
		require.NoError(t, err)
		require.Len(t, parser.droppedColumns, 3)
		require.True(t, parser.droppedColumns["b"])
		require.True(t, parser.droppedColumns["d"])
		require.True(t, parser.droppedColumns["e"])
	}
	{
		parser := NewAlterTableParser()
		statement := "drop column b, drop bad statement, add column i int"
		err := parser.ParseAlterStatement(statement)
		require.NoError(t, err)
		require.Len(t, parser.droppedColumns, 1)
		require.True(t, parser.droppedColumns["b"])
	}
}

func TestParseAlterStatementRenameTable(t *testing.T) {
	{
		parser := NewAlterTableParser()
		statement := "drop column b"
		err := parser.ParseAlterStatement(statement)
		require.NoError(t, err)
		require.False(t, parser.isRenameTable)
	}
	{
		parser := NewAlterTableParser()
		statement := "rename as something_else"
		err := parser.ParseAlterStatement(statement)
		require.NoError(t, err)
		require.True(t, parser.isRenameTable)
	}
	{
		parser := NewAlterTableParser()
		statement := "drop column b, rename as something_else"
		err := parser.ParseAlterStatement(statement)
		require.NoError(t, err)
		require.Equal(t, statement, parser.alterStatementOptions)
		require.True(t, parser.isRenameTable)
	}
	{
		parser := NewAlterTableParser()
		statement := "engine=innodb rename as something_else"
		err := parser.ParseAlterStatement(statement)
		require.NoError(t, err)
		require.True(t, parser.isRenameTable)
	}
	{
		parser := NewAlterTableParser()
		statement := "rename as something_else, engine=innodb"
		err := parser.ParseAlterStatement(statement)
		require.NoError(t, err)
		require.True(t, parser.isRenameTable)
	}
}

func TestParseAlterStatementExplicitTable(t *testing.T) {
	{
		parser := NewAlterTableParser()
		statement := "drop column b"
		err := parser.ParseAlterStatement(statement)
		require.NoError(t, err)
		require.Equal(t, "", parser.explicitSchema)
		require.Equal(t, "", parser.explicitTable)
		require.Equal(t, "drop column b", parser.alterStatementOptions)
		require.Equal(t, []string{"drop column b"}, parser.alterTokens)
	}
	{
		parser := NewAlterTableParser()
		statement := "alter table tbl drop column b"
		err := parser.ParseAlterStatement(statement)
		require.NoError(t, err)
		require.Equal(t, "", parser.explicitSchema)
		require.Equal(t, "tbl", parser.explicitTable)
		require.Equal(t, "drop column b", parser.alterStatementOptions)
		require.Equal(t, []string{"drop column b"}, parser.alterTokens)
	}
	{
		parser := NewAlterTableParser()
		statement := "alter table `tbl` drop column b"
		err := parser.ParseAlterStatement(statement)
		require.NoError(t, err)
		require.Equal(t, parser.explicitSchema, "")
		require.Equal(t, parser.explicitTable, "tbl")
		require.Equal(t, parser.alterStatementOptions, "drop column b")
		require.Equal(t, parser.alterTokens, []string{"drop column b"})
	}
	{
		parser := NewAlterTableParser()
		statement := "alter table `scm with spaces`.`tbl` drop column b"
		err := parser.ParseAlterStatement(statement)
		require.NoError(t, err)
		require.Equal(t, parser.explicitSchema, "scm with spaces")
		require.Equal(t, parser.explicitTable, "tbl")
		require.Equal(t, parser.alterStatementOptions, "drop column b")
		require.Equal(t, parser.alterTokens, []string{"drop column b"})
	}
	{
		parser := NewAlterTableParser()
		statement := "alter table `scm`.`tbl with spaces` drop column b"
		err := parser.ParseAlterStatement(statement)
		require.NoError(t, err)
		require.Equal(t, parser.explicitSchema, "scm")
		require.Equal(t, parser.explicitTable, "tbl with spaces")
		require.Equal(t, parser.alterStatementOptions, "drop column b")
		require.Equal(t, parser.alterTokens, []string{"drop column b"})
	}
	{
		parser := NewAlterTableParser()
		statement := "alter table `scm`.tbl drop column b"
		err := parser.ParseAlterStatement(statement)
		require.NoError(t, err)
		require.Equal(t, parser.explicitSchema, "scm")
		require.Equal(t, parser.explicitTable, "tbl")
		require.Equal(t, parser.alterStatementOptions, "drop column b")
		require.Equal(t, parser.alterTokens, []string{"drop column b"})
	}
	{
		parser := NewAlterTableParser()
		statement := "alter table scm.`tbl` drop column b"
		err := parser.ParseAlterStatement(statement)
		require.NoError(t, err)
		require.Equal(t, parser.explicitSchema, "scm")
		require.Equal(t, parser.explicitTable, "tbl")
		require.Equal(t, parser.alterStatementOptions, "drop column b")
		require.Equal(t, parser.alterTokens, []string{"drop column b"})
	}
	{
		parser := NewAlterTableParser()
		statement := "alter table scm.tbl drop column b"
		err := parser.ParseAlterStatement(statement)
		require.NoError(t, err)
		require.Equal(t, parser.explicitSchema, "scm")
		require.Equal(t, parser.explicitTable, "tbl")
		require.Equal(t, parser.alterStatementOptions, "drop column b")
		require.Equal(t, parser.alterTokens, []string{"drop column b"})
	}
	{
		parser := NewAlterTableParser()
		statement := "alter table scm.tbl drop column b, add index idx(i)"
		err := parser.ParseAlterStatement(statement)
		require.NoError(t, err)
		require.Equal(t, parser.explicitSchema, "scm")
		require.Equal(t, parser.explicitTable, "tbl")
		require.Equal(t, parser.alterStatementOptions, "drop column b, add index idx(i)")
		require.Equal(t, parser.alterTokens, []string{"drop column b", "add index idx(i)"})
	}
}

func TestParseEnumValues(t *testing.T) {
	{
		s := "enum('red','green','blue','orange')"
		values := ParseEnumValues(s)
		require.Equal(t, values, "'red','green','blue','orange'")
	}
	{
		s := "('red','green','blue','orange')"
		values := ParseEnumValues(s)
		require.Equal(t, values, "('red','green','blue','orange')")
	}
	{
		s := "zzz"
		values := ParseEnumValues(s)
		require.Equal(t, values, "zzz")
	}
}
