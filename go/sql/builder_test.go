/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package sql

import (
	"testing"

	"regexp"
	"strings"

	"github.com/openark/golib/log"
	"github.com/stretchr/testify/require"
)

var (
	spacesRegexp = regexp.MustCompile(`[ \t\n\r]+`)
)

func init() {
	log.SetLevel(log.ERROR)
}

func normalizeQuery(name string) string {
	name = strings.ReplaceAll(name, "`", "")
	name = spacesRegexp.ReplaceAllString(name, " ")
	name = strings.TrimSpace(name)
	return name
}

func TestEscapeName(t *testing.T) {
	names := []string{"my_table", `"my_table"`, "`my_table`"}
	for _, name := range names {
		escaped := EscapeName(name)
		require.Equal(t, "`my_table`", escaped)
	}
}

func TestBuildEqualsComparison(t *testing.T) {
	{
		columns := []string{"c1"}
		values := []string{"@v1"}
		comparison, err := BuildEqualsComparison(columns, values)
		require.NoError(t, err)
		require.Equal(t, "((`c1` = @v1))", comparison)
	}
	{
		columns := []string{"c1", "c2"}
		values := []string{"@v1", "@v2"}
		comparison, err := BuildEqualsComparison(columns, values)
		require.NoError(t, err)
		require.Equal(t, "((`c1` = @v1) and (`c2` = @v2))", comparison)
	}
	{
		columns := []string{"c1"}
		values := []string{"@v1", "@v2"}
		_, err := BuildEqualsComparison(columns, values)
		require.Error(t, err)
	}
	{
		columns := []string{}
		values := []string{}
		_, err := BuildEqualsComparison(columns, values)
		require.Error(t, err)
	}
}

func TestBuildEqualsPreparedComparison(t *testing.T) {
	{
		columns := []string{"c1", "c2"}
		comparison, err := BuildEqualsPreparedComparison(columns)
		require.NoError(t, err)
		require.Equal(t, "((`c1` = ?) and (`c2` = ?))", comparison)
	}
}

func TestBuildSetPreparedClause(t *testing.T) {
	{
		columns := NewColumnList([]string{"c1"})
		clause, err := BuildSetPreparedClause(columns)
		require.NoError(t, err)
		require.Equal(t, "`c1`=?", clause)
	}
	{
		columns := NewColumnList([]string{"c1", "c2"})
		clause, err := BuildSetPreparedClause(columns)
		require.NoError(t, err)
		require.Equal(t, "`c1`=?, `c2`=?", clause)
	}
	{
		columns := NewColumnList([]string{})
		_, err := BuildSetPreparedClause(columns)
		require.Error(t, err)
	}
}

func TestBuildRangeComparison(t *testing.T) {
	{
		columns := []string{"c1"}
		values := []string{"@v1"}
		args := []interface{}{3}
		comparison, explodedArgs, err := BuildRangeComparison(columns, values, args, LessThanComparisonSign)
		require.NoError(t, err)
		require.Equal(t, "((`c1` < @v1))", comparison)
		require.Equal(t, []interface{}{3}, explodedArgs)
	}
	{
		columns := []string{"c1"}
		values := []string{"@v1"}
		args := []interface{}{3}
		comparison, explodedArgs, err := BuildRangeComparison(columns, values, args, LessThanOrEqualsComparisonSign)
		require.NoError(t, err)
		require.Equal(t, "((`c1` < @v1) or ((`c1` = @v1)))", comparison)
		require.Equal(t, []interface{}{3, 3}, explodedArgs)
	}
	{
		columns := []string{"c1", "c2"}
		values := []string{"@v1", "@v2"}
		args := []interface{}{3, 17}
		comparison, explodedArgs, err := BuildRangeComparison(columns, values, args, LessThanComparisonSign)
		require.NoError(t, err)
		require.Equal(t, "((`c1` < @v1) or (((`c1` = @v1)) AND (`c2` < @v2)))", comparison)
		require.Equal(t, []interface{}{3, 3, 17}, explodedArgs)
	}
	{
		columns := []string{"c1", "c2"}
		values := []string{"@v1", "@v2"}
		args := []interface{}{3, 17}
		comparison, explodedArgs, err := BuildRangeComparison(columns, values, args, LessThanOrEqualsComparisonSign)
		require.NoError(t, err)
		require.Equal(t, "((`c1` < @v1) or (((`c1` = @v1)) AND (`c2` < @v2)) or ((`c1` = @v1) and (`c2` = @v2)))", comparison)
		require.Equal(t, []interface{}{3, 3, 17, 3, 17}, explodedArgs)
	}
	{
		columns := []string{"c1", "c2", "c3"}
		values := []string{"@v1", "@v2", "@v3"}
		args := []interface{}{3, 17, 22}
		comparison, explodedArgs, err := BuildRangeComparison(columns, values, args, LessThanOrEqualsComparisonSign)
		require.NoError(t, err)
		require.Equal(t, "((`c1` < @v1) or (((`c1` = @v1)) AND (`c2` < @v2)) or (((`c1` = @v1) and (`c2` = @v2)) AND (`c3` < @v3)) or ((`c1` = @v1) and (`c2` = @v2) and (`c3` = @v3)))", comparison)
		require.Equal(t, []interface{}{3, 3, 17, 3, 17, 22, 3, 17, 22}, explodedArgs)
	}
	{
		columns := []string{"c1"}
		values := []string{"@v1", "@v2"}
		args := []interface{}{3, 17}
		_, _, err := BuildRangeComparison(columns, values, args, LessThanOrEqualsComparisonSign)
		require.Error(t, err)
	}
	{
		columns := []string{}
		values := []string{}
		args := []interface{}{}
		_, _, err := BuildRangeComparison(columns, values, args, LessThanOrEqualsComparisonSign)
		require.Error(t, err)
	}
}

func TestBuildRangeInsertQuery(t *testing.T) {
	databaseName := "mydb"
	originalTableName := "tbl"
	ghostTableName := "ghost"
	sharedColumns := []string{"id", "name", "position"}
	{
		uniqueKey := "PRIMARY"
		uniqueKeyColumns := NewColumnList([]string{"id"})
		rangeStartValues := []string{"@v1s"}
		rangeEndValues := []string{"@v1e"}
		rangeStartArgs := []interface{}{3}
		rangeEndArgs := []interface{}{103}

		query, explodedArgs, err := BuildRangeInsertQuery(databaseName, originalTableName, ghostTableName, sharedColumns, sharedColumns, uniqueKey, uniqueKeyColumns, rangeStartValues, rangeEndValues, rangeStartArgs, rangeEndArgs, true, true, true)
		require.NoError(t, err)
		expected := `
			insert /* gh-ost mydb.tbl */ ignore
			into
				mydb.ghost
				(id, name, position)
			(
				select id, name, position
				from
					mydb.tbl
				force index (PRIMARY)
				where
					(((id > @v1s) or ((id = @v1s)))
					and ((id < @v1e) or ((id = @v1e))))
				for share nowait
			)`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
		require.Equal(t, []interface{}{3, 3, 103, 103}, explodedArgs)
	}
	{
		uniqueKey := "name_position_uidx"
		uniqueKeyColumns := NewColumnList([]string{"name", "position"})
		rangeStartValues := []string{"@v1s", "@v2s"}
		rangeEndValues := []string{"@v1e", "@v2e"}
		rangeStartArgs := []interface{}{3, 17}
		rangeEndArgs := []interface{}{103, 117}

		query, explodedArgs, err := BuildRangeInsertQuery(databaseName, originalTableName, ghostTableName, sharedColumns, sharedColumns, uniqueKey, uniqueKeyColumns, rangeStartValues, rangeEndValues, rangeStartArgs, rangeEndArgs, true, true, true)
		require.NoError(t, err)
		expected := `
			insert /* gh-ost mydb.tbl */ ignore
			into
				mydb.ghost
				(id, name, position)
			(
				select id, name, position
				from
					mydb.tbl
				force index (name_position_uidx)
				where
					(((name > @v1s) or (((name = @v1s))
					AND (position > @v2s))
					or ((name = @v1s)
					and (position = @v2s)))
					and ((name < @v1e)
					or (((name = @v1e))
					AND (position < @v2e))
					or ((name = @v1e) and (position = @v2e))))
				for share nowait
			)`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
		require.Equal(t, []interface{}{3, 3, 17, 3, 17, 103, 103, 117, 103, 117}, explodedArgs)
	}
}

func TestBuildRangeInsertQueryRenameMap(t *testing.T) {
	databaseName := "mydb"
	originalTableName := "tbl"
	ghostTableName := "ghost"
	sharedColumns := []string{"id", "name", "position"}
	mappedSharedColumns := []string{"id", "name", "location"}
	{
		uniqueKey := "PRIMARY"
		uniqueKeyColumns := NewColumnList([]string{"id"})
		rangeStartValues := []string{"@v1s"}
		rangeEndValues := []string{"@v1e"}
		rangeStartArgs := []interface{}{3}
		rangeEndArgs := []interface{}{103}

		query, explodedArgs, err := BuildRangeInsertQuery(databaseName, originalTableName, ghostTableName, sharedColumns, mappedSharedColumns, uniqueKey, uniqueKeyColumns, rangeStartValues, rangeEndValues, rangeStartArgs, rangeEndArgs, true, true, true)
		require.NoError(t, err)
		expected := `
			insert /* gh-ost mydb.tbl */ ignore
			into
				mydb.ghost
				(id, name, location)
			(
				select id, name, position
				from
					mydb.tbl
				force index (PRIMARY)
				where
					(((id > @v1s) or ((id = @v1s)))
					and
					((id < @v1e) or ((id = @v1e))))
				for share nowait
			)`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
		require.Equal(t, []interface{}{3, 3, 103, 103}, explodedArgs)
	}
	{
		uniqueKey := "name_position_uidx"
		uniqueKeyColumns := NewColumnList([]string{"name", "position"})
		rangeStartValues := []string{"@v1s", "@v2s"}
		rangeEndValues := []string{"@v1e", "@v2e"}
		rangeStartArgs := []interface{}{3, 17}
		rangeEndArgs := []interface{}{103, 117}

		query, explodedArgs, err := BuildRangeInsertQuery(databaseName, originalTableName, ghostTableName, sharedColumns, mappedSharedColumns, uniqueKey, uniqueKeyColumns, rangeStartValues, rangeEndValues, rangeStartArgs, rangeEndArgs, true, true, true)
		require.NoError(t, err)
		expected := `
			insert /* gh-ost mydb.tbl */ ignore
			into
				mydb.ghost
				(id, name, location)
			(
				select id, name, position
				from
					mydb.tbl
				force index (name_position_uidx)
				where
					(((name > @v1s) or (((name = @v1s))
					AND (position > @v2s)) or ((name = @v1s) and (position = @v2s)))
					and ((name < @v1e) or (((name = @v1e)) AND (position < @v2e))
					or ((name = @v1e) and (position = @v2e))))
				for share nowait
			)`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
		require.Equal(t, []interface{}{3, 3, 17, 3, 17, 103, 103, 117, 103, 117}, explodedArgs)
	}
}

func TestBuildRangeInsertPreparedQuery(t *testing.T) {
	databaseName := "mydb"
	originalTableName := "tbl"
	ghostTableName := "ghost"
	sharedColumns := []string{"id", "name", "position"}
	{
		uniqueKey := "name_position_uidx"
		uniqueKeyColumns := NewColumnList([]string{"name", "position"})
		rangeStartArgs := []interface{}{3, 17}
		rangeEndArgs := []interface{}{103, 117}

		query, explodedArgs, err := BuildRangeInsertPreparedQuery(databaseName, originalTableName, ghostTableName, sharedColumns, sharedColumns, uniqueKey, uniqueKeyColumns, rangeStartArgs, rangeEndArgs, true, true, true)
		require.NoError(t, err)
		expected := `
			insert /* gh-ost mydb.tbl */ ignore
			into
				mydb.ghost
				(id, name, position)
			(
				select id, name, position
				from
					mydb.tbl
				force index (name_position_uidx)
				where (((name > ?) or (((name = ?)) AND (position > ?)) or ((name = ?) and (position = ?))) and ((name < ?) or (((name = ?)) AND (position < ?)) or ((name = ?) and (position = ?))))
				for share nowait
			)`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
		require.Equal(t, []interface{}{3, 3, 17, 3, 17, 103, 103, 117, 103, 117}, explodedArgs)
	}
}

func TestBuildUniqueKeyRangeEndPreparedQueryViaOffset(t *testing.T) {
	databaseName := "mydb"
	originalTableName := "tbl"
	var chunkSize int64 = 500
	{
		uniqueKeyColumns := NewColumnList([]string{"name", "position"})
		rangeStartArgs := []interface{}{3, 17}
		rangeEndArgs := []interface{}{103, 117}

		query, explodedArgs, err := BuildUniqueKeyRangeEndPreparedQueryViaOffset(databaseName, originalTableName, uniqueKeyColumns, rangeStartArgs, rangeEndArgs, chunkSize, false, "test")
		require.NoError(t, err)
		expected := `
			select /* gh-ost mydb.tbl test */
				name, position
			from
				mydb.tbl
			where
				((name > ?) or (((name = ?)) AND (position > ?))) and ((name < ?) or (((name = ?)) AND (position < ?)) or ((name = ?) and (position = ?)))
			order by
				name asc, position asc
			limit 1
			offset 499`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
		require.Equal(t, []interface{}{3, 3, 17, 103, 103, 117, 103, 117}, explodedArgs)
	}
}

func TestBuildUniqueKeyRangeEndPreparedQueryViaTemptable(t *testing.T) {
	databaseName := "mydb"
	originalTableName := "tbl"
	var chunkSize int64 = 500
	{
		uniqueKeyColumns := NewColumnList([]string{"name", "position"})
		rangeStartArgs := []interface{}{3, 17}
		rangeEndArgs := []interface{}{103, 117}

		query, explodedArgs, err := BuildUniqueKeyRangeEndPreparedQueryViaTemptable(databaseName, originalTableName, uniqueKeyColumns, rangeStartArgs, rangeEndArgs, chunkSize, false, "test")
		require.NoError(t, err)
		expected := `
			select /* gh-ost mydb.tbl test */
				name, position
			from (
				select
					name, position
				from
					mydb.tbl
				where ((name > ?) or (((name = ?)) AND (position > ?))) and ((name < ?) or (((name = ?)) AND (position < ?)) or ((name = ?) and (position = ?)))
				order by
					name asc, position asc
				limit 500
			) select_osc_chunk
			order by
				name desc, position desc
			limit 1`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
		require.Equal(t, []interface{}{3, 3, 17, 103, 103, 117, 103, 117}, explodedArgs)
	}
}

func TestBuildUniqueKeyMinValuesPreparedQuery(t *testing.T) {
	databaseName := "mydb"
	originalTableName := "tbl"
	uniqueKeyColumns := NewColumnList([]string{"name", "position"})
	uniqueKey := &UniqueKey{Name: "PRIMARY", Columns: *uniqueKeyColumns}
	{
		query, err := BuildUniqueKeyMinValuesPreparedQuery(databaseName, originalTableName, uniqueKey)
		require.NoError(t, err)
		expected := `
			select /* gh-ost mydb.tbl */ name, position
			  from
			    mydb.tbl
			  force index (PRIMARY)
			  order by
			    name asc, position asc
			  limit 1
		`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
	}
	{
		query, err := BuildUniqueKeyMaxValuesPreparedQuery(databaseName, originalTableName, uniqueKey)
		require.NoError(t, err)
		expected := `
			select /* gh-ost mydb.tbl */ name, position
			  from
			    mydb.tbl
			  force index (PRIMARY)
			  order by
			    name desc, position desc
			  limit 1
		`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
	}
}

func TestBuildDMLDeleteQuery(t *testing.T) {
	databaseName := "mydb"
	tableName := "tbl"
	tableColumns := NewColumnList([]string{"id", "name", "rank", "position", "age"})
	args := []interface{}{3, "testname", "first", 17, 23}
	{
		uniqueKeyColumns := NewColumnList([]string{"position"})
		builder, err := NewDMLDeleteQueryBuilder(databaseName, tableName, tableColumns, uniqueKeyColumns)
		require.NoError(t, err)

		query, uniqueKeyArgs, err := builder.BuildQuery(args)
		require.NoError(t, err)
		expected := `
			delete /* gh-ost mydb.tbl */
				from
					mydb.tbl
				where
					((position = ?))
		`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
		require.Equal(t, []interface{}{17}, uniqueKeyArgs)
	}
	{
		uniqueKeyColumns := NewColumnList([]string{"name", "position"})
		builder, err := NewDMLDeleteQueryBuilder(databaseName, tableName, tableColumns, uniqueKeyColumns)
		require.NoError(t, err)

		query, uniqueKeyArgs, err := builder.BuildQuery(args)
		require.NoError(t, err)
		expected := `
			delete /* gh-ost mydb.tbl */
				from
					mydb.tbl
				where
					((name = ?) and (position = ?))
		`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
		require.Equal(t, []interface{}{"testname", 17}, uniqueKeyArgs)
	}
	{
		uniqueKeyColumns := NewColumnList([]string{"position", "name"})
		builder, err := NewDMLDeleteQueryBuilder(databaseName, tableName, tableColumns, uniqueKeyColumns)
		require.NoError(t, err)

		query, uniqueKeyArgs, err := builder.BuildQuery(args)
		require.NoError(t, err)
		expected := `
			delete /* gh-ost mydb.tbl */
				from
					mydb.tbl
				where
					((position = ?) and (name = ?))
		`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
		require.Equal(t, []interface{}{17, "testname"}, uniqueKeyArgs)
	}
	{
		uniqueKeyColumns := NewColumnList([]string{"position", "name"})
		args := []interface{}{"first", 17}
		builder, err := NewDMLDeleteQueryBuilder(databaseName, tableName, tableColumns, uniqueKeyColumns)
		require.NoError(t, err)

		_, _, err = builder.BuildQuery(args)
		require.Error(t, err)
	}
}

func TestBuildDMLDeleteQuerySignedUnsigned(t *testing.T) {
	databaseName := "mydb"
	tableName := "tbl"
	tableColumns := NewColumnList([]string{"id", "name", "rank", "position", "age"})
	uniqueKeyColumns := NewColumnList([]string{"position"})
	builder, err := NewDMLDeleteQueryBuilder(databaseName, tableName, tableColumns, uniqueKeyColumns)
	require.NoError(t, err)
	{
		// test signed (expect no change)
		args := []interface{}{3, "testname", "first", -1, 23}
		query, uniqueKeyArgs, err := builder.BuildQuery(args)
		require.NoError(t, err)
		expected := `
			delete /* gh-ost mydb.tbl */
				from
					mydb.tbl
				where
					((position = ?))
		`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
		require.Equal(t, []interface{}{-1}, uniqueKeyArgs)
	}
	{
		// test unsigned
		args := []interface{}{3, "testname", "first", int8(-1), 23}
		uniqueKeyColumns.SetUnsigned("position")
		query, uniqueKeyArgs, err := builder.BuildQuery(args)
		require.NoError(t, err)
		expected := `
			delete /* gh-ost mydb.tbl */
				from
					mydb.tbl
				where
					((position = ?))
		`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
		require.Equal(t, []interface{}{uint8(255)}, uniqueKeyArgs)
	}
}

func TestBuildDMLInsertQuery(t *testing.T) {
	databaseName := "mydb"
	tableName := "tbl"
	tableColumns := NewColumnList([]string{"id", "name", "rank", "position", "age"})
	args := []interface{}{3, "testname", "first", 17, 23}
	{
		sharedColumns := NewColumnList([]string{"id", "name", "position", "age"})
		builder, err := NewDMLInsertQueryBuilder(databaseName, tableName, tableColumns, sharedColumns, sharedColumns)
		require.NoError(t, err)
		query, sharedArgs, err := builder.BuildQuery(args)
		require.NoError(t, err)
		expected := `
			insert /* gh-ost mydb.tbl */ ignore
				into mydb.tbl
					(id, name, position, age)
				values
					(?, ?, ?, ?)
		`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
		require.Equal(t, []interface{}{3, "testname", 17, 23}, sharedArgs)
	}
	{
		sharedColumns := NewColumnList([]string{"position", "name", "age", "id"})
		builder, err := NewDMLInsertQueryBuilder(databaseName, tableName, tableColumns, sharedColumns, sharedColumns)
		require.NoError(t, err)
		query, sharedArgs, err := builder.BuildQuery(args)
		require.NoError(t, err)
		expected := `
			insert /* gh-ost mydb.tbl */ ignore
				into mydb.tbl
					(position, name, age, id)
				values
					(?, ?, ?, ?)
		`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
		require.Equal(t, []interface{}{17, "testname", 23, 3}, sharedArgs)
	}
	{
		sharedColumns := NewColumnList([]string{"position", "name", "surprise", "id"})
		_, err := NewDMLInsertQueryBuilder(databaseName, tableName, tableColumns, sharedColumns, sharedColumns)
		require.Error(t, err)
	}
	{
		sharedColumns := NewColumnList([]string{})
		_, err := NewDMLInsertQueryBuilder(databaseName, tableName, tableColumns, sharedColumns, sharedColumns)
		require.Error(t, err)
	}
}

func TestBuildDMLInsertQuerySignedUnsigned(t *testing.T) {
	databaseName := "mydb"
	tableName := "tbl"
	tableColumns := NewColumnList([]string{"id", "name", "rank", "position", "age"})
	sharedColumns := NewColumnList([]string{"id", "name", "position", "age"})
	{
		// testing signed
		args := []interface{}{3, "testname", "first", int8(-1), 23}
		sharedColumns := NewColumnList([]string{"id", "name", "position", "age"})
		builder, err := NewDMLInsertQueryBuilder(databaseName, tableName, tableColumns, sharedColumns, sharedColumns)
		require.NoError(t, err)
		query, sharedArgs, err := builder.BuildQuery(args)
		require.NoError(t, err)
		expected := `
			insert /* gh-ost mydb.tbl */ ignore
				into mydb.tbl
					(id, name, position, age)
				values
					(?, ?, ?, ?)
		`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
		require.Equal(t, []interface{}{3, "testname", int8(-1), 23}, sharedArgs)
	}
	{
		// testing unsigned
		args := []interface{}{3, "testname", "first", int8(-1), 23}
		sharedColumns.SetUnsigned("position")
		builder, err := NewDMLInsertQueryBuilder(databaseName, tableName, tableColumns, sharedColumns, sharedColumns)
		require.NoError(t, err)
		query, sharedArgs, err := builder.BuildQuery(args)
		require.NoError(t, err)
		expected := `
			insert /* gh-ost mydb.tbl */ ignore
				into mydb.tbl
					(id, name, position, age)
				values
					(?, ?, ?, ?)
		`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
		require.Equal(t, []interface{}{3, "testname", uint8(255), 23}, sharedArgs)
	}
	{
		// testing unsigned
		args := []interface{}{3, "testname", "first", int32(-1), 23}
		sharedColumns.SetUnsigned("position")
		builder, err := NewDMLInsertQueryBuilder(databaseName, tableName, tableColumns, sharedColumns, sharedColumns)
		require.NoError(t, err)
		query, sharedArgs, err := builder.BuildQuery(args)
		require.NoError(t, err)
		expected := `
			insert /* gh-ost mydb.tbl */ ignore
				into mydb.tbl
					(id, name, position, age)
				values
					(?, ?, ?, ?)
		`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
		require.Equal(t, []interface{}{3, "testname", uint32(4294967295), 23}, sharedArgs)
	}
}

func TestBuildDMLUpdateQuery(t *testing.T) {
	databaseName := "mydb"
	tableName := "tbl"
	tableColumns := NewColumnList([]string{"id", "name", "rank", "position", "age"})
	valueArgs := []interface{}{3, "testname", "newval", 17, 23}
	whereArgs := []interface{}{3, "testname", "findme", 17, 56}
	{
		sharedColumns := NewColumnList([]string{"id", "name", "position", "age"})
		uniqueKeyColumns := NewColumnList([]string{"position"})
		builder, err := NewDMLUpdateQueryBuilder(databaseName, tableName, tableColumns, sharedColumns, sharedColumns, uniqueKeyColumns)
		require.NoError(t, err)
		query, updateArgs, err := builder.BuildQuery(valueArgs, whereArgs)
		require.NoError(t, err)
		expected := `
			update /* gh-ost mydb.tbl */
			  mydb.tbl
					set id=?, name=?, position=?, age=?
				where
					((position = ?))
		`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
		require.Equal(t, []interface{}{3, "testname", 17, 23, 17}, updateArgs)
	}
	{
		sharedColumns := NewColumnList([]string{"id", "name", "position", "age"})
		uniqueKeyColumns := NewColumnList([]string{"position", "name"})
		builder, err := NewDMLUpdateQueryBuilder(databaseName, tableName, tableColumns, sharedColumns, sharedColumns, uniqueKeyColumns)
		require.NoError(t, err)
		query, updateArgs, err := builder.BuildQuery(valueArgs, whereArgs)
		require.NoError(t, err)
		expected := `
			update /* gh-ost mydb.tbl */
			  mydb.tbl
					set id=?, name=?, position=?, age=?
				where
					((position = ?) and (name = ?))
		`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
		require.Equal(t, []interface{}{3, "testname", 17, 23, 17, "testname"}, updateArgs)
	}
	{
		sharedColumns := NewColumnList([]string{"id", "name", "position", "age"})
		uniqueKeyColumns := NewColumnList([]string{"age"})
		builder, err := NewDMLUpdateQueryBuilder(databaseName, tableName, tableColumns, sharedColumns, sharedColumns, uniqueKeyColumns)
		require.NoError(t, err)
		query, updateArgs, err := builder.BuildQuery(valueArgs, whereArgs)
		require.NoError(t, err)
		expected := `
			update /* gh-ost mydb.tbl */
			  mydb.tbl
					set id=?, name=?, position=?, age=?
				where
					((age = ?))
		`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
		require.Equal(t, []interface{}{3, "testname", 17, 23, 56}, updateArgs)
	}
	{
		sharedColumns := NewColumnList([]string{"id", "name", "position", "age"})
		uniqueKeyColumns := NewColumnList([]string{"age", "position", "id", "name"})
		builder, err := NewDMLUpdateQueryBuilder(databaseName, tableName, tableColumns, sharedColumns, sharedColumns, uniqueKeyColumns)
		require.NoError(t, err)
		query, updateArgs, err := builder.BuildQuery(valueArgs, whereArgs)
		require.NoError(t, err)
		expected := `
			update /* gh-ost mydb.tbl */
			  mydb.tbl
					set id=?, name=?, position=?, age=?
				where
					((age = ?) and (position = ?) and (id = ?) and (name = ?))
		`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
		require.Equal(t, []interface{}{3, "testname", 17, 23, 56, 17, 3, "testname"}, updateArgs)
	}
	{
		sharedColumns := NewColumnList([]string{"id", "name", "position", "age"})
		uniqueKeyColumns := NewColumnList([]string{"age", "surprise"})
		_, err := NewDMLUpdateQueryBuilder(databaseName, tableName, tableColumns, sharedColumns, sharedColumns, uniqueKeyColumns)
		require.Error(t, err)
	}
	{
		sharedColumns := NewColumnList([]string{"id", "name", "position", "age"})
		uniqueKeyColumns := NewColumnList([]string{})
		_, err := NewDMLUpdateQueryBuilder(databaseName, tableName, tableColumns, sharedColumns, sharedColumns, uniqueKeyColumns)
		require.Error(t, err)
	}
	{
		sharedColumns := NewColumnList([]string{"id", "name", "position", "age"})
		mappedColumns := NewColumnList([]string{"id", "name", "role", "age"})
		uniqueKeyColumns := NewColumnList([]string{"id"})
		builder, err := NewDMLUpdateQueryBuilder(databaseName, tableName, tableColumns, sharedColumns, mappedColumns, uniqueKeyColumns)
		require.NoError(t, err)
		query, updateArgs, err := builder.BuildQuery(valueArgs, whereArgs)
		require.NoError(t, err)
		expected := `
			update /* gh-ost mydb.tbl */
			  mydb.tbl
					set id=?, name=?, role=?, age=?
				where
					((id = ?))
		`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
		require.Equal(t, []interface{}{3, "testname", 17, 23, 3}, updateArgs)
	}
}

func TestBuildDMLUpdateQuerySignedUnsigned(t *testing.T) {
	databaseName := "mydb"
	tableName := "tbl"
	tableColumns := NewColumnList([]string{"id", "name", "rank", "position", "age"})
	valueArgs := []interface{}{3, "testname", "newval", int8(-17), int8(-2)}
	whereArgs := []interface{}{3, "testname", "findme", int8(-3), 56}
	sharedColumns := NewColumnList([]string{"id", "name", "position", "age"})
	uniqueKeyColumns := NewColumnList([]string{"position"})
	builder, err := NewDMLUpdateQueryBuilder(databaseName, tableName, tableColumns, sharedColumns, sharedColumns, uniqueKeyColumns)
	require.NoError(t, err)
	{
		// test signed
		query, updateArgs, err := builder.BuildQuery(valueArgs, whereArgs)
		require.NoError(t, err)
		expected := `
			update /* gh-ost mydb.tbl */
			  mydb.tbl
					set id=?, name=?, position=?, age=?
				where
					((position = ?))
		`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
		require.Equal(t, []interface{}{3, "testname", int8(-17), int8(-2), int8(-3)}, updateArgs)
	}
	{
		// test unsigned
		sharedColumns.SetUnsigned("age")
		uniqueKeyColumns.SetUnsigned("position")
		query, updateArgs, err := builder.BuildQuery(valueArgs, whereArgs)
		require.NoError(t, err)
		expected := `
			update /* gh-ost mydb.tbl */
			  mydb.tbl
					set id=?, name=?, position=?, age=?
				where
					((position = ?))
		`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
		require.Equal(t, []interface{}{3, "testname", int8(-17), uint8(254), uint8(253)}, updateArgs)
	}
}

func TestMoveTableCopySelectQueryBuilder(t *testing.T) {
	t.Run("single column unique key", func(t *testing.T) {
		sharedColumns := NewColumnList([]string{"id", "name", "position"})
		uniqueKeyColumns := NewColumnList([]string{"id"})

		builder, err := NewMoveTableCopySelectQueryBuilder("mydb", "tbl", sharedColumns, "PRIMARY", uniqueKeyColumns, true)
		require.NoError(t, err)

		query, args, err := builder.BuildQuery([]any{3}, []any{103})
		require.NoError(t, err)

		expected := `
			select /* gh-ost mydb.tbl */ id, name, position
			from
				mydb.tbl
			force index (PRIMARY)
			where
				(((id > ?) or ((id = ?))) and ((id < ?) or ((id = ?))))
		`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
		require.Equal(t, []any{3, 3, 103, 103}, args)
	})

	t.Run("single column unique key without range start", func(t *testing.T) {
		sharedColumns := NewColumnList([]string{"id", "name", "position"})
		uniqueKeyColumns := NewColumnList([]string{"id"})

		builder, err := NewMoveTableCopySelectQueryBuilder("mydb", "tbl", sharedColumns, "PRIMARY", uniqueKeyColumns, false)
		require.NoError(t, err)

		query, args, err := builder.BuildQuery([]any{3}, []any{103})
		require.NoError(t, err)

		expected := `
			select /* gh-ost mydb.tbl */ id, name, position
			from
				mydb.tbl
			force index (PRIMARY)
			where
				(((id > ?)) and ((id < ?) or ((id = ?))))
		`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
		require.Equal(t, []any{3, 103, 103}, args)
	})

	t.Run("compound unique key", func(t *testing.T) {
		sharedColumns := NewColumnList([]string{"id", "name", "position"})
		uniqueKeyColumns := NewColumnList([]string{"name", "position"})

		builder, err := NewMoveTableCopySelectQueryBuilder("mydb", "tbl", sharedColumns, "name_position_uidx", uniqueKeyColumns, true)
		require.NoError(t, err)

		query, args, err := builder.BuildQuery([]any{3, 17}, []any{103, 117})
		require.NoError(t, err)

		expected := `
			select /* gh-ost mydb.tbl */ id, name, position
			from
				mydb.tbl
			force index (name_position_uidx)
			where
				(((name > ?) or (((name = ?)) AND (position > ?)) or ((name = ?) and (position = ?)))
				and ((name < ?) or (((name = ?)) AND (position < ?)) or ((name = ?) and (position = ?))))
		`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
		require.Equal(t, []any{3, 3, 17, 3, 17, 103, 103, 117, 103, 117}, args)
	})

	t.Run("reuses prepared statement across calls", func(t *testing.T) {
		sharedColumns := NewColumnList([]string{"id", "name"})
		uniqueKeyColumns := NewColumnList([]string{"id"})

		builder, err := NewMoveTableCopySelectQueryBuilder("mydb", "tbl", sharedColumns, "PRIMARY", uniqueKeyColumns, true)
		require.NoError(t, err)

		query1, args1, err := builder.BuildQuery([]any{1}, []any{10})
		require.NoError(t, err)
		query2, args2, err := builder.BuildQuery([]any{11}, []any{20})
		require.NoError(t, err)

		require.Equal(t, query1, query2)
		require.Equal(t, []any{1, 1, 10, 10}, args1)
		require.Equal(t, []any{11, 11, 20, 20}, args2)
	})

	t.Run("wrong args count", func(t *testing.T) {
		sharedColumns := NewColumnList([]string{"id", "name"})
		uniqueKeyColumns := NewColumnList([]string{"id"})

		builder, err := NewMoveTableCopySelectQueryBuilder("mydb", "tbl", sharedColumns, "PRIMARY", uniqueKeyColumns, true)
		require.NoError(t, err)

		_, _, err = builder.BuildQuery([]any{1, 2}, []any{10})
		require.Error(t, err)
	})
}

func BenchmarkMoveTableCopySelectQueryBuilderBuildQuery(b *testing.B) {
	sharedColumns := NewColumnList([]string{"id", "name", "position"})
	uniqueKeyColumns := NewColumnList([]string{"name", "position"})

	builder, err := NewMoveTableCopySelectQueryBuilder("mydb", "tbl", sharedColumns, "name_position_uidx", uniqueKeyColumns, true)
	if err != nil {
		b.Fatal(err)
	}

	rangeStartArgs := []any{3, 17}
	rangeEndArgs := []any{103, 117}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := builder.BuildQuery(rangeStartArgs, rangeEndArgs)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestMoveTableCopyInsertQueryBuilder(t *testing.T) {
	t.Run("single row", func(t *testing.T) {
		sharedColumns := NewColumnList([]string{"id", "name", "position"})

		builder, err := NewMoveTableCopyInsertQueryBuilder("mydb", "ghost", sharedColumns)
		require.NoError(t, err)

		values := []*ColumnValues{
			ToColumnValues([]interface{}{1, "alice", 10}),
		}
		query, args, err := builder.BuildQuery(values)
		require.NoError(t, err)

		expected := `
			insert /* gh-ost mydb.ghost */ ignore
			into
				mydb.ghost
				(id, name, position)
			values
			(?, ?, ?)
		`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
		require.Equal(t, []any{1, "alice", 10}, args)
	})

	t.Run("multiple rows", func(t *testing.T) {
		sharedColumns := NewColumnList([]string{"id", "name", "position"})

		builder, err := NewMoveTableCopyInsertQueryBuilder("mydb", "ghost", sharedColumns)
		require.NoError(t, err)

		values := []*ColumnValues{
			ToColumnValues([]interface{}{1, "alice", 10}),
			ToColumnValues([]interface{}{2, "bob", 20}),
			ToColumnValues([]interface{}{3, "carol", 30}),
		}
		query, args, err := builder.BuildQuery(values)
		require.NoError(t, err)

		expected := `
			insert /* gh-ost mydb.ghost */ ignore
			into
				mydb.ghost
				(id, name, position)
			values
			(?, ?, ?),
			(?, ?, ?),
			(?, ?, ?)
		`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
		require.Equal(t, []any{1, "alice", 10, 2, "bob", 20, 3, "carol", 30}, args)
	})

	t.Run("wrong column count", func(t *testing.T) {
		sharedColumns := NewColumnList([]string{"id", "name", "position"})

		builder, err := NewMoveTableCopyInsertQueryBuilder("mydb", "ghost", sharedColumns)
		require.NoError(t, err)

		values := []*ColumnValues{
			ToColumnValues([]interface{}{1, "alice"}),
		}
		_, _, err = builder.BuildQuery(values)
		require.Error(t, err)
	})

	t.Run("reuses prepared statement", func(t *testing.T) {
		sharedColumns := NewColumnList([]string{"id", "name"})

		builder, err := NewMoveTableCopyInsertQueryBuilder("mydb", "ghost", sharedColumns)
		require.NoError(t, err)

		values1 := []*ColumnValues{ToColumnValues([]interface{}{1, "a"})}
		values2 := []*ColumnValues{ToColumnValues([]interface{}{2, "b"})}

		query1, args1, err := builder.BuildQuery(values1)
		require.NoError(t, err)
		query2, args2, err := builder.BuildQuery(values2)
		require.NoError(t, err)

		require.Equal(t, query1, query2)
		require.Equal(t, []any{1, "a"}, args1)
		require.Equal(t, []any{2, "b"}, args2)
	})
}

func BenchmarkMoveTableCopyInsertQueryBuilderBuildQuery(b *testing.B) {
	sharedColumns := NewColumnList([]string{"id", "name", "position"})

	builder, err := NewMoveTableCopyInsertQueryBuilder("mydb", "ghost", sharedColumns)
	if err != nil {
		b.Fatal(err)
	}

	values := []*ColumnValues{
		ToColumnValues([]interface{}{1, "alice", 10}),
		ToColumnValues([]interface{}{2, "bob", 20}),
		ToColumnValues([]interface{}{3, "carol", 30}),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := builder.BuildQuery(values)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestCheckpointQueryBuilder(t *testing.T) {
	databaseName := "mydb"
	tableName := "_tbl_ghk"
	valueArgs := []interface{}{"mona", "mascot", int8(-17), "anothername", "anotherposition", int8(-2)}
	uniqueKeyColumns := NewColumnList([]string{"name", "position", "my_very_long_column_that_is_64_utf8_characters_long_很长很长很长很长很长很长"})
	builder, err := NewCheckpointQueryBuilder(databaseName, tableName, uniqueKeyColumns)
	require.NoError(t, err)
	query, uniqueKeyArgs, err := builder.BuildQuery(valueArgs)
	require.NoError(t, err)
	expected := `
		insert /* gh-ost */ into mydb._tbl_ghk
		(gh_ost_chk_timestamp, gh_ost_chk_coords, gh_ost_chk_iteration,
		 gh_ost_rows_copied, gh_ost_dml_applied, gh_ost_is_cutover,
		 name_min, position_min, my_very_long_column_that_is_64_utf8_characters_long_很长很长很长很长_min,
		 name_max, position_max, my_very_long_column_that_is_64_utf8_characters_long_很长很长很长很长_max)
		values
		(unix_timestamp(now()), ?, ?,
			 ?, ?, ?,
			 ?, ?, ?,
			 ?, ?, ?)
    `
	require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
	require.Equal(t, []interface{}{"mona", "mascot", int8(-17), "anothername", "anotherposition", int8(-2)}, uniqueKeyArgs)
}
