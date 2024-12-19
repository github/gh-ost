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
	name = strings.Replace(name, "`", "", -1)
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

func TestBuildUniqueKeyRangeEndPreparedQuery(t *testing.T) {
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
			select /* gh-ost mydb.tbl test */ name, position
			from (
				select
					name, position
				from
					mydb.tbl
				where ((name > ?) or (((name = ?)) AND (position > ?))) and ((name < ?) or (((name = ?)) AND (position < ?)) or ((name = ?) and (position = ?)))
				order by
					name asc, position asc
				limit 500) select_osc_chunk
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
			replace /* gh-ost mydb.tbl */
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
			replace /* gh-ost mydb.tbl */
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
			replace /* gh-ost mydb.tbl */
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
			replace /* gh-ost mydb.tbl */
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
			replace /* gh-ost mydb.tbl */
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
		query, sharedArgs, uniqueKeyArgs, err := builder.BuildQuery(valueArgs, whereArgs)
		require.NoError(t, err)
		expected := `
			update /* gh-ost mydb.tbl */
			  mydb.tbl
					set id=?, name=?, position=?, age=?
				where
					((position = ?))
		`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
		require.Equal(t, []interface{}{3, "testname", 17, 23}, sharedArgs)
		require.Equal(t, []interface{}{17}, uniqueKeyArgs)
	}
	{
		sharedColumns := NewColumnList([]string{"id", "name", "position", "age"})
		uniqueKeyColumns := NewColumnList([]string{"position", "name"})
		builder, err := NewDMLUpdateQueryBuilder(databaseName, tableName, tableColumns, sharedColumns, sharedColumns, uniqueKeyColumns)
		require.NoError(t, err)
		query, sharedArgs, uniqueKeyArgs, err := builder.BuildQuery(valueArgs, whereArgs)
		require.NoError(t, err)
		expected := `
			update /* gh-ost mydb.tbl */
			  mydb.tbl
					set id=?, name=?, position=?, age=?
				where
					((position = ?) and (name = ?))
		`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
		require.Equal(t, []interface{}{3, "testname", 17, 23}, sharedArgs)
		require.Equal(t, []interface{}{17, "testname"}, uniqueKeyArgs)
	}
	{
		sharedColumns := NewColumnList([]string{"id", "name", "position", "age"})
		uniqueKeyColumns := NewColumnList([]string{"age"})
		builder, err := NewDMLUpdateQueryBuilder(databaseName, tableName, tableColumns, sharedColumns, sharedColumns, uniqueKeyColumns)
		require.NoError(t, err)
		query, sharedArgs, uniqueKeyArgs, err := builder.BuildQuery(valueArgs, whereArgs)
		require.NoError(t, err)
		expected := `
			update /* gh-ost mydb.tbl */
			  mydb.tbl
					set id=?, name=?, position=?, age=?
				where
					((age = ?))
		`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
		require.Equal(t, []interface{}{3, "testname", 17, 23}, sharedArgs)
		require.Equal(t, []interface{}{56}, uniqueKeyArgs)
	}
	{
		sharedColumns := NewColumnList([]string{"id", "name", "position", "age"})
		uniqueKeyColumns := NewColumnList([]string{"age", "position", "id", "name"})
		builder, err := NewDMLUpdateQueryBuilder(databaseName, tableName, tableColumns, sharedColumns, sharedColumns, uniqueKeyColumns)
		require.NoError(t, err)
		query, sharedArgs, uniqueKeyArgs, err := builder.BuildQuery(valueArgs, whereArgs)
		require.NoError(t, err)
		expected := `
			update /* gh-ost mydb.tbl */
			  mydb.tbl
					set id=?, name=?, position=?, age=?
				where
					((age = ?) and (position = ?) and (id = ?) and (name = ?))
		`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
		require.Equal(t, []interface{}{3, "testname", 17, 23}, sharedArgs)
		require.Equal(t, []interface{}{56, 17, 3, "testname"}, uniqueKeyArgs)
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
		query, sharedArgs, uniqueKeyArgs, err := builder.BuildQuery(valueArgs, whereArgs)
		require.NoError(t, err)
		expected := `
			update /* gh-ost mydb.tbl */
			  mydb.tbl
					set id=?, name=?, role=?, age=?
				where
					((id = ?))
		`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
		require.Equal(t, []interface{}{3, "testname", 17, 23}, sharedArgs)
		require.Equal(t, []interface{}{3}, uniqueKeyArgs)
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
		query, sharedArgs, uniqueKeyArgs, err := builder.BuildQuery(valueArgs, whereArgs)
		require.NoError(t, err)
		expected := `
			update /* gh-ost mydb.tbl */
			  mydb.tbl
					set id=?, name=?, position=?, age=?
				where
					((position = ?))
		`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
		require.Equal(t, []interface{}{3, "testname", int8(-17), int8(-2)}, sharedArgs)
		require.Equal(t, []interface{}{int8(-3)}, uniqueKeyArgs)
	}
	{
		// test unsigned
		sharedColumns.SetUnsigned("age")
		uniqueKeyColumns.SetUnsigned("position")
		query, sharedArgs, uniqueKeyArgs, err := builder.BuildQuery(valueArgs, whereArgs)
		require.NoError(t, err)
		expected := `
			update /* gh-ost mydb.tbl */
			  mydb.tbl
					set id=?, name=?, position=?, age=?
				where
					((position = ?))
		`
		require.Equal(t, normalizeQuery(expected), normalizeQuery(query))
		require.Equal(t, []interface{}{3, "testname", int8(-17), uint8(254)}, sharedArgs)
		require.Equal(t, []interface{}{uint8(253)}, uniqueKeyArgs)
	}
}
