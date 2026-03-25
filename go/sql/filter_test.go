/*
   Copyright 2025 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package sql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewRowFilter_Empty(t *testing.T) {
	filter, err := NewRowFilter("", nil)
	require.NoError(t, err)
	require.Nil(t, filter)
}

func TestNewRowFilter_SimpleEquals(t *testing.T) {
	columns := NewColumnList([]string{"id", "name", "status"})
	filter, err := NewRowFilter("status = 'active'", columns)
	require.NoError(t, err)
	require.NotNil(t, filter)
	require.Len(t, filter.Conditions, 1)
	require.Equal(t, "status", filter.Conditions[0].Column)
	require.Equal(t, OpEquals, filter.Conditions[0].Operator)
	require.Equal(t, "active", filter.Conditions[0].Value)
}

func TestNewRowFilter_NumericComparison(t *testing.T) {
	columns := NewColumnList([]string{"id", "age", "score"})
	filter, err := NewRowFilter("age >= 18", columns)
	require.NoError(t, err)
	require.NotNil(t, filter)
	require.Len(t, filter.Conditions, 1)
	require.Equal(t, "age", filter.Conditions[0].Column)
	require.Equal(t, OpGreaterThanOrEquals, filter.Conditions[0].Operator)
	require.Equal(t, int64(18), filter.Conditions[0].Value)
}

func TestNewRowFilter_DateComparison(t *testing.T) {
	columns := NewColumnList([]string{"id", "created_at"})
	filter, err := NewRowFilter("created_at >= '2024-01-01'", columns)
	require.NoError(t, err)
	require.NotNil(t, filter)
	require.Len(t, filter.Conditions, 1)
	require.Equal(t, "created_at", filter.Conditions[0].Column)
	require.Equal(t, OpGreaterThanOrEquals, filter.Conditions[0].Operator)

	expectedDate, _ := time.Parse("2006-01-02", "2024-01-01")
	require.Equal(t, expectedDate, filter.Conditions[0].Value)
}

func TestNewRowFilter_AndConditions(t *testing.T) {
	columns := NewColumnList([]string{"id", "status", "age"})
	filter, err := NewRowFilter("status = 'active' AND age >= 18", columns)
	require.NoError(t, err)
	require.NotNil(t, filter)
	require.Len(t, filter.Conditions, 2)
	require.Len(t, filter.Operators, 1)
	require.Equal(t, LogicalAnd, filter.Operators[0])
}

func TestNewRowFilter_OrConditions(t *testing.T) {
	columns := NewColumnList([]string{"id", "status"})
	filter, err := NewRowFilter("status = 'active' OR status = 'pending'", columns)
	require.NoError(t, err)
	require.NotNil(t, filter)
	require.Len(t, filter.Conditions, 2)
	require.Len(t, filter.Operators, 1)
	require.Equal(t, LogicalOr, filter.Operators[0])
}

func TestNewRowFilter_IsNull(t *testing.T) {
	columns := NewColumnList([]string{"id", "deleted_at"})
	filter, err := NewRowFilter("deleted_at IS NULL", columns)
	require.NoError(t, err)
	require.NotNil(t, filter)
	require.Len(t, filter.Conditions, 1)
	require.Equal(t, "deleted_at", filter.Conditions[0].Column)
	require.Equal(t, OpIsNull, filter.Conditions[0].Operator)
}

func TestNewRowFilter_IsNotNull(t *testing.T) {
	columns := NewColumnList([]string{"id", "email"})
	filter, err := NewRowFilter("email IS NOT NULL", columns)
	require.NoError(t, err)
	require.NotNil(t, filter)
	require.Len(t, filter.Conditions, 1)
	require.Equal(t, "email", filter.Conditions[0].Column)
	require.Equal(t, OpIsNotNull, filter.Conditions[0].Operator)
}

func TestRowFilter_Matches_SimpleEquals(t *testing.T) {
	columns := NewColumnList([]string{"id", "status"})
	filter, err := NewRowFilter("status = 'active'", columns)
	require.NoError(t, err)

	// Row matches
	require.True(t, filter.Matches([]interface{}{1, "active"}))

	// Row doesn't match
	require.False(t, filter.Matches([]interface{}{1, "inactive"}))
}

func TestRowFilter_Matches_NumericGreaterThan(t *testing.T) {
	columns := NewColumnList([]string{"id", "age"})
	filter, err := NewRowFilter("age >= 18", columns)
	require.NoError(t, err)

	require.True(t, filter.Matches([]interface{}{1, int64(18)}))
	require.True(t, filter.Matches([]interface{}{1, int64(25)}))
	require.False(t, filter.Matches([]interface{}{1, int64(17)}))
}

func TestRowFilter_Matches_DateComparison(t *testing.T) {
	columns := NewColumnList([]string{"id", "created_at"})
	filter, err := NewRowFilter("created_at >= '2024-01-01'", columns)
	require.NoError(t, err)

	date2024, _ := time.Parse("2006-01-02", "2024-06-15")
	date2023, _ := time.Parse("2006-01-02", "2023-06-15")

	require.True(t, filter.Matches([]interface{}{1, date2024}))
	require.False(t, filter.Matches([]interface{}{1, date2023}))
}

func TestRowFilter_Matches_AndConditions(t *testing.T) {
	columns := NewColumnList([]string{"id", "status", "age"})
	filter, err := NewRowFilter("status = 'active' AND age >= 18", columns)
	require.NoError(t, err)

	// Both conditions match
	require.True(t, filter.Matches([]interface{}{1, "active", int64(25)}))

	// Only status matches
	require.False(t, filter.Matches([]interface{}{1, "active", int64(15)}))

	// Only age matches
	require.False(t, filter.Matches([]interface{}{1, "inactive", int64(25)}))

	// Neither matches
	require.False(t, filter.Matches([]interface{}{1, "inactive", int64(15)}))
}

func TestRowFilter_Matches_OrConditions(t *testing.T) {
	columns := NewColumnList([]string{"id", "status"})
	filter, err := NewRowFilter("status = 'active' OR status = 'pending'", columns)
	require.NoError(t, err)

	require.True(t, filter.Matches([]interface{}{1, "active"}))
	require.True(t, filter.Matches([]interface{}{1, "pending"}))
	require.False(t, filter.Matches([]interface{}{1, "deleted"}))
}

func TestRowFilter_Matches_IsNull(t *testing.T) {
	columns := NewColumnList([]string{"id", "deleted_at"})
	filter, err := NewRowFilter("deleted_at IS NULL", columns)
	require.NoError(t, err)

	require.True(t, filter.Matches([]interface{}{1, nil}))
	require.False(t, filter.Matches([]interface{}{1, time.Now()}))
}

func TestRowFilter_Matches_IsNotNull(t *testing.T) {
	columns := NewColumnList([]string{"id", "email"})
	filter, err := NewRowFilter("email IS NOT NULL", columns)
	require.NoError(t, err)

	require.True(t, filter.Matches([]interface{}{1, "test@example.com"}))
	require.False(t, filter.Matches([]interface{}{1, nil}))
}

func TestRowFilter_Matches_NotEquals(t *testing.T) {
	columns := NewColumnList([]string{"id", "status"})
	filter, err := NewRowFilter("status != 'deleted'", columns)
	require.NoError(t, err)

	require.True(t, filter.Matches([]interface{}{1, "active"}))
	require.False(t, filter.Matches([]interface{}{1, "deleted"}))
}

func TestRowFilter_Matches_LessThan(t *testing.T) {
	columns := NewColumnList([]string{"id", "priority"})
	filter, err := NewRowFilter("priority < 5", columns)
	require.NoError(t, err)

	require.True(t, filter.Matches([]interface{}{1, int64(3)}))
	require.False(t, filter.Matches([]interface{}{1, int64(5)}))
	require.False(t, filter.Matches([]interface{}{1, int64(7)}))
}

func TestRowFilter_IsEmpty(t *testing.T) {
	columns := NewColumnList([]string{"id"})

	filter, _ := NewRowFilter("", columns)
	require.Nil(t, filter)

	filter2, _ := NewRowFilter("id = 1", columns)
	require.False(t, filter2.IsEmpty())
}

func TestRowFilter_Matches_UnknownColumn(t *testing.T) {
	columns := NewColumnList([]string{"id", "name"})
	filter, err := NewRowFilter("unknown_column = 'value'", columns)
	require.NoError(t, err)

	// Unknown column should result in no match (safe default)
	require.False(t, filter.Matches([]interface{}{1, "test"}))
}

func TestBuildRangeInsertQueryWithFilter(t *testing.T) {
	databaseName := "mydb"
	originalTableName := "tbl"
	ghostTableName := "ghost"
	sharedColumns := []string{"id", "name", "created_at"}
	uniqueKey := "PRIMARY"
	uniqueKeyColumns := NewColumnList([]string{"id"})
	rangeStartArgs := []interface{}{1}
	rangeEndArgs := []interface{}{100}

	// Test with filter
	query, _, err := BuildRangeInsertPreparedQueryWithFilter(
		databaseName, originalTableName, ghostTableName,
		sharedColumns, sharedColumns,
		uniqueKey, uniqueKeyColumns,
		rangeStartArgs, rangeEndArgs,
		true, true, false,
		"created_at >= '2024-01-01'",
	)
	require.NoError(t, err)
	require.Contains(t, query, "and (created_at >= '2024-01-01')")

	// Test without filter (backward compatibility)
	query2, _, err := BuildRangeInsertPreparedQuery(
		databaseName, originalTableName, ghostTableName,
		sharedColumns, sharedColumns,
		uniqueKey, uniqueKeyColumns,
		rangeStartArgs, rangeEndArgs,
		true, true, false,
	)
	require.NoError(t, err)
	require.NotContains(t, query2, "and (created_at")
}
