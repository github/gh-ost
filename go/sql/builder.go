/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-osc/blob/master/LICENSE
*/

package sql

import (
	"fmt"
	"strconv"
	"strings"
)

type ValueComparisonSign string

const (
	LessThanComparisonSign            ValueComparisonSign = "<"
	LessThanOrEqualsComparisonSign                        = "<="
	EqualsComparisonSign                                  = "="
	GreaterThanOrEqualsComparisonSign                     = ">="
	GreaterThanComparisonSign                             = ">"
	NotEqualsComparisonSign                               = "!="
)

// EscapeName will escape a db/table/column/... name by wrapping with backticks.
// It is not fool proof. I'm just trying to do the right thing here, not solving
// SQL injection issues, which should be irrelevant for this tool.
func EscapeName(name string) string {
	if unquoted, err := strconv.Unquote(name); err == nil {
		name = unquoted
	}
	return fmt.Sprintf("`%s`", name)
}

func BuildValueComparison(column string, value string, comparisonSign ValueComparisonSign) (result string, err error) {
	if column == "" {
		return "", fmt.Errorf("Empty column in GetValueComparison")
	}
	if value == "" {
		return "", fmt.Errorf("Empty value in GetValueComparison")
	}
	comparison := fmt.Sprintf("(%s %s %s)", EscapeName(column), string(comparisonSign), value)
	return comparison, err
}

func BuildEqualsComparison(columns []string, values []string) (result string, err error) {
	if len(columns) == 0 {
		return "", fmt.Errorf("Got 0 columns in GetEqualsComparison")
	}
	if len(columns) != len(values) {
		return "", fmt.Errorf("Got %d columns but %d values in GetEqualsComparison", len(columns), len(values))
	}
	comparisons := []string{}
	for i, column := range columns {
		value := values[i]
		comparison, err := BuildValueComparison(column, value, EqualsComparisonSign)
		if err != nil {
			return "", err
		}
		comparisons = append(comparisons, comparison)
	}
	result = strings.Join(comparisons, " and ")
	result = fmt.Sprintf("(%s)", result)
	return result, nil
}

func BuildRangeComparison(columns []string, values []string, comparisonSign ValueComparisonSign) (result string, err error) {
	if len(columns) == 0 {
		return "", fmt.Errorf("Got 0 columns in GetRangeComparison")
	}
	if len(columns) != len(values) {
		return "", fmt.Errorf("Got %d columns but %d values in GetEqualsComparison", len(columns), len(values))
	}
	includeEquals := false
	if comparisonSign == LessThanOrEqualsComparisonSign {
		comparisonSign = LessThanComparisonSign
		includeEquals = true
	}
	if comparisonSign == GreaterThanOrEqualsComparisonSign {
		comparisonSign = GreaterThanComparisonSign
		includeEquals = true
	}
	comparisons := []string{}

	for i, column := range columns {
		//
		value := values[i]
		rangeComparison, err := BuildValueComparison(column, value, comparisonSign)
		if err != nil {
			return "", err
		}
		if len(columns[0:i]) > 0 {
			equalitiesComparison, err := BuildEqualsComparison(columns[0:i], values[0:i])
			if err != nil {
				return "", err
			}
			comparison := fmt.Sprintf("(%s AND %s)", equalitiesComparison, rangeComparison)
			comparisons = append(comparisons, comparison)
		} else {
			comparisons = append(comparisons, rangeComparison)
		}
	}

	if includeEquals {
		comparison, err := BuildEqualsComparison(columns, values)
		if err != nil {
			return "", nil
		}
		comparisons = append(comparisons, comparison)
	}
	result = strings.Join(comparisons, " or ")
	result = fmt.Sprintf("(%s)", result)
	return result, nil
}

func BuildRangePreparedComparison(columns []string, comparisonSign ValueComparisonSign) (result string, err error) {
	values := make([]string, len(columns), len(columns))
	for i := range columns {
		values[i] = "?"
	}
	return BuildRangeComparison(columns, values, comparisonSign)
}

func BuildRangeInsertQuery(databaseName, originalTableName, ghostTableName string, sharedColumns []string, uniqueKey string, uniqueKeyColumns, rangeStartValues, rangeEndValues []string) (string, error) {
	if len(sharedColumns) == 0 {
		return "", fmt.Errorf("Got 0 shared columns in BuildRangeInsertQuery")
	}
	databaseName = EscapeName(databaseName)
	originalTableName = EscapeName(originalTableName)
	ghostTableName = EscapeName(ghostTableName)
	for i := range sharedColumns {
		sharedColumns[i] = EscapeName(sharedColumns[i])
	}
	uniqueKey = EscapeName(uniqueKey)

	sharedColumnsListing := strings.Join(sharedColumns, ", ")
	rangeStartComparison, err := BuildRangeComparison(uniqueKeyColumns, rangeStartValues, GreaterThanOrEqualsComparisonSign)
	if err != nil {
		return "", err
	}
	rangeEndComparison, err := BuildRangeComparison(uniqueKeyColumns, rangeEndValues, LessThanOrEqualsComparisonSign)
	if err != nil {
		return "", err
	}
	query := fmt.Sprintf(`
      insert /* gh-osc %s.%s */ ignore into %s.%s (%s)
      (select %s from %s.%s force index (%s)
        where (%s and %s)
      )
    `, databaseName, originalTableName, databaseName, ghostTableName, sharedColumnsListing,
		sharedColumnsListing, databaseName, originalTableName, uniqueKey,
		rangeStartComparison, rangeEndComparison)
	return query, nil
}

func BuildRangeInsertPreparedQuery(databaseName, originalTableName, ghostTableName string, sharedColumns []string, uniqueKey string, uniqueKeyColumns []string) (string, error) {
	rangeStartValues := make([]string, len(uniqueKeyColumns), len(uniqueKeyColumns))
	rangeEndValues := make([]string, len(uniqueKeyColumns), len(uniqueKeyColumns))
	for i := range uniqueKeyColumns {
		rangeStartValues[i] = "?"
		rangeEndValues[i] = "?"
	}
	return BuildRangeInsertQuery(databaseName, originalTableName, ghostTableName, sharedColumns, uniqueKey, uniqueKeyColumns, rangeStartValues, rangeEndValues)
}

func BuildUniqueKeyRangeEndPreparedQuery(databaseName, originalTableName string, uniqueKeyColumns []string, chunkSize int) (string, error) {
	if len(uniqueKeyColumns) == 0 {
		return "", fmt.Errorf("Got 0 shared columns in BuildRangeInsertQuery")
	}
	databaseName = EscapeName(databaseName)
	originalTableName = EscapeName(originalTableName)

	rangeStartComparison, err := BuildRangePreparedComparison(uniqueKeyColumns, GreaterThanComparisonSign)
	if err != nil {
		return "", err
	}
	rangeEndComparison, err := BuildRangePreparedComparison(uniqueKeyColumns, LessThanOrEqualsComparisonSign)
	if err != nil {
		return "", err
	}
	uniqueKeyColumnAscending := make([]string, len(uniqueKeyColumns), len(uniqueKeyColumns))
	uniqueKeyColumnDescending := make([]string, len(uniqueKeyColumns), len(uniqueKeyColumns))
	for i := range uniqueKeyColumns {
		uniqueKeyColumns[i] = EscapeName(uniqueKeyColumns[i])
		uniqueKeyColumnAscending[i] = fmt.Sprintf("%s asc", uniqueKeyColumns[i])
		uniqueKeyColumnDescending[i] = fmt.Sprintf("%s desc", uniqueKeyColumns[i])
	}
	query := fmt.Sprintf(`
      select /* gh-osc %s.%s */ %s
				from (
					select
							%s
						from
							%s.%s
						where %s and %s
						order by
							%s
						limit %d
				) select_osc_chunk
			order by
				%s
			limit 1
    `, databaseName, originalTableName, strings.Join(uniqueKeyColumns, ", "),
		strings.Join(uniqueKeyColumns, ", "), databaseName, originalTableName,
		rangeStartComparison, rangeEndComparison,
		strings.Join(uniqueKeyColumnAscending, ", "), chunkSize,
		strings.Join(uniqueKeyColumnDescending, ", "),
	)
	return query, nil
}
