/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
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

func fixArgType(arg interface{}, isUnsigned bool) interface{} {
	if !isUnsigned {
		return arg
	}
	// unsigned
	if i, ok := arg.(int8); ok {
		return uint8(i)
	}
	if i, ok := arg.(int16); ok {
		return uint16(i)
	}
	if i, ok := arg.(int32); ok {
		return uint32(i)
	}
	if i, ok := arg.(int64); ok {
		return strconv.FormatUint(uint64(i), 10)
	}
	if i, ok := arg.(int); ok {
		return uint(i)
	}
	return arg
}

func buildPreparedValues(length int) []string {
	values := make([]string, length, length)
	for i := 0; i < length; i++ {
		values[i] = "?"
	}
	return values
}

func duplicateNames(names []string) []string {
	duplicate := make([]string, len(names), len(names))
	copy(duplicate, names)
	return duplicate
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

func BuildEqualsPreparedComparison(columns []string) (result string, err error) {
	values := buildPreparedValues(len(columns))
	return BuildEqualsComparison(columns, values)
}

func BuildSetPreparedClause(columns []string) (result string, err error) {
	if len(columns) == 0 {
		return "", fmt.Errorf("Got 0 columns in BuildSetPreparedClause")
	}
	setTokens := []string{}
	for _, column := range columns {
		setTokens = append(setTokens, fmt.Sprintf("%s=?", EscapeName(column)))
	}
	return strings.Join(setTokens, ", "), nil
}

func BuildRangeComparison(columns []string, values []string, args []interface{}, comparisonSign ValueComparisonSign) (result string, explodedArgs []interface{}, err error) {
	if len(columns) == 0 {
		return "", explodedArgs, fmt.Errorf("Got 0 columns in GetRangeComparison")
	}
	if len(columns) != len(values) {
		return "", explodedArgs, fmt.Errorf("Got %d columns but %d values in GetEqualsComparison", len(columns), len(values))
	}
	if len(columns) != len(args) {
		return "", explodedArgs, fmt.Errorf("Got %d columns but %d args in GetEqualsComparison", len(columns), len(args))
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
			return "", explodedArgs, err
		}
		if len(columns[0:i]) > 0 {
			equalitiesComparison, err := BuildEqualsComparison(columns[0:i], values[0:i])
			if err != nil {
				return "", explodedArgs, err
			}
			comparison := fmt.Sprintf("(%s AND %s)", equalitiesComparison, rangeComparison)
			comparisons = append(comparisons, comparison)
			explodedArgs = append(explodedArgs, args[0:i]...)
			explodedArgs = append(explodedArgs, args[i])
		} else {
			comparisons = append(comparisons, rangeComparison)
			explodedArgs = append(explodedArgs, args[i])
		}
	}

	if includeEquals {
		comparison, err := BuildEqualsComparison(columns, values)
		if err != nil {
			return "", explodedArgs, nil
		}
		comparisons = append(comparisons, comparison)
		explodedArgs = append(explodedArgs, args...)
	}
	result = strings.Join(comparisons, " or ")
	result = fmt.Sprintf("(%s)", result)
	return result, explodedArgs, nil
}

func BuildRangePreparedComparison(columns []string, args []interface{}, comparisonSign ValueComparisonSign) (result string, explodedArgs []interface{}, err error) {
	values := buildPreparedValues(len(columns))
	return BuildRangeComparison(columns, values, args, comparisonSign)
}

func BuildRangeInsertQuery(databaseName, originalTableName, ghostTableName string, sharedColumns []string, mappedSharedColumns []string, virtualColumns []string, uniqueKey string, uniqueKeyColumns, rangeStartValues, rangeEndValues []string, rangeStartArgs, rangeEndArgs []interface{}, includeRangeStartValues bool, transactionalTable bool) (result string, explodedArgs []interface{}, err error) {
	var filteredMappedSharedColumns []string
	var filteredSharedColumns []string

	if len(sharedColumns) == 0 {
		return "", explodedArgs, fmt.Errorf("Got 0 shared columns in BuildRangeInsertQuery")
	}
	databaseName = EscapeName(databaseName)
	originalTableName = EscapeName(originalTableName)
	ghostTableName = EscapeName(ghostTableName)

	mappedSharedColumns = duplicateNames(mappedSharedColumns)
	for i := range mappedSharedColumns {
		isvirtual := false
		for j := range virtualColumns {
			if virtualColumns[j] == mappedSharedColumns[i] {
				isvirtual = true
			}
		}
		if isvirtual == false { // Remove virtual columns from the list as inserting them doesn't make sense and lets the insert fail
			filteredMappedSharedColumns = append(filteredMappedSharedColumns, EscapeName(mappedSharedColumns[i]))
		}
	}
	mappedSharedColumnsListing := strings.Join(filteredMappedSharedColumns, ", ")

	sharedColumns = duplicateNames(sharedColumns)
	for i := range sharedColumns {
		isvirtual := false
		for j := range virtualColumns {
			if virtualColumns[j] == sharedColumns[i] {
				isvirtual = true
			}
		}
		if isvirtual == false { // Remove virtual columns from the list as inserting them doesn't make sense and lets the insert fail
			filteredSharedColumns = append(filteredSharedColumns, EscapeName(sharedColumns[i]))
		}
	}
	sharedColumnsListing := strings.Join(filteredSharedColumns, ", ")

	uniqueKey = EscapeName(uniqueKey)
	var minRangeComparisonSign ValueComparisonSign = GreaterThanComparisonSign
	if includeRangeStartValues {
		minRangeComparisonSign = GreaterThanOrEqualsComparisonSign
	}
	rangeStartComparison, rangeExplodedArgs, err := BuildRangeComparison(uniqueKeyColumns, rangeStartValues, rangeStartArgs, minRangeComparisonSign)
	if err != nil {
		return "", explodedArgs, err
	}
	explodedArgs = append(explodedArgs, rangeExplodedArgs...)
	rangeEndComparison, rangeExplodedArgs, err := BuildRangeComparison(uniqueKeyColumns, rangeEndValues, rangeEndArgs, LessThanOrEqualsComparisonSign)
	if err != nil {
		return "", explodedArgs, err
	}
	explodedArgs = append(explodedArgs, rangeExplodedArgs...)
	transactionalClause := ""
	if transactionalTable {
		transactionalClause = "lock in share mode"
	}
	result = fmt.Sprintf(`
      insert /* gh-ost %s.%s */ ignore into %s.%s (%s)
      (select %s from %s.%s force index (%s)
        where (%s and %s) %s
      )
    `, databaseName, originalTableName, databaseName, ghostTableName, mappedSharedColumnsListing,
		sharedColumnsListing, databaseName, originalTableName, uniqueKey,
		rangeStartComparison, rangeEndComparison, transactionalClause)
	return result, explodedArgs, nil
}

func BuildRangeInsertPreparedQuery(databaseName, originalTableName, ghostTableName string, sharedColumns []string, mappedSharedColumns []string, virtualColumns []string, uniqueKey string, uniqueKeyColumns []string, rangeStartArgs, rangeEndArgs []interface{}, includeRangeStartValues bool, transactionalTable bool) (result string, explodedArgs []interface{}, err error) {
	rangeStartValues := buildPreparedValues(len(uniqueKeyColumns))
	rangeEndValues := buildPreparedValues(len(uniqueKeyColumns))
	return BuildRangeInsertQuery(databaseName, originalTableName, ghostTableName, sharedColumns, mappedSharedColumns, virtualColumns, uniqueKey, uniqueKeyColumns, rangeStartValues, rangeEndValues, rangeStartArgs, rangeEndArgs, includeRangeStartValues, transactionalTable)
}

func BuildUniqueKeyRangeEndPreparedQuery(databaseName, tableName string, uniqueKeyColumns []string, rangeStartArgs, rangeEndArgs []interface{}, chunkSize int64, includeRangeStartValues bool, hint string) (result string, explodedArgs []interface{}, err error) {
	if len(uniqueKeyColumns) == 0 {
		return "", explodedArgs, fmt.Errorf("Got 0 columns in BuildUniqueKeyRangeEndPreparedQuery")
	}
	databaseName = EscapeName(databaseName)
	tableName = EscapeName(tableName)

	var startRangeComparisonSign ValueComparisonSign = GreaterThanComparisonSign
	if includeRangeStartValues {
		startRangeComparisonSign = GreaterThanOrEqualsComparisonSign
	}
	rangeStartComparison, rangeExplodedArgs, err := BuildRangePreparedComparison(uniqueKeyColumns, rangeStartArgs, startRangeComparisonSign)
	if err != nil {
		return "", explodedArgs, err
	}
	explodedArgs = append(explodedArgs, rangeExplodedArgs...)
	rangeEndComparison, rangeExplodedArgs, err := BuildRangePreparedComparison(uniqueKeyColumns, rangeEndArgs, LessThanOrEqualsComparisonSign)
	if err != nil {
		return "", explodedArgs, err
	}
	explodedArgs = append(explodedArgs, rangeExplodedArgs...)

	uniqueKeyColumns = duplicateNames(uniqueKeyColumns)
	uniqueKeyColumnAscending := make([]string, len(uniqueKeyColumns), len(uniqueKeyColumns))
	uniqueKeyColumnDescending := make([]string, len(uniqueKeyColumns), len(uniqueKeyColumns))
	for i := range uniqueKeyColumns {
		uniqueKeyColumns[i] = EscapeName(uniqueKeyColumns[i])
		uniqueKeyColumnAscending[i] = fmt.Sprintf("%s asc", uniqueKeyColumns[i])
		uniqueKeyColumnDescending[i] = fmt.Sprintf("%s desc", uniqueKeyColumns[i])
	}
	result = fmt.Sprintf(`
      select /* gh-ost %s.%s %s */ %s
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
    `, databaseName, tableName, hint, strings.Join(uniqueKeyColumns, ", "),
		strings.Join(uniqueKeyColumns, ", "), databaseName, tableName,
		rangeStartComparison, rangeEndComparison,
		strings.Join(uniqueKeyColumnAscending, ", "), chunkSize,
		strings.Join(uniqueKeyColumnDescending, ", "),
	)
	return result, explodedArgs, nil
}

func BuildUniqueKeyMinValuesPreparedQuery(databaseName, tableName string, uniqueKeyColumns []string) (string, error) {
	return buildUniqueKeyMinMaxValuesPreparedQuery(databaseName, tableName, uniqueKeyColumns, "asc")
}

func BuildUniqueKeyMaxValuesPreparedQuery(databaseName, tableName string, uniqueKeyColumns []string) (string, error) {
	return buildUniqueKeyMinMaxValuesPreparedQuery(databaseName, tableName, uniqueKeyColumns, "desc")
}

func buildUniqueKeyMinMaxValuesPreparedQuery(databaseName, tableName string, uniqueKeyColumns []string, order string) (string, error) {
	if len(uniqueKeyColumns) == 0 {
		return "", fmt.Errorf("Got 0 columns in BuildUniqueKeyMinMaxValuesPreparedQuery")
	}
	databaseName = EscapeName(databaseName)
	tableName = EscapeName(tableName)

	uniqueKeyColumns = duplicateNames(uniqueKeyColumns)
	uniqueKeyColumnOrder := make([]string, len(uniqueKeyColumns), len(uniqueKeyColumns))
	for i := range uniqueKeyColumns {
		uniqueKeyColumns[i] = EscapeName(uniqueKeyColumns[i])
		uniqueKeyColumnOrder[i] = fmt.Sprintf("%s %s", uniqueKeyColumns[i], order)
	}
	query := fmt.Sprintf(`
      select /* gh-ost %s.%s */ %s
				from
					%s.%s
				order by
					%s
				limit 1
    `, databaseName, tableName, strings.Join(uniqueKeyColumns, ", "),
		databaseName, tableName,
		strings.Join(uniqueKeyColumnOrder, ", "),
	)
	return query, nil
}

func BuildDMLDeleteQuery(databaseName, tableName string, tableColumns, uniqueKeyColumns *ColumnList, args []interface{}) (result string, uniqueKeyArgs []interface{}, err error) {
	if len(args) != tableColumns.Len() {
		return result, uniqueKeyArgs, fmt.Errorf("args count differs from table column count in BuildDMLDeleteQuery")
	}
	if uniqueKeyColumns.Len() == 0 {
		return result, uniqueKeyArgs, fmt.Errorf("No unique key columns found in BuildDMLDeleteQuery")
	}
	for _, column := range uniqueKeyColumns.Names {
		tableOrdinal := tableColumns.Ordinals[column]
		arg := fixArgType(args[tableOrdinal], uniqueKeyColumns.IsUnsigned(column))
		uniqueKeyArgs = append(uniqueKeyArgs, arg)
	}
	databaseName = EscapeName(databaseName)
	tableName = EscapeName(tableName)
	equalsComparison, err := BuildEqualsPreparedComparison(uniqueKeyColumns.Names)
	if err != nil {
		return result, uniqueKeyArgs, err
	}
	result = fmt.Sprintf(`
			delete /* gh-ost %s.%s */
				from
					%s.%s
				where
					%s
		`, databaseName, tableName,
		databaseName, tableName,
		equalsComparison,
	)
	return result, uniqueKeyArgs, nil
}

func BuildDMLInsertQuery(databaseName, tableName string, tableColumns, sharedColumns *ColumnList, virtualColumns *ColumnList, args []interface{}) (result string, sharedArgs []interface{}, err error) {
	if len(args) != tableColumns.Len() {
		return result, args, fmt.Errorf("args count differs from table column count in BuildDMLInsertQuery")
	}
	if !sharedColumns.IsSubsetOf(tableColumns) {
		return result, args, fmt.Errorf("shared columns is not a subset of table columns in BuildDMLInsertQuery")
	}
	if sharedColumns.Len() == 0 {
		return result, args, fmt.Errorf("No shared columns found in BuildDMLInsertQuery")
	}
	databaseName = EscapeName(databaseName)
	tableName = EscapeName(tableName)

	for _, column := range sharedColumns.Names {
		tableOrdinal := tableColumns.Ordinals[column]
		arg := fixArgType(args[tableOrdinal], sharedColumns.IsUnsigned(column))
		sharedArgs = append(sharedArgs, arg)
	}

	sharedColumnNames := duplicateNames(sharedColumns.Names)
	for i := range sharedColumnNames {
		sharedColumnNames[i] = EscapeName(sharedColumnNames[i])
	}
	preparedValues := buildPreparedValues(sharedColumns.Len())

	result = fmt.Sprintf(`
			replace /* gh-ost %s.%s */ into
				%s.%s
					(%s)
				values
					(%s)
		`, databaseName, tableName,
		databaseName, tableName,
		strings.Join(sharedColumnNames, ", "),
		strings.Join(preparedValues, ", "),
	)
	return result, sharedArgs, nil
}

func BuildDMLUpdateQuery(databaseName, tableName string, tableColumns, sharedColumns, virtualColumns, uniqueKeyColumns *ColumnList, valueArgs, whereArgs []interface{}) (result string, sharedArgs, uniqueKeyArgs []interface{}, err error) {
	if len(valueArgs) != tableColumns.Len() {
		return result, sharedArgs, uniqueKeyArgs, fmt.Errorf("value args count differs from table column count in BuildDMLUpdateQuery")
	}
	if len(whereArgs) != tableColumns.Len() {
		return result, sharedArgs, uniqueKeyArgs, fmt.Errorf("where args count differs from table column count in BuildDMLUpdateQuery")
	}
	if !sharedColumns.IsSubsetOf(tableColumns) {
		return result, sharedArgs, uniqueKeyArgs, fmt.Errorf("shared columns is not a subset of table columns in BuildDMLUpdateQuery")
	}
	if !uniqueKeyColumns.IsSubsetOf(sharedColumns) {
		return result, sharedArgs, uniqueKeyArgs, fmt.Errorf("unique key columns is not a subset of shared columns in BuildDMLUpdateQuery")
	}
	if sharedColumns.Len() == 0 {
		return result, sharedArgs, uniqueKeyArgs, fmt.Errorf("No shared columns found in BuildDMLUpdateQuery")
	}
	if uniqueKeyColumns.Len() == 0 {
		return result, sharedArgs, uniqueKeyArgs, fmt.Errorf("No unique key columns found in BuildDMLUpdateQuery")
	}
	databaseName = EscapeName(databaseName)
	tableName = EscapeName(tableName)

	for _, column := range sharedColumns.Names {
		tableOrdinal := tableColumns.Ordinals[column]
		arg := fixArgType(valueArgs[tableOrdinal], sharedColumns.IsUnsigned(column))
		sharedArgs = append(sharedArgs, arg)
	}

	for _, column := range uniqueKeyColumns.Names {
		tableOrdinal := tableColumns.Ordinals[column]
		arg := fixArgType(whereArgs[tableOrdinal], uniqueKeyColumns.IsUnsigned(column))
		uniqueKeyArgs = append(uniqueKeyArgs, arg)
	}

	sharedColumnNames := duplicateNames(sharedColumns.Names)
	for i := range sharedColumnNames {
		sharedColumnNames[i] = EscapeName(sharedColumnNames[i])
	}
	setClause, err := BuildSetPreparedClause(sharedColumnNames)

	equalsComparison, err := BuildEqualsPreparedComparison(uniqueKeyColumns.Names)
	result = fmt.Sprintf(`
 			update /* gh-ost %s.%s */
 					%s.%s
				set
					%s
				where
 					%s
 		`, databaseName, tableName,
		databaseName, tableName,
		setClause,
		equalsComparison,
	)
	return result, sharedArgs, uniqueKeyArgs, nil
}
