/*
   Copyright 2022 GitHub Inc.
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
	LessThanOrEqualsComparisonSign    ValueComparisonSign = "<="
	EqualsComparisonSign              ValueComparisonSign = "="
	GreaterThanOrEqualsComparisonSign ValueComparisonSign = ">="
	GreaterThanComparisonSign         ValueComparisonSign = ">"
	NotEqualsComparisonSign           ValueComparisonSign = "!="
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

func buildColumnsPreparedValues(columns *ColumnList) []string {
	values := make([]string, columns.Len())
	for i, column := range columns.Columns() {
		var token string
		if column.timezoneConversion != nil {
			token = fmt.Sprintf("convert_tz(?, '%s', '%s')", column.timezoneConversion.ToTimezone, "+00:00")
		} else if column.enumToTextConversion {
			token = fmt.Sprintf("ELT(?, %s)", column.EnumValues)
		} else if column.Type == JSONColumnType {
			token = "convert(? using utf8mb4)"
		} else {
			token = "?"
		}
		values[i] = token
	}
	return values
}

func buildPreparedValues(length int) []string {
	values := make([]string, length)
	for i := 0; i < length; i++ {
		values[i] = "?"
	}
	return values
}

func duplicateNames(names []string) []string {
	duplicate := make([]string, len(names))
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

func BuildSetPreparedClause(columns *ColumnList) (result string, err error) {
	if columns.Len() == 0 {
		return "", fmt.Errorf("Got 0 columns in BuildSetPreparedClause")
	}
	setTokens := []string{}
	for _, column := range columns.Columns() {
		var setToken string
		if column.timezoneConversion != nil {
			setToken = fmt.Sprintf("%s=convert_tz(?, '%s', '%s')", EscapeName(column.Name), column.timezoneConversion.ToTimezone, "+00:00")
		} else if column.enumToTextConversion {
			setToken = fmt.Sprintf("%s=ELT(?, %s)", EscapeName(column.Name), column.EnumValues)
		} else if column.Type == JSONColumnType {
			setToken = fmt.Sprintf("%s=convert(? using utf8mb4)", EscapeName(column.Name))
		} else {
			setToken = fmt.Sprintf("%s=?", EscapeName(column.Name))
		}
		setTokens = append(setTokens, setToken)
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
		value := values[i]
		rangeComparison, err := BuildValueComparison(column, value, comparisonSign)
		if err != nil {
			return "", explodedArgs, err
		}
		if i > 0 {
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
			return "", explodedArgs, err
		}
		comparisons = append(comparisons, comparison)
		explodedArgs = append(explodedArgs, args...)
	}
	result = strings.Join(comparisons, " or ")
	result = fmt.Sprintf("(%s)", result)
	return result, explodedArgs, nil
}

func BuildRangePreparedComparison(columns *ColumnList, args []interface{}, comparisonSign ValueComparisonSign) (result string, explodedArgs []interface{}, err error) {
	values := buildColumnsPreparedValues(columns)
	return BuildRangeComparison(columns.Names(), values, args, comparisonSign)
}

func BuildRangeInsertQuery(databaseName, originalTableName, ghostTableName string, sharedColumns []string, mappedSharedColumns []string, uniqueKey string, uniqueKeyColumns *ColumnList, rangeStartValues, rangeEndValues []string, rangeStartArgs, rangeEndArgs []interface{}, includeRangeStartValues bool, transactionalTable bool, noWait bool) (result string, explodedArgs []interface{}, err error) {
	if len(sharedColumns) == 0 {
		return "", explodedArgs, fmt.Errorf("Got 0 shared columns in BuildRangeInsertQuery")
	}
	databaseName = EscapeName(databaseName)
	originalTableName = EscapeName(originalTableName)
	ghostTableName = EscapeName(ghostTableName)

	mappedSharedColumns = duplicateNames(mappedSharedColumns)
	for i := range mappedSharedColumns {
		mappedSharedColumns[i] = EscapeName(mappedSharedColumns[i])
	}
	mappedSharedColumnsListing := strings.Join(mappedSharedColumns, ", ")

	sharedColumns = duplicateNames(sharedColumns)
	for i := range sharedColumns {
		sharedColumns[i] = EscapeName(sharedColumns[i])
	}
	sharedColumnsListing := strings.Join(sharedColumns, ", ")

	uniqueKey = EscapeName(uniqueKey)
	var minRangeComparisonSign ValueComparisonSign = GreaterThanComparisonSign
	if includeRangeStartValues {
		minRangeComparisonSign = GreaterThanOrEqualsComparisonSign
	}
	rangeStartComparison, rangeExplodedArgs, err := BuildRangeComparison(uniqueKeyColumns.Names(), rangeStartValues, rangeStartArgs, minRangeComparisonSign)
	if err != nil {
		return "", explodedArgs, err
	}
	explodedArgs = append(explodedArgs, rangeExplodedArgs...)
	transactionalClause := ""
	if transactionalTable {
		if noWait {
			transactionalClause = "for share nowait"
		} else {
			transactionalClause = "lock in share mode"
		}
	}
	rangeEndComparison, rangeExplodedArgs, err := BuildRangeComparison(uniqueKeyColumns.Names(), rangeEndValues, rangeEndArgs, LessThanOrEqualsComparisonSign)
	if err != nil {
		return "", explodedArgs, err
	}
	explodedArgs = append(explodedArgs, rangeExplodedArgs...)
	result = fmt.Sprintf(`
		insert /* gh-ost %s.%s */ ignore
		into
			%s.%s
			(%s)
		(
			select %s
			from
				%s.%s
			force index (%s)
			where
				(%s and %s)
				%s
		)`,
		databaseName, originalTableName, databaseName, ghostTableName, mappedSharedColumnsListing,
		sharedColumnsListing, databaseName, originalTableName, uniqueKey,
		rangeStartComparison, rangeEndComparison, transactionalClause)
	return result, explodedArgs, nil
}

func BuildRangeInsertPreparedQuery(databaseName, originalTableName, ghostTableName string, sharedColumns []string, mappedSharedColumns []string, uniqueKey string, uniqueKeyColumns *ColumnList, rangeStartArgs, rangeEndArgs []interface{}, includeRangeStartValues bool, transactionalTable bool, noWait bool) (result string, explodedArgs []interface{}, err error) {
	rangeStartValues := buildColumnsPreparedValues(uniqueKeyColumns)
	rangeEndValues := buildColumnsPreparedValues(uniqueKeyColumns)
	return BuildRangeInsertQuery(databaseName, originalTableName, ghostTableName, sharedColumns, mappedSharedColumns, uniqueKey, uniqueKeyColumns, rangeStartValues, rangeEndValues, rangeStartArgs, rangeEndArgs, includeRangeStartValues, transactionalTable, noWait)
}

func BuildUniqueKeyRangeEndPreparedQueryViaOffset(databaseName, tableName string, uniqueKeyColumns *ColumnList, rangeStartArgs, rangeEndArgs []interface{}, chunkSize int64, includeRangeStartValues bool, hint string) (result string, explodedArgs []interface{}, err error) {
	if uniqueKeyColumns.Len() == 0 {
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

	uniqueKeyColumnNames := duplicateNames(uniqueKeyColumns.Names())
	uniqueKeyColumnAscending := make([]string, len(uniqueKeyColumnNames))
	uniqueKeyColumnDescending := make([]string, len(uniqueKeyColumnNames))
	for i, column := range uniqueKeyColumns.Columns() {
		uniqueKeyColumnNames[i] = EscapeName(uniqueKeyColumnNames[i])
		if column.Type == EnumColumnType {
			uniqueKeyColumnAscending[i] = fmt.Sprintf("concat(%s) asc", uniqueKeyColumnNames[i])
			uniqueKeyColumnDescending[i] = fmt.Sprintf("concat(%s) desc", uniqueKeyColumnNames[i])
		} else {
			uniqueKeyColumnAscending[i] = fmt.Sprintf("%s asc", uniqueKeyColumnNames[i])
			uniqueKeyColumnDescending[i] = fmt.Sprintf("%s desc", uniqueKeyColumnNames[i])
		}
	}
	result = fmt.Sprintf(`
		select /* gh-ost %s.%s %s */
			%s
		from
			%s.%s
		where
			%s and %s
		order by
			%s
		limit 1
		offset %d`,
		databaseName, tableName, hint,
		strings.Join(uniqueKeyColumnNames, ", "),
		databaseName, tableName,
		rangeStartComparison, rangeEndComparison,
		strings.Join(uniqueKeyColumnAscending, ", "),
		(chunkSize - 1),
	)
	return result, explodedArgs, nil
}

func BuildUniqueKeyRangeEndPreparedQueryViaTemptable(databaseName, tableName string, uniqueKeyColumns *ColumnList, rangeStartArgs, rangeEndArgs []interface{}, chunkSize int64, includeRangeStartValues bool, hint string) (result string, explodedArgs []interface{}, err error) {
	if uniqueKeyColumns.Len() == 0 {
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

	uniqueKeyColumnNames := duplicateNames(uniqueKeyColumns.Names())
	uniqueKeyColumnAscending := make([]string, len(uniqueKeyColumnNames))
	uniqueKeyColumnDescending := make([]string, len(uniqueKeyColumnNames))
	for i, column := range uniqueKeyColumns.Columns() {
		uniqueKeyColumnNames[i] = EscapeName(uniqueKeyColumnNames[i])
		if column.Type == EnumColumnType {
			uniqueKeyColumnAscending[i] = fmt.Sprintf("concat(%s) asc", uniqueKeyColumnNames[i])
			uniqueKeyColumnDescending[i] = fmt.Sprintf("concat(%s) desc", uniqueKeyColumnNames[i])
		} else {
			uniqueKeyColumnAscending[i] = fmt.Sprintf("%s asc", uniqueKeyColumnNames[i])
			uniqueKeyColumnDescending[i] = fmt.Sprintf("%s desc", uniqueKeyColumnNames[i])
		}
	}
	result = fmt.Sprintf(`
		select /* gh-ost %s.%s %s */ %s
		from (
			select
				%s
			from
				%s.%s
			where
				%s and %s
			order by
				%s
			limit %d) select_osc_chunk
		order by
			%s
		limit 1`,
		databaseName, tableName, hint, strings.Join(uniqueKeyColumnNames, ", "),
		strings.Join(uniqueKeyColumnNames, ", "), databaseName, tableName,
		rangeStartComparison, rangeEndComparison,
		strings.Join(uniqueKeyColumnAscending, ", "), chunkSize,
		strings.Join(uniqueKeyColumnDescending, ", "),
	)
	return result, explodedArgs, nil
}

func BuildUniqueKeyMinValuesPreparedQuery(databaseName, tableName string, uniqueKey *UniqueKey) (string, error) {
	return buildUniqueKeyMinMaxValuesPreparedQuery(databaseName, tableName, uniqueKey, "asc")
}

func BuildUniqueKeyMaxValuesPreparedQuery(databaseName, tableName string, uniqueKey *UniqueKey) (string, error) {
	return buildUniqueKeyMinMaxValuesPreparedQuery(databaseName, tableName, uniqueKey, "desc")
}

func buildUniqueKeyMinMaxValuesPreparedQuery(databaseName, tableName string, uniqueKey *UniqueKey, order string) (string, error) {
	if uniqueKey.Columns.Len() == 0 {
		return "", fmt.Errorf("Got 0 columns in BuildUniqueKeyMinMaxValuesPreparedQuery")
	}
	databaseName = EscapeName(databaseName)
	tableName = EscapeName(tableName)

	uniqueKeyColumnNames := duplicateNames(uniqueKey.Columns.Names())
	uniqueKeyColumnOrder := make([]string, len(uniqueKeyColumnNames))
	for i, column := range uniqueKey.Columns.Columns() {
		uniqueKeyColumnNames[i] = EscapeName(uniqueKeyColumnNames[i])
		if column.Type == EnumColumnType {
			uniqueKeyColumnOrder[i] = fmt.Sprintf("concat(%s) %s", uniqueKeyColumnNames[i], order)
		} else {
			uniqueKeyColumnOrder[i] = fmt.Sprintf("%s %s", uniqueKeyColumnNames[i], order)
		}
	}
	query := fmt.Sprintf(`
		select /* gh-ost %s.%s */ %s
		from
			%s.%s
		force index (%s)
		order by
			%s
		limit 1`,
		databaseName, tableName, strings.Join(uniqueKeyColumnNames, ", "),
		databaseName, tableName, uniqueKey.Name,
		strings.Join(uniqueKeyColumnOrder, ", "),
	)
	return query, nil
}

// DMLDeleteQueryBuilder can build DELETE queries for DML events.
// It holds the prepared query statement so it doesn't need to be recreated every time.
type DMLDeleteQueryBuilder struct {
	tableColumns, uniqueKeyColumns *ColumnList
	preparedStatement              string
}

// NewDMLDeleteQueryBuilder creates a new DMLDeleteQueryBuilder.
// It prepares the DELETE query statement.
// Returns an error if no unique key columns are given
// or the prepared statement cannot be built.
func NewDMLDeleteQueryBuilder(databaseName, tableName string, tableColumns, uniqueKeyColumns *ColumnList) (*DMLDeleteQueryBuilder, error) {
	if uniqueKeyColumns.Len() == 0 {
		return nil, fmt.Errorf("no unique key columns found in NewDMLDeleteQueryBuilder")
	}
	databaseName = EscapeName(databaseName)
	tableName = EscapeName(tableName)
	equalsComparison, err := BuildEqualsPreparedComparison(uniqueKeyColumns.Names())
	if err != nil {
		return nil, err
	}

	stmt := fmt.Sprintf(`
		delete /* gh-ost %s.%s */
		from
			%s.%s
		where
			%s`,
		databaseName, tableName,
		databaseName, tableName,
		equalsComparison,
	)

	b := &DMLDeleteQueryBuilder{
		tableColumns:      tableColumns,
		uniqueKeyColumns:  uniqueKeyColumns,
		preparedStatement: stmt,
	}
	return b, nil
}

// BuildQuery builds the arguments array for a DML event DELETE query.
// It returns the query string and the unique key arguments array.
// Returns an error if the number of arguments is not equal to the number of table columns.
func (b *DMLDeleteQueryBuilder) BuildQuery(args []interface{}) (string, []interface{}, error) {
	if len(args) != b.tableColumns.Len() {
		return "", nil, fmt.Errorf("args count differs from table column count in BuildDMLDeleteQuery")
	}
	uniqueKeyArgs := make([]interface{}, 0, b.uniqueKeyColumns.Len())
	for _, column := range b.uniqueKeyColumns.Columns() {
		tableOrdinal := b.tableColumns.Ordinals[column.Name]
		arg := column.convertArg(args[tableOrdinal], true)
		uniqueKeyArgs = append(uniqueKeyArgs, arg)
	}
	return b.preparedStatement, uniqueKeyArgs, nil
}

// DMLInsertQueryBuilder can build INSERT queries for DML events.
// It holds the prepared query statement so it doesn't need to be recreated every time.
type DMLInsertQueryBuilder struct {
	tableColumns, sharedColumns *ColumnList
	preparedStatement           string
}

// NewDMLInsertQueryBuilder creates a new DMLInsertQueryBuilder.
// It prepares the INSERT query statement.
// Returns an error if no shared columns are given, the shared columns are not a subset of the table columns,
// or the prepared statement cannot be built.
func NewDMLInsertQueryBuilder(databaseName, tableName string, tableColumns, sharedColumns, mappedSharedColumns *ColumnList) (*DMLInsertQueryBuilder, error) {
	if !sharedColumns.IsSubsetOf(tableColumns) {
		return nil, fmt.Errorf("shared columns is not a subset of table columns in NewDMLInsertQueryBuilder")
	}
	if sharedColumns.Len() == 0 {
		return nil, fmt.Errorf("no shared columns found in NewDMLInsertQueryBuilder")
	}
	databaseName = EscapeName(databaseName)
	tableName = EscapeName(tableName)
	mappedSharedColumnNames := duplicateNames(mappedSharedColumns.Names())
	for i := range mappedSharedColumnNames {
		mappedSharedColumnNames[i] = EscapeName(mappedSharedColumnNames[i])
	}
	preparedValues := buildColumnsPreparedValues(mappedSharedColumns)

	stmt := fmt.Sprintf(`
		replace /* gh-ost %s.%s */
		into
			%s.%s
			(%s)
		values
			(%s)`,
		databaseName, tableName,
		databaseName, tableName,
		strings.Join(mappedSharedColumnNames, ", "),
		strings.Join(preparedValues, ", "),
	)

	return &DMLInsertQueryBuilder{
		tableColumns:      tableColumns,
		sharedColumns:     sharedColumns,
		preparedStatement: stmt,
	}, nil
}

// BuildQuery builds the arguments array for a DML event INSERT query.
// It returns the query string and the shared arguments array.
// Returns an error if the number of arguments differs from the number of table columns.
func (b *DMLInsertQueryBuilder) BuildQuery(args []interface{}) (string, []interface{}, error) {
	if len(args) != b.tableColumns.Len() {
		return "", nil, fmt.Errorf("args count differs from table column count in BuildDMLInsertQuery")
	}
	sharedArgs := make([]interface{}, 0, b.sharedColumns.Len())
	for _, column := range b.sharedColumns.Columns() {
		tableOrdinal := b.tableColumns.Ordinals[column.Name]
		arg := column.convertArg(args[tableOrdinal], false)
		sharedArgs = append(sharedArgs, arg)
	}
	return b.preparedStatement, sharedArgs, nil
}

// DMLUpdateQueryBuilder can build UPDATE queries for DML events.
// It holds the prepared query statement so it doesn't need to be recreated every time.
type DMLUpdateQueryBuilder struct {
	tableColumns, sharedColumns, uniqueKeyColumns *ColumnList
	preparedStatement                             string
}

// NewDMLUpdateQueryBuilder creates a new DMLUpdateQueryBuilder.
// It prepares the UPDATE query statement.
// Returns an error if no shared columns are given, the shared columns are not a subset of the table columns,
// no unique key columns are given or the prepared statement cannot be built.
func NewDMLUpdateQueryBuilder(databaseName, tableName string, tableColumns, sharedColumns, mappedSharedColumns, uniqueKeyColumns *ColumnList) (*DMLUpdateQueryBuilder, error) {
	if !sharedColumns.IsSubsetOf(tableColumns) {
		return nil, fmt.Errorf("shared columns is not a subset of table columns in NewDMLUpdateQueryBuilder")
	}
	if sharedColumns.Len() == 0 {
		return nil, fmt.Errorf("no shared columns found in NewDMLUpdateQueryBuilder")
	}
	if uniqueKeyColumns.Len() == 0 {
		return nil, fmt.Errorf("no unique key columns found in NewDMLUpdateQueryBuilder")
	}
	// If unique key contains virtual columns, those column won't be in sharedColumns
	// which only contains non-virtual columns
	nonVirtualUniqueKeyColumns := uniqueKeyColumns.FilterBy(func(column Column) bool { return !column.IsVirtual })
	if !nonVirtualUniqueKeyColumns.IsSubsetOf(sharedColumns) {
		return nil, fmt.Errorf("unique key columns is not a subset of shared columns in NewDMLUpdateQueryBuilder")
	}
	databaseName = EscapeName(databaseName)
	tableName = EscapeName(tableName)
	setClause, err := BuildSetPreparedClause(mappedSharedColumns)
	if err != nil {
		return nil, err
	}

	equalsComparison, err := BuildEqualsPreparedComparison(uniqueKeyColumns.Names())
	if err != nil {
		return nil, err
	}
	stmt := fmt.Sprintf(`
		update /* gh-ost %s.%s */
			%s.%s
		set
			%s
		where
			%s`,
		databaseName, tableName,
		databaseName, tableName,
		setClause,
		equalsComparison,
	)
	return &DMLUpdateQueryBuilder{
		tableColumns:      tableColumns,
		sharedColumns:     sharedColumns,
		uniqueKeyColumns:  uniqueKeyColumns,
		preparedStatement: stmt,
	}, nil
}

// BuildQuery builds the arguments array for a DML event UPDATE query.
// It returns the query string, the shared arguments array, and the unique key arguments array.
func (b *DMLUpdateQueryBuilder) BuildQuery(valueArgs, whereArgs []interface{}) (string, []interface{}, []interface{}, error) {
	sharedArgs := make([]interface{}, 0, b.sharedColumns.Len())
	for _, column := range b.sharedColumns.Columns() {
		tableOrdinal := b.tableColumns.Ordinals[column.Name]
		arg := column.convertArg(valueArgs[tableOrdinal], false)
		sharedArgs = append(sharedArgs, arg)
	}

	uniqueKeyArgs := make([]interface{}, 0, b.uniqueKeyColumns.Len())
	for _, column := range b.uniqueKeyColumns.Columns() {
		tableOrdinal := b.tableColumns.Ordinals[column.Name]
		arg := column.convertArg(whereArgs[tableOrdinal], true)
		uniqueKeyArgs = append(uniqueKeyArgs, arg)
	}

	return b.preparedStatement, sharedArgs, uniqueKeyArgs, nil
}
