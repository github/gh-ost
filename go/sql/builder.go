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
	LessThanOrEqualsComparisonSign    ValueComparisonSign = "<="
	EqualsComparisonSign              ValueComparisonSign = "="
	GreaterThanOrEqualsComparisonSign ValueComparisonSign = ">="
	GreaterThanComparisonSign         ValueComparisonSign = ">"
	NotEqualsComparisonSign           ValueComparisonSign = "!="
)

type PartitionInfo struct {
	PartitionName string
	TableRows     int64
}

func (p *PartitionInfo) String() string {
	return fmt.Sprintf("{Name: %s, Rows: %d}", p.PartitionName, p.TableRows)
}

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
	values := make([]string, columns.Len(), columns.Len())
	for i, column := range columns.Columns() {
		var token string
		// 时区问题如何处理呢?
		if column.timezoneConversion != nil {
			token = fmt.Sprintf("convert_tz(?, '%s', '%s')", column.timezoneConversion.ToTimezone, "+00:00")
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

func BuildSetPreparedClause(columns *ColumnList) (result string, err error) {
	if columns.Len() == 0 {
		return "", fmt.Errorf("Got 0 columns in BuildSetPreparedClause")
	}
	setTokens := []string{}
	for _, column := range columns.Columns() {
		var setToken string
		if column.timezoneConversion != nil {
			setToken = fmt.Sprintf("%s=convert_tz(?, '%s', '%s')", EscapeName(column.Name), column.timezoneConversion.ToTimezone, "+00:00")
		} else if column.Type == JSONColumnType {
			setToken = fmt.Sprintf("%s=convert(? using utf8mb4)", EscapeName(column.Name))
		} else {
			setToken = fmt.Sprintf("%s=?", EscapeName(column.Name))
		}
		setTokens = append(setTokens, setToken)
	}
	return strings.Join(setTokens, ", "), nil
}

// 构建向量 > 或者 >= 条件
func BuildRangeComparison(columns []string, values []string, args []interface{},
	comparisonSign ValueComparisonSign) (result string, explodedArgs []interface{}, err error) {

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

	// (a[0], a[1], a[2], a[3]) > (1, 2, 3, 4) = value
	for i, column := range columns {
		// (a[0:i] == value[0:i] && a[i] > value[i]) OR ((a[0:ii] == value[0:ii] && a[ii] > value[ii])) OR ...
		// 边界不包含?
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

func BuildRangePreparedComparison(columns *ColumnList, args []interface{}, comparisonSign ValueComparisonSign) (
	result string, explodedArgs []interface{}, err error) {
	values := buildColumnsPreparedValues(columns)
	return BuildRangeComparison(columns.Names(), values, args, comparisonSign)
}

// originFilterCondition 在做schema migration是顺便将数据做个过滤，例如：可以扔掉一周一样的数据
// 直接删除大量数据实在不方便，效率低，而且需要整理再进行optimize; 可以通过gh-ost一步到位
//
func BuildRangeInsertQuery(databaseName, originalTableName, ghostTableName string, partition *PartitionInfo, originFilterCondition string, sharedColumns []string, mappedSharedColumns []string, uniqueKey string, uniqueKeyColumns *ColumnList, rangeStartValues, rangeEndValues []string, rangeStartArgs, rangeEndArgs []interface{}, includeRangeStartValues bool, transactionalTable bool) (result string, explodedArgs []interface{}, err error) {

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
	rangeStartComparison, rangeExplodedArgs, err := BuildRangeComparison(uniqueKeyColumns.Names(),
		rangeStartValues, rangeStartArgs, minRangeComparisonSign)
	if err != nil {
		return "", explodedArgs, err
	}
	explodedArgs = append(explodedArgs, rangeExplodedArgs...)
	rangeEndComparison, rangeExplodedArgs, err := BuildRangeComparison(uniqueKeyColumns.Names(),
		rangeEndValues, rangeEndArgs, LessThanOrEqualsComparisonSign)
	if err != nil {
		return "", explodedArgs, err
	}
	explodedArgs = append(explodedArgs, rangeExplodedArgs...)
	transactionalClause := ""
	if transactionalTable {
		transactionalClause = "lock in share mode"
	}

	// insert ignore into 如果之前插入了重复元素，则直接忽略
	// DML Event随时都在监控中，新的修改总会同步到ghost table中
	//
	if len(originFilterCondition) > 0 {
		originFilterCondition = " and (" + originFilterCondition + ")"
	}
	//
	// 例如: select id, a, b, c, d from db.origin_table force index (primary_key) where (id > 1 and id < 1000 and created_time > 12121212)
	//
	partitionInfo := ""
	if partition != nil {
		partitionInfo = fmt.Sprintf("partition(%s)", partition.PartitionName)
	}

	// insert/select都添加partition限制，减少锁表次数
	result = fmt.Sprintf(`
      insert /* gh-ost %s.%s */ ignore into %s.%s %s (%s)
      (select %s from %s.%s %s force index (%s)
        where (%s and %s %s) %s
      )
    `, databaseName, originalTableName, databaseName, ghostTableName, partitionInfo, mappedSharedColumnsListing,
		sharedColumnsListing, databaseName, originalTableName, partitionInfo, uniqueKey,
		rangeStartComparison, rangeEndComparison, originFilterCondition, transactionalClause)
	return result, explodedArgs, nil
}

func BuildRangeInsertPreparedQuery(databaseName, originalTableName, ghostTableName string, partition *PartitionInfo, originFilterCondition string,
	sharedColumns []string, mappedSharedColumns []string, uniqueKey string, uniqueKeyColumns *ColumnList,
	rangeStartArgs, rangeEndArgs []interface{}, includeRangeStartValues bool,
	transactionalTable bool) (result string, explodedArgs []interface{}, err error) {

	rangeStartValues := buildColumnsPreparedValues(uniqueKeyColumns)
	rangeEndValues := buildColumnsPreparedValues(uniqueKeyColumns)

	return BuildRangeInsertQuery(databaseName, originalTableName, ghostTableName,
		partition, originFilterCondition,
		sharedColumns, mappedSharedColumns,
		uniqueKey, uniqueKeyColumns,
		rangeStartValues, rangeEndValues, rangeStartArgs, rangeEndArgs,
		includeRangeStartValues, transactionalTable)
}

func BuildUniqueKeyRangeEndPreparedQueryViaOffset(databaseName, tableName string, partition *PartitionInfo, uniqueKeyColumns *ColumnList, rangeStartArgs, rangeEndArgs []interface{}, chunkSize int64, includeRangeStartValues bool, hint string) (result string, explodedArgs []interface{}, err error) {
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
	uniqueKeyColumnAscending := make([]string, len(uniqueKeyColumnNames), len(uniqueKeyColumnNames))
	uniqueKeyColumnDescending := make([]string, len(uniqueKeyColumnNames), len(uniqueKeyColumnNames))
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
	partitionInfo := ""
	if partition != nil {
		partitionInfo = fmt.Sprintf("partition(%s)", partition.PartitionName)
	}
	result = fmt.Sprintf(`
				select  /* gh-ost %s.%s %s */
						%s
					from
						%s.%s %s 
					where %s and %s
					order by
						%s
					limit 1
					offset %d
    `, databaseName, tableName, hint,
		strings.Join(uniqueKeyColumnNames, ", "),
		databaseName, tableName, partitionInfo, // dbname.tablename
		rangeStartComparison, rangeEndComparison,
		strings.Join(uniqueKeyColumnAscending, ", "),
		(chunkSize - 1),
	)
	return result, explodedArgs, nil
}

func BuildUniqueKeyRangeEndPreparedQueryViaTemptable(databaseName, tableName string, partition *PartitionInfo, uniqueKeyColumns *ColumnList, rangeStartArgs, rangeEndArgs []interface{}, chunkSize int64, includeRangeStartValues bool, hint string) (result string, explodedArgs []interface{}, err error) {
	if uniqueKeyColumns.Len() == 0 {
		return "", explodedArgs, fmt.Errorf("Got 0 columns in BuildUniqueKeyRangeEndPreparedQuery")
	}
	databaseName = EscapeName(databaseName)
	tableName = EscapeName(tableName)

	// > vs. >=
	var startRangeComparisonSign = GreaterThanComparisonSign
	if includeRangeStartValues {
		startRangeComparisonSign = GreaterThanOrEqualsComparisonSign
	}

	// 两组条件的筛选: 1
	// a > (1, 1, 2, 3)
	rangeStartComparison, rangeExplodedArgs, err := BuildRangePreparedComparison(uniqueKeyColumns, rangeStartArgs, startRangeComparisonSign)
	if err != nil {
		return "", explodedArgs, err
	}
	explodedArgs = append(explodedArgs, rangeExplodedArgs...)

	// 两组条件的筛选: 2
	// a < (100, 100, 100, 1212)
	rangeEndComparison, rangeExplodedArgs, err := BuildRangePreparedComparison(uniqueKeyColumns, rangeEndArgs, LessThanOrEqualsComparisonSign)
	if err != nil {
		return "", explodedArgs, err
	}
	explodedArgs = append(explodedArgs, rangeExplodedArgs...)

	// 合并条件，生成SQL
	uniqueKeyColumnNames := duplicateNames(uniqueKeyColumns.Names())
	uniqueKeyColumnAscending := make([]string, len(uniqueKeyColumnNames), len(uniqueKeyColumnNames))
	uniqueKeyColumnDescending := make([]string, len(uniqueKeyColumnNames), len(uniqueKeyColumnNames))
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
	partitionInfo := ""
	if partition != nil {
		partitionInfo = fmt.Sprintf("partition(%s)", partition.PartitionName)
	}
	// 如何获取某个区间的端点？
	result = fmt.Sprintf(`
      select /* gh-ost %s.%s %s */ %s
				from (
					select
							%s
						from
							%s.%s %s 
						where %s and %s
						order by
							%s
						limit %d
				) select_osc_chunk
			order by
				%s
			limit 1
    `, databaseName, tableName, hint, strings.Join(uniqueKeyColumnNames, ", "),
		// 内部查询，选择最从min-range开始的: chunkSize个元素
		strings.Join(uniqueKeyColumnNames, ", "), databaseName, tableName, partitionInfo,
		rangeStartComparison, rangeEndComparison,
		strings.Join(uniqueKeyColumnAscending, ", "), chunkSize,
		// 按照降序排列，选择最大的value
		strings.Join(uniqueKeyColumnDescending, ", "),
	)
	return result, explodedArgs, nil
}

// Key Min
func BuildUniqueKeyMinValuesPreparedQuery(databaseName, tableName string, partition *PartitionInfo, uniqueKeyColumns *ColumnList) (string, error) {
	return buildUniqueKeyMinMaxValuesPreparedQuery(databaseName, tableName, partition, uniqueKeyColumns, "asc")
}

// Key Max
func BuildUniqueKeyMaxValuesPreparedQuery(databaseName, tableName string, partition *PartitionInfo, uniqueKeyColumns *ColumnList) (string, error) {
	return buildUniqueKeyMinMaxValuesPreparedQuery(databaseName, tableName, partition, uniqueKeyColumns, "desc")
}

func buildUniqueKeyMinMaxValuesPreparedQuery(databaseName, tableName string, partition *PartitionInfo, uniqueKeyColumns *ColumnList, order string) (string, error) {
	if uniqueKeyColumns.Len() == 0 {
		return "", fmt.Errorf("Got 0 columns in BuildUniqueKeyMinMaxValuesPreparedQuery")
	}
	databaseName = EscapeName(databaseName)
	tableName = EscapeName(tableName)

	uniqueKeyColumnNames := duplicateNames(uniqueKeyColumns.Names())
	uniqueKeyColumnOrder := make([]string, len(uniqueKeyColumnNames), len(uniqueKeyColumnNames))

	// 寻找一个唯一索引
	for i, column := range uniqueKeyColumns.Columns() {
		uniqueKeyColumnNames[i] = EscapeName(uniqueKeyColumnNames[i])
		if column.Type == EnumColumnType {
			// enum: concat关系?
			uniqueKeyColumnOrder[i] = fmt.Sprintf("concat(%s) %s", uniqueKeyColumnNames[i], order)
		} else {
			uniqueKeyColumnOrder[i] = fmt.Sprintf("%s %s", uniqueKeyColumnNames[i], order)
		}
	}

	partitionInfo := ""
	if partition != nil {
		partitionInfo = fmt.Sprintf("partition(%s)", partition.PartitionName)
	}

	// 选择某个条件的1条记录
	query := fmt.Sprintf(`
      select /* gh-ost %s.%s */ %s
				from
					%s.%s %s 
				order by
					%s
				limit 1
    `, databaseName, tableName, strings.Join(uniqueKeyColumnNames, ", "),
		databaseName, tableName, partitionInfo,
		strings.Join(uniqueKeyColumnOrder, ", "),
	)
	return query, nil
}

//
// Original Table中删除数据，如何处理呢?
//
func BuildDMLDeleteQuery(databaseName, tableName string,
	tableColumns, uniqueKeyColumns *ColumnList,
	args []interface{}) (result string, uniqueKeyArgs []interface{}, err error) {

	if len(args) != tableColumns.Len() {
		return result, uniqueKeyArgs, fmt.Errorf("args count differs from table column count in BuildDMLDeleteQuery")
	}
	if uniqueKeyColumns.Len() == 0 {
		return result, uniqueKeyArgs, fmt.Errorf("No unique key columns found in BuildDMLDeleteQuery")
	}

	// uniqueKeyColumns 是前后都完全一致的列?
	for _, column := range uniqueKeyColumns.Columns() {
		tableOrdinal := tableColumns.Ordinals[column.Name] // uniqueKey的位置
		arg := column.convertArg(args[tableOrdinal])       // args: full row image
		uniqueKeyArgs = append(uniqueKeyArgs, arg)
	}

	databaseName = EscapeName(databaseName)
	tableName = EscapeName(tableName)
	equalsComparison, err := BuildEqualsPreparedComparison(uniqueKeyColumns.Names())
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

func BuildDMLInsertQuery(databaseName, tableName string,
	tableColumns, sharedColumns, mappedSharedColumns *ColumnList, args []interface{}) (result string, sharedArgs []interface{}, err error) {

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

	for _, column := range sharedColumns.Columns() {
		tableOrdinal := tableColumns.Ordinals[column.Name]
		arg := column.convertArg(args[tableOrdinal])
		sharedArgs = append(sharedArgs, arg)
	}

	mappedSharedColumnNames := duplicateNames(mappedSharedColumns.Names())
	for i := range mappedSharedColumnNames {
		mappedSharedColumnNames[i] = EscapeName(mappedSharedColumnNames[i])
	}
	preparedValues := buildColumnsPreparedValues(mappedSharedColumns)

	// 目标Table不存在partition的概念
	result = fmt.Sprintf(`
			replace /* gh-ost %s.%s */ into
				%s.%s 
					(%s)
				values
					(%s)
		`, databaseName, tableName,
		databaseName, tableName,
		strings.Join(mappedSharedColumnNames, ", "),
		strings.Join(preparedValues, ", "),
	)
	return result, sharedArgs, nil
}

func BuildDMLUpdateQuery(databaseName, tableName string,
	tableColumns, sharedColumns, mappedSharedColumns, uniqueKeyColumns *ColumnList, valueArgs, whereArgs []interface{}) (result string, sharedArgs, uniqueKeyArgs []interface{}, err error) {
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

	for _, column := range sharedColumns.Columns() {
		tableOrdinal := tableColumns.Ordinals[column.Name]
		arg := column.convertArg(valueArgs[tableOrdinal])
		sharedArgs = append(sharedArgs, arg)
	}

	for _, column := range uniqueKeyColumns.Columns() {
		tableOrdinal := tableColumns.Ordinals[column.Name]
		arg := column.convertArg(whereArgs[tableOrdinal])
		uniqueKeyArgs = append(uniqueKeyArgs, arg)

	}

	setClause, err := BuildSetPreparedClause(mappedSharedColumns)

	equalsComparison, err := BuildEqualsPreparedComparison(uniqueKeyColumns.Names())
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
