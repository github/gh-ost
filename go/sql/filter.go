/*
   Copyright 2025 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package sql

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// ComparisonOperator represents a comparison operator in a filter condition
type ComparisonOperator string

const (
	OpEquals              ComparisonOperator = "="
	OpNotEquals           ComparisonOperator = "!="
	OpNotEqualsAlt        ComparisonOperator = "<>"
	OpLessThan            ComparisonOperator = "<"
	OpLessThanOrEquals    ComparisonOperator = "<="
	OpGreaterThan         ComparisonOperator = ">"
	OpGreaterThanOrEquals ComparisonOperator = ">="
	OpIsNull              ComparisonOperator = "IS NULL"
	OpIsNotNull           ComparisonOperator = "IS NOT NULL"
	OpLike                ComparisonOperator = "LIKE"
	OpIn                  ComparisonOperator = "IN"
)

// FilterCondition represents a single condition in a WHERE clause
type FilterCondition struct {
	Column   string
	Operator ComparisonOperator
	Value    interface{} // string, int64, float64, time.Time, []interface{} for IN, or nil for IS NULL
}

// LogicalOperator represents AND/OR
type LogicalOperator string

const (
	LogicalAnd LogicalOperator = "AND"
	LogicalOr  LogicalOperator = "OR"
)

// RowFilter represents a parsed WHERE clause that can evaluate rows
type RowFilter struct {
	WhereClause string
	Conditions  []FilterCondition
	Operators   []LogicalOperator // len = len(Conditions) - 1
	columnMap   map[string]int    // column name -> ordinal position
}

// NewRowFilter parses a WHERE clause and creates a RowFilter
func NewRowFilter(whereClause string, columns *ColumnList) (*RowFilter, error) {
	if whereClause == "" {
		return nil, nil
	}

	filter := &RowFilter{
		WhereClause: whereClause,
		Conditions:  []FilterCondition{},
		Operators:   []LogicalOperator{},
		columnMap:   make(map[string]int),
	}

	// Build column name -> ordinal map
	if columns != nil {
		for i, col := range columns.Columns() {
			filter.columnMap[strings.ToLower(col.Name)] = i
		}
	}

	// Parse the WHERE clause
	if err := filter.parse(whereClause); err != nil {
		return nil, err
	}

	return filter, nil
}

// parse parses the WHERE clause into conditions
func (f *RowFilter) parse(whereClause string) error {
	// Normalize whitespace
	whereClause = strings.TrimSpace(whereClause)
	if whereClause == "" {
		return nil
	}

	// Split by AND/OR (simple parsing - doesn't handle nested parentheses)
	// This regex captures AND/OR as delimiters while preserving them
	splitRegex := regexp.MustCompile(`(?i)\s+(AND|OR)\s+`)
	parts := splitRegex.Split(whereClause, -1)
	operators := splitRegex.FindAllStringSubmatch(whereClause, -1)

	for i, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		condition, err := f.parseCondition(part)
		if err != nil {
			return fmt.Errorf("failed to parse condition '%s': %w", part, err)
		}
		f.Conditions = append(f.Conditions, condition)

		if i < len(operators) {
			op := strings.ToUpper(strings.TrimSpace(operators[i][1]))
			if op == "AND" {
				f.Operators = append(f.Operators, LogicalAnd)
			} else {
				f.Operators = append(f.Operators, LogicalOr)
			}
		}
	}

	return nil
}

// parseCondition parses a single condition like "column >= 'value'"
func (f *RowFilter) parseCondition(condition string) (FilterCondition, error) {
	condition = strings.TrimSpace(condition)

	// Remove surrounding parentheses if present
	for strings.HasPrefix(condition, "(") && strings.HasSuffix(condition, ")") {
		condition = strings.TrimPrefix(condition, "(")
		condition = strings.TrimSuffix(condition, ")")
		condition = strings.TrimSpace(condition)
	}

	// Check for IS NULL / IS NOT NULL
	isNullRegex := regexp.MustCompile(`(?i)^(\w+)\s+IS\s+NULL$`)
	isNotNullRegex := regexp.MustCompile(`(?i)^(\w+)\s+IS\s+NOT\s+NULL$`)

	if match := isNullRegex.FindStringSubmatch(condition); match != nil {
		return FilterCondition{
			Column:   strings.ToLower(match[1]),
			Operator: OpIsNull,
			Value:    nil,
		}, nil
	}

	if match := isNotNullRegex.FindStringSubmatch(condition); match != nil {
		return FilterCondition{
			Column:   strings.ToLower(match[1]),
			Operator: OpIsNotNull,
			Value:    nil,
		}, nil
	}

	// Check for IN clause
	inRegex := regexp.MustCompile(`(?i)^(\w+)\s+IN\s*\((.+)\)$`)
	if match := inRegex.FindStringSubmatch(condition); match != nil {
		column := strings.ToLower(match[1])
		valuesStr := match[2]
		values, err := f.parseInValues(valuesStr)
		if err != nil {
			return FilterCondition{}, err
		}
		return FilterCondition{
			Column:   column,
			Operator: OpIn,
			Value:    values,
		}, nil
	}

	// Standard comparison operators (order matters - check multi-char first)
	operators := []struct {
		pattern string
		op      ComparisonOperator
	}{
		{"<>", OpNotEqualsAlt},
		{"!=", OpNotEquals},
		{">=", OpGreaterThanOrEquals},
		{"<=", OpLessThanOrEquals},
		{">", OpGreaterThan},
		{"<", OpLessThan},
		{"=", OpEquals},
	}

	for _, opDef := range operators {
		idx := strings.Index(condition, opDef.pattern)
		if idx > 0 {
			column := strings.TrimSpace(condition[:idx])
			valueStr := strings.TrimSpace(condition[idx+len(opDef.pattern):])

			// Remove backticks from column name
			column = strings.Trim(column, "`")
			column = strings.ToLower(column)

			value, err := f.parseValue(valueStr)
			if err != nil {
				return FilterCondition{}, err
			}

			return FilterCondition{
				Column:   column,
				Operator: opDef.op,
				Value:    value,
			}, nil
		}
	}

	return FilterCondition{}, fmt.Errorf("could not parse condition: %s", condition)
}

// parseValue parses a value string into an appropriate Go type
func (f *RowFilter) parseValue(valueStr string) (interface{}, error) {
	valueStr = strings.TrimSpace(valueStr)

	// String literal (single or double quoted)
	if (strings.HasPrefix(valueStr, "'") && strings.HasSuffix(valueStr, "'")) ||
		(strings.HasPrefix(valueStr, "\"") && strings.HasSuffix(valueStr, "\"")) {
		unquoted := valueStr[1 : len(valueStr)-1]
		// Try to parse as date/datetime
		if t, err := time.Parse("2006-01-02 15:04:05", unquoted); err == nil {
			return t, nil
		}
		if t, err := time.Parse("2006-01-02", unquoted); err == nil {
			return t, nil
		}
		return unquoted, nil
	}

	// NULL
	if strings.ToUpper(valueStr) == "NULL" {
		return nil, nil
	}

	// Integer
	if i, err := strconv.ParseInt(valueStr, 10, 64); err == nil {
		return i, nil
	}

	// Float
	if f, err := strconv.ParseFloat(valueStr, 64); err == nil {
		return f, nil
	}

	// Boolean
	if strings.ToUpper(valueStr) == "TRUE" {
		return true, nil
	}
	if strings.ToUpper(valueStr) == "FALSE" {
		return false, nil
	}

	return valueStr, nil
}

// parseInValues parses the values inside an IN clause
func (f *RowFilter) parseInValues(valuesStr string) ([]interface{}, error) {
	// Simple comma split (doesn't handle commas inside strings)
	parts := strings.Split(valuesStr, ",")
	values := make([]interface{}, 0, len(parts))
	for _, part := range parts {
		v, err := f.parseValue(strings.TrimSpace(part))
		if err != nil {
			return nil, err
		}
		values = append(values, v)
	}
	return values, nil
}

// SetColumnMap updates the column name to ordinal mapping
func (f *RowFilter) SetColumnMap(columns *ColumnList) {
	f.columnMap = make(map[string]int)
	if columns != nil {
		for i, col := range columns.Columns() {
			f.columnMap[strings.ToLower(col.Name)] = i
		}
	}
}

// Matches evaluates whether a row (as a slice of values) matches the filter
func (f *RowFilter) Matches(rowValues []interface{}) bool {
	if len(f.Conditions) == 0 {
		return true
	}

	result := f.evaluateCondition(f.Conditions[0], rowValues)

	for i := 1; i < len(f.Conditions); i++ {
		condResult := f.evaluateCondition(f.Conditions[i], rowValues)

		if i-1 < len(f.Operators) {
			switch f.Operators[i-1] {
			case LogicalAnd:
				result = result && condResult
			case LogicalOr:
				result = result || condResult
			}
		}
	}

	return result
}

// evaluateCondition evaluates a single condition against row values
func (f *RowFilter) evaluateCondition(cond FilterCondition, rowValues []interface{}) bool {
	ordinal, exists := f.columnMap[cond.Column]
	if !exists || ordinal >= len(rowValues) {
		// Column not found - default to not matching for safety
		return false
	}

	rowValue := rowValues[ordinal]

	switch cond.Operator {
	case OpIsNull:
		return rowValue == nil
	case OpIsNotNull:
		return rowValue != nil
	case OpIn:
		return f.evaluateIn(rowValue, cond.Value.([]interface{}))
	default:
		return f.compare(rowValue, cond.Value, cond.Operator)
	}
}

// evaluateIn checks if rowValue is in the list of values
func (f *RowFilter) evaluateIn(rowValue interface{}, values []interface{}) bool {
	for _, v := range values {
		if f.equals(rowValue, v) {
			return true
		}
	}
	return false
}

// equals checks if two values are equal (with type coercion)
func (f *RowFilter) equals(a, b interface{}) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Convert both to strings for comparison (simple but handles most cases)
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	return aStr == bStr
}

// compare compares two values using the given operator
func (f *RowFilter) compare(rowValue, filterValue interface{}, op ComparisonOperator) bool {
	if rowValue == nil || filterValue == nil {
		if op == OpEquals || op == OpNotEquals || op == OpNotEqualsAlt {
			isEqual := (rowValue == nil && filterValue == nil)
			if op == OpEquals {
				return isEqual
			}
			return !isEqual
		}
		return false
	}

	// Try to compare as times first
	rowTime := f.toTime(rowValue)
	filterTime := f.toTime(filterValue)
	if rowTime != nil && filterTime != nil {
		return f.compareTimes(*rowTime, *filterTime, op)
	}

	// Try to compare as numbers
	rowNum, rowIsNum := f.toFloat64(rowValue)
	filterNum, filterIsNum := f.toFloat64(filterValue)
	if rowIsNum && filterIsNum {
		return f.compareNumbers(rowNum, filterNum, op)
	}

	// Fall back to string comparison
	rowStr := fmt.Sprintf("%v", rowValue)
	filterStr := fmt.Sprintf("%v", filterValue)
	return f.compareStrings(rowStr, filterStr, op)
}

// toTime attempts to convert a value to time.Time
func (f *RowFilter) toTime(v interface{}) *time.Time {
	switch t := v.(type) {
	case time.Time:
		return &t
	case *time.Time:
		return t
	case string:
		if parsed, err := time.Parse("2006-01-02 15:04:05", t); err == nil {
			return &parsed
		}
		if parsed, err := time.Parse("2006-01-02", t); err == nil {
			return &parsed
		}
	}
	return nil
}

// toFloat64 attempts to convert a value to float64
func (f *RowFilter) toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	case string:
		if f, err := strconv.ParseFloat(n, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

// compareTimes compares two times
func (f *RowFilter) compareTimes(a, b time.Time, op ComparisonOperator) bool {
	switch op {
	case OpEquals:
		return a.Equal(b)
	case OpNotEquals, OpNotEqualsAlt:
		return !a.Equal(b)
	case OpLessThan:
		return a.Before(b)
	case OpLessThanOrEquals:
		return a.Before(b) || a.Equal(b)
	case OpGreaterThan:
		return a.After(b)
	case OpGreaterThanOrEquals:
		return a.After(b) || a.Equal(b)
	}
	return false
}

// compareNumbers compares two numbers
func (f *RowFilter) compareNumbers(a, b float64, op ComparisonOperator) bool {
	switch op {
	case OpEquals:
		return a == b
	case OpNotEquals, OpNotEqualsAlt:
		return a != b
	case OpLessThan:
		return a < b
	case OpLessThanOrEquals:
		return a <= b
	case OpGreaterThan:
		return a > b
	case OpGreaterThanOrEquals:
		return a >= b
	}
	return false
}

// compareStrings compares two strings
func (f *RowFilter) compareStrings(a, b string, op ComparisonOperator) bool {
	switch op {
	case OpEquals:
		return a == b
	case OpNotEquals, OpNotEqualsAlt:
		return a != b
	case OpLessThan:
		return a < b
	case OpLessThanOrEquals:
		return a <= b
	case OpGreaterThan:
		return a > b
	case OpGreaterThanOrEquals:
		return a >= b
	}
	return false
}

// GetWhereClause returns the original WHERE clause
func (f *RowFilter) GetWhereClause() string {
	return f.WhereClause
}

// IsEmpty returns true if the filter has no conditions
func (f *RowFilter) IsEmpty() bool {
	return len(f.Conditions) == 0
}
