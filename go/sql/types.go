/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package sql

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type ColumnType int

const (
	UnknownColumnType ColumnType = iota
	TimestampColumnType
	DateTimeColumnType
	EnumColumnType
	MediumIntColumnType
	JSONColumnType
	FloatColumnType
)

const maxMediumintUnsigned int32 = 16777215

type TimezoneConversion struct {
	ToTimezone string
}

type Column struct {
	Name               string
	IsUnsigned         bool
	Charset            string
	Type               ColumnType
	timezoneConversion *TimezoneConversion
}

func (this *Column) convertArg(arg interface{}) interface{} {
	if s, ok := arg.(string); ok {
		// string, charset conversion
		if encoding, ok := charsetEncodingMap[this.Charset]; ok {
			arg, _ = encoding.NewDecoder().String(s)
		}
		return arg
	}

	if this.IsUnsigned {
		if i, ok := arg.(int8); ok {
			return uint8(i)
		}
		if i, ok := arg.(int16); ok {
			return uint16(i)
		}
		if i, ok := arg.(int32); ok {
			if this.Type == MediumIntColumnType {
				// problem with mediumint is that it's a 3-byte type. There is no compatible golang type to match that.
				// So to convert from negative to positive we'd need to convert the value manually
				if i >= 0 {
					return i
				}
				return uint32(maxMediumintUnsigned + i + 1)
			}
			return uint32(i)
		}
		if i, ok := arg.(int64); ok {
			return strconv.FormatUint(uint64(i), 10)
		}
		if i, ok := arg.(int); ok {
			return uint(i)
		}
	}
	return arg
}

func NewColumns(names []string) []Column {
	result := make([]Column, len(names))
	for i := range names {
		result[i].Name = names[i]
	}
	return result
}

func ParseColumns(names string) []Column {
	namesArray := strings.Split(names, ",")
	return NewColumns(namesArray)
}

// ColumnsMap maps a column name onto its ordinal position
type ColumnsMap map[string]int

func NewEmptyColumnsMap() ColumnsMap {
	columnsMap := make(map[string]int)
	return ColumnsMap(columnsMap)
}

func NewColumnsMap(orderedColumns []Column) ColumnsMap {
	columnsMap := NewEmptyColumnsMap()
	for i, column := range orderedColumns {
		columnsMap[column.Name] = i
	}
	return columnsMap
}

// ColumnList makes for a named list of columns
type ColumnList struct {
	columns  []Column
	Ordinals ColumnsMap
}

// NewColumnList creates an object given ordered list of column names
func NewColumnList(names []string) *ColumnList {
	result := &ColumnList{
		columns: NewColumns(names),
	}
	result.Ordinals = NewColumnsMap(result.columns)
	return result
}

// ParseColumnList parses a comma delimited list of column names
func ParseColumnList(names string) *ColumnList {
	result := &ColumnList{
		columns: ParseColumns(names),
	}
	result.Ordinals = NewColumnsMap(result.columns)
	return result
}

func (this *ColumnList) Columns() []Column {
	return this.columns
}

func (this *ColumnList) Names() []string {
	names := make([]string, len(this.columns))
	for i := range this.columns {
		names[i] = this.columns[i].Name
	}
	return names
}

func (this *ColumnList) GetColumn(columnName string) *Column {
	if ordinal, ok := this.Ordinals[columnName]; ok {
		return &this.columns[ordinal]
	}
	return nil
}

func (this *ColumnList) SetUnsigned(columnName string) {
	this.GetColumn(columnName).IsUnsigned = true
}

func (this *ColumnList) IsUnsigned(columnName string) bool {
	return this.GetColumn(columnName).IsUnsigned
}

func (this *ColumnList) SetCharset(columnName string, charset string) {
	this.GetColumn(columnName).Charset = charset
}

func (this *ColumnList) GetCharset(columnName string) string {
	return this.GetColumn(columnName).Charset
}

func (this *ColumnList) SetColumnType(columnName string, columnType ColumnType) {
	this.GetColumn(columnName).Type = columnType
}

func (this *ColumnList) GetColumnType(columnName string) ColumnType {
	return this.GetColumn(columnName).Type
}

func (this *ColumnList) SetConvertDatetimeToTimestamp(columnName string, toTimezone string) {
	this.GetColumn(columnName).timezoneConversion = &TimezoneConversion{ToTimezone: toTimezone}
}

func (this *ColumnList) HasTimezoneConversion(columnName string) bool {
	return this.GetColumn(columnName).timezoneConversion != nil
}

func (this *ColumnList) String() string {
	return strings.Join(this.Names(), ",")
}

func (this *ColumnList) Equals(other *ColumnList) bool {
	return reflect.DeepEqual(this.Columns, other.Columns)
}

func (this *ColumnList) EqualsByNames(other *ColumnList) bool {
	return reflect.DeepEqual(this.Names(), other.Names())
}

// IsSubsetOf returns 'true' when column names of this list are a subset of
// another list, in arbitrary order (order agnostic)
func (this *ColumnList) IsSubsetOf(other *ColumnList) bool {
	for _, column := range this.columns {
		if _, exists := other.Ordinals[column.Name]; !exists {
			return false
		}
	}
	return true
}

func (this *ColumnList) Len() int {
	return len(this.columns)
}

// UniqueKey is the combination of a key's name and columns
type UniqueKey struct {
	Name            string
	Columns         ColumnList
	HasNullable     bool
	IsAutoIncrement bool
}

// IsPrimary checks if this unique key is primary
func (this *UniqueKey) IsPrimary() bool {
	return this.Name == "PRIMARY"
}

func (this *UniqueKey) Len() int {
	return this.Columns.Len()
}

func (this *UniqueKey) String() string {
	description := this.Name
	if this.IsAutoIncrement {
		description = fmt.Sprintf("%s (auto_increment)", description)
	}
	return fmt.Sprintf("%s: %s; has nullable: %+v", description, this.Columns.Names(), this.HasNullable)
}

type ColumnValues struct {
	abstractValues []interface{}
	ValuesPointers []interface{}
}

func NewColumnValues(length int) *ColumnValues {
	result := &ColumnValues{
		abstractValues: make([]interface{}, length),
		ValuesPointers: make([]interface{}, length),
	}
	for i := 0; i < length; i++ {
		result.ValuesPointers[i] = &result.abstractValues[i]
	}

	return result
}

func ToColumnValues(abstractValues []interface{}) *ColumnValues {
	result := &ColumnValues{
		abstractValues: abstractValues,
		ValuesPointers: make([]interface{}, len(abstractValues)),
	}
	for i := 0; i < len(abstractValues); i++ {
		result.ValuesPointers[i] = &result.abstractValues[i]
	}

	return result
}

func (this *ColumnValues) AbstractValues() []interface{} {
	return this.abstractValues
}

func (this *ColumnValues) StringColumn(index int) string {
	val := this.AbstractValues()[index]
	if ints, ok := val.([]uint8); ok {
		return string(ints)
	}
	return fmt.Sprintf("%+v", val)
}

func (this *ColumnValues) String() string {
	stringValues := []string{}
	for i := range this.AbstractValues() {
		stringValues = append(stringValues, this.StringColumn(i))
	}
	return strings.Join(stringValues, ",")
}
