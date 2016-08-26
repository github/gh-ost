/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package sql

import (
	"fmt"
	"reflect"
	"strings"
)

// ColumnsMap maps a column onto its ordinal position
type ColumnsMap map[string]int

func NewEmptyColumnsMap() ColumnsMap {
	columnsMap := make(map[string]int)
	return ColumnsMap(columnsMap)
}

func NewColumnsMap(orderedNames []string) ColumnsMap {
	columnsMap := NewEmptyColumnsMap()
	for i, column := range orderedNames {
		columnsMap[column] = i
	}
	return columnsMap
}

// ColumnList makes for a named list of columns
type ColumnList struct {
	Names         []string
	Ordinals      ColumnsMap
	UnsignedFlags ColumnsMap
}

// NewColumnList creates an object given ordered list of column names
func NewColumnList(names []string) *ColumnList {
	result := &ColumnList{
		Names:         names,
		UnsignedFlags: NewEmptyColumnsMap(),
	}
	result.Ordinals = NewColumnsMap(result.Names)
	return result
}

// ParseColumnList parses a comma delimited list of column names
func ParseColumnList(columns string) *ColumnList {
	result := &ColumnList{
		Names:         strings.Split(columns, ","),
		UnsignedFlags: NewEmptyColumnsMap(),
	}
	result.Ordinals = NewColumnsMap(result.Names)
	return result
}

func (this *ColumnList) SetUnsigned(columnName string) {
	this.UnsignedFlags[columnName] = 1
}

func (this *ColumnList) IsUnsigned(columnName string) bool {
	return this.UnsignedFlags[columnName] == 1
}

func (this *ColumnList) String() string {
	return strings.Join(this.Names, ",")
}

func (this *ColumnList) Equals(other *ColumnList) bool {
	return reflect.DeepEqual(this.Names, other.Names)
}

// IsSubsetOf returns 'true' when column names of this list are a subset of
// another list, in arbitrary order (order agnostic)
func (this *ColumnList) IsSubsetOf(other *ColumnList) bool {
	for _, column := range this.Names {
		if _, exists := other.Ordinals[column]; !exists {
			return false
		}
	}
	return true
}

func (this *ColumnList) Len() int {
	return len(this.Names)
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
	return fmt.Sprintf("%s: %s; has nullable: %+v", description, this.Columns.Names, this.HasNullable)
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
