/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-osc/blob/master/LICENSE
*/

package sql

import (
	"fmt"
	"reflect"
	"strings"
)

// ColumnList makes for a named list of columns
type ColumnList []string

// ParseColumnList parses a comma delimited list of column names
func ParseColumnList(columns string) *ColumnList {
	result := ColumnList(strings.Split(columns, ","))
	return &result
}

func (this *ColumnList) String() string {
	return strings.Join(*this, ",")
}

func (this *ColumnList) Equals(other *ColumnList) bool {
	return reflect.DeepEqual(*this, *other)
}

// UniqueKey is the combination of a key's name and columns
type UniqueKey struct {
	Name        string
	Columns     ColumnList
	HasNullable bool
}

// IsPrimary cehcks if this unique key is primary
func (this *UniqueKey) IsPrimary() bool {
	return this.Name == "PRIMARY"
}

func (this *UniqueKey) String() string {
	return fmt.Sprintf("%s: %s; has nullable: %+v", this.Name, this.Columns, this.HasNullable)
}
