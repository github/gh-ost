/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package sql

import (
	"bytes"
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
	BinaryColumnType
)

const maxMediumintUnsigned int32 = 16777215

type TimezoneConversion struct {
	ToTimezone string
}

type CharacterSetConversion struct {
	ToCharset   string
	FromCharset string
}

type Column struct {
	Name       string
	IsUnsigned bool
	IsVirtual  bool
	Charset    string
	// Type represents a subset of MySQL types
	// used for mapping columns to golang values.
	Type                 ColumnType
	EnumValues           string
	timezoneConversion   *TimezoneConversion
	enumToTextConversion bool
	// add Octet length for binary type, fix bytes with suffix "00" get clipped in mysql binlog.
	// https://github.com/github/gh-ost/issues/909
	BinaryOctetLength uint
	charsetConversion *CharacterSetConversion
	CharacterSetName  string
	Nullable          bool
	MySQLType         string
}

func (cl *Column) convertArg(arg interface{}) interface{} {
	var arg2Bytes []byte
	if s, ok := arg.(string); ok {
		arg2Bytes = []byte(s)
	} else if b, ok := arg.([]uint8); ok {
		arg2Bytes = b
	} else {
		arg2Bytes = nil
	}

	if arg2Bytes != nil {
		if cl.Charset != "" && cl.charsetConversion == nil {
			arg = arg2Bytes
		} else if cl.Charset == "" && (strings.Contains(cl.MySQLType, "binary") || strings.HasSuffix(cl.MySQLType, "blob")) {
			// varbinary/binary/blob column: no charset means binary storage. Return []byte so
			// the MySQL driver sends MYSQL_TYPE_BLOB (binary) rather than MYSQL_TYPE_VAR_STRING
			// (text with the connection's charset/collation metadata, often utf8mb4), which would
			// cause MySQL to validate the bytes and emit Warning 1300 for byte sequences that are
			// invalid in that charset.
			arg = arg2Bytes
		} else {
			if encoding, ok := charsetEncodingMap[cl.Charset]; ok {
				decodedBytes, _ := encoding.NewDecoder().Bytes(arg2Bytes)
				arg = string(decodedBytes)
			}
		}

		if cl.Type == BinaryColumnType {
			size := len(arg2Bytes)
			if uint(size) < cl.BinaryOctetLength {
				buf := bytes.NewBuffer(arg2Bytes)
				for i := uint(0); i < (cl.BinaryOctetLength - uint(size)); i++ {
					buf.Write([]byte{0})
				}
				arg = buf.Bytes()
			}
		}

		return arg
	}

	if cl.IsUnsigned {
		if i, ok := arg.(int8); ok {
			return uint8(i)
		}
		if i, ok := arg.(int16); ok {
			return uint16(i)
		}
		if i, ok := arg.(int32); ok {
			if cl.Type == MediumIntColumnType {
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

func (cl *ColumnList) Columns() []Column {
	return cl.columns
}

func (cl *ColumnList) Names() []string {
	names := make([]string, len(cl.columns))
	for i := range cl.columns {
		names[i] = cl.columns[i].Name
	}
	return names
}

func (cl *ColumnList) GetColumn(columnName string) *Column {
	if ordinal, ok := cl.Ordinals[columnName]; ok {
		return &cl.columns[ordinal]
	}
	return nil
}

func (cl *ColumnList) SetUnsigned(columnName string) {
	cl.GetColumn(columnName).IsUnsigned = true
}

func (cl *ColumnList) IsUnsigned(columnName string) bool {
	return cl.GetColumn(columnName).IsUnsigned
}

func (cl *ColumnList) SetCharset(columnName string, charset string) {
	cl.GetColumn(columnName).Charset = charset
}

func (cl *ColumnList) GetCharset(columnName string) string {
	return cl.GetColumn(columnName).Charset
}

func (cl *ColumnList) SetColumnType(columnName string, columnType ColumnType) {
	cl.GetColumn(columnName).Type = columnType
}

func (cl *ColumnList) GetColumnType(columnName string) ColumnType {
	return cl.GetColumn(columnName).Type
}

func (cl *ColumnList) SetConvertDatetimeToTimestamp(columnName string, toTimezone string) {
	cl.GetColumn(columnName).timezoneConversion = &TimezoneConversion{ToTimezone: toTimezone}
}

func (cl *ColumnList) HasTimezoneConversion(columnName string) bool {
	return cl.GetColumn(columnName).timezoneConversion != nil
}

func (cl *ColumnList) SetEnumToTextConversion(columnName string) {
	cl.GetColumn(columnName).enumToTextConversion = true
}

func (cl *ColumnList) IsEnumToTextConversion(columnName string) bool {
	return cl.GetColumn(columnName).enumToTextConversion
}

func (cl *ColumnList) SetEnumValues(columnName string, enumValues string) {
	cl.GetColumn(columnName).EnumValues = enumValues
}

func (cl *ColumnList) String() string {
	return strings.Join(cl.Names(), ",")
}

func (cl *ColumnList) Equals(other *ColumnList) bool {
	return reflect.DeepEqual(cl.Columns, other.Columns)
}

func (cl *ColumnList) EqualsByNames(other *ColumnList) bool {
	return reflect.DeepEqual(cl.Names(), other.Names())
}

// IsSubsetOf returns 'true' when column names of cl list are a subset of
// another list, in arbitrary order (order agnostic)
func (cl *ColumnList) IsSubsetOf(other *ColumnList) bool {
	for _, column := range cl.columns {
		if _, exists := other.Ordinals[column.Name]; !exists {
			return false
		}
	}
	return true
}

func (cl *ColumnList) FilterBy(f func(Column) bool) *ColumnList {
	filteredCols := make([]Column, 0, len(cl.columns))
	for _, column := range cl.columns {
		if f(column) {
			filteredCols = append(filteredCols, column)
		}
	}
	return &ColumnList{Ordinals: cl.Ordinals, columns: filteredCols}
}

func (cl *ColumnList) Len() int {
	return len(cl.columns)
}

func (cl *ColumnList) SetCharsetConversion(columnName string, fromCharset string, toCharset string) {
	cl.GetColumn(columnName).charsetConversion = &CharacterSetConversion{FromCharset: fromCharset, ToCharset: toCharset}
}

// UniqueKey is the combination of a key's name and columns
type UniqueKey struct {
	Name             string
	NameInGhostTable string // Name of the corresponding key in the Ghost table in case it is being renamed
	Columns          ColumnList
	HasNullable      bool
	IsAutoIncrement  bool
}

// IsPrimary checks if this unique key is primary
func (uk *UniqueKey) IsPrimary() bool {
	return uk.Name == "PRIMARY"
}

func (uk *UniqueKey) Len() int {
	return uk.Columns.Len()
}

func (uk *UniqueKey) String() string {
	description := uk.Name
	if uk.IsAutoIncrement {
		description = fmt.Sprintf("%s (auto_increment)", description)
	}
	return fmt.Sprintf("%s: %s; has nullable: %+v", description, uk.Columns.Names(), uk.HasNullable)
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

func (cv *ColumnValues) AbstractValues() []interface{} {
	return cv.abstractValues
}

func (cv *ColumnValues) StringColumn(index int) string {
	val := cv.AbstractValues()[index]
	if ints, ok := val.([]uint8); ok {
		return fmt.Sprintf("%x", ints)
	}
	return fmt.Sprintf("%+v", val)
}

func (cv *ColumnValues) String() string {
	stringValues := []string{}
	for i := range cv.AbstractValues() {
		stringValues = append(stringValues, cv.StringColumn(i))
	}
	return strings.Join(stringValues, ",")
}

func (cv *ColumnValues) Clone() *ColumnValues {
	newCv := NewColumnValues(len(cv.abstractValues))
	copy(newCv.abstractValues, cv.abstractValues)
	return newCv
}
