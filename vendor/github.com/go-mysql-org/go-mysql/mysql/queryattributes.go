package mysql

import (
	"encoding/binary"
	"fmt"
	"log/slog"
)

// Query Attributes in MySQL are key/value pairs passed along with COM_QUERY or COM_STMT_EXECUTE
//
// Supported Value types: string, uint64
//
// Resources:
// - https://dev.mysql.com/doc/refman/8.4/en/query-attributes.html
// - https://github.com/mysql/mysql-server/blob/trunk/include/mysql/components/services/mysql_query_attributes.h
// - https://archive.fosdem.org/2021/schedule/event/mysql_protocl/
type QueryAttribute struct {
	Name  string
	Value any
}

// TypeAndFlag returns the type MySQL field type of the value and the field flag.
func (qa *QueryAttribute) TypeAndFlag() []byte {
	switch v := qa.Value.(type) {
	case string:
		return []byte{MYSQL_TYPE_STRING, 0x0}
	case uint64:
		return []byte{MYSQL_TYPE_LONGLONG, PARAM_UNSIGNED}
	default:
		slog.Warn("query attribute with unsupported type", slog.String("type", fmt.Sprintf("%T", v)))
	}
	return []byte{0x0, 0x0} // type 0x0, flag 0x0, to not break the protocol
}

// ValueBytes returns the encoded value
func (qa *QueryAttribute) ValueBytes() []byte {
	switch v := qa.Value.(type) {
	case string:
		return PutLengthEncodedString([]byte(v))
	case uint64:
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, v)
		return b
	default:
		slog.Warn("query attribute with unsupported type", slog.String("type", fmt.Sprintf("%T", v)))
	}
	return []byte{0x0} // 0 length value to not break the protocol
}
