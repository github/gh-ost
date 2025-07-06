package mysql

import (
	"bytes"
	"encoding/binary"
	"math"
	"strconv"
	"time"

	"github.com/pingcap/errors"

	"github.com/go-mysql-org/go-mysql/utils"
)

func FormatTextValue(value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case int8:
		return strconv.AppendInt(nil, int64(v), 10), nil
	case int16:
		return strconv.AppendInt(nil, int64(v), 10), nil
	case int32:
		return strconv.AppendInt(nil, int64(v), 10), nil
	case int64:
		return strconv.AppendInt(nil, v, 10), nil
	case int:
		return strconv.AppendInt(nil, int64(v), 10), nil
	case uint8:
		return strconv.AppendUint(nil, uint64(v), 10), nil
	case uint16:
		return strconv.AppendUint(nil, uint64(v), 10), nil
	case uint32:
		return strconv.AppendUint(nil, uint64(v), 10), nil
	case uint64:
		return strconv.AppendUint(nil, v, 10), nil
	case uint:
		return strconv.AppendUint(nil, uint64(v), 10), nil
	case float32:
		return strconv.AppendFloat(nil, float64(v), 'f', -1, 64), nil
	case float64:
		return strconv.AppendFloat(nil, v, 'f', -1, 64), nil
	case []byte:
		return v, nil
	case string:
		return utils.StringToByteSlice(v), nil
	case time.Time:
		return utils.StringToByteSlice(v.Format(time.DateTime)), nil
	case nil:
		return nil, nil
	default:
		return nil, errors.Errorf("invalid type %T", value)
	}
}

func toBinaryDateTime(t time.Time) ([]byte, error) {
	var buf bytes.Buffer

	if t.IsZero() {
		return nil, nil
	}

	year, month, day := t.Year(), t.Month(), t.Day()
	hour, min, sec := t.Hour(), t.Minute(), t.Second()
	nanosec := t.Nanosecond()

	if nanosec > 0 {
		buf.WriteByte(byte(11))
		_ = binary.Write(&buf, binary.LittleEndian, uint16(year))
		buf.WriteByte(byte(month))
		buf.WriteByte(byte(day))
		buf.WriteByte(byte(hour))
		buf.WriteByte(byte(min))
		buf.WriteByte(byte(sec))
		_ = binary.Write(&buf, binary.LittleEndian, uint32(nanosec/1000))
	} else if hour > 0 || min > 0 || sec > 0 {
		buf.WriteByte(byte(7))
		_ = binary.Write(&buf, binary.LittleEndian, uint16(year))
		buf.WriteByte(byte(month))
		buf.WriteByte(byte(day))
		buf.WriteByte(byte(hour))
		buf.WriteByte(byte(min))
		buf.WriteByte(byte(sec))
	} else {
		buf.WriteByte(byte(4))
		_ = binary.Write(&buf, binary.LittleEndian, uint16(year))
		buf.WriteByte(byte(month))
		buf.WriteByte(byte(day))
	}

	return buf.Bytes(), nil
}

func formatBinaryValue(value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case int8:
		return Uint64ToBytes(uint64(v)), nil
	case int16:
		return Uint64ToBytes(uint64(v)), nil
	case int32:
		return Uint64ToBytes(uint64(v)), nil
	case int64:
		return Uint64ToBytes(uint64(v)), nil
	case int:
		return Uint64ToBytes(uint64(v)), nil
	case uint8:
		return Uint64ToBytes(uint64(v)), nil
	case uint16:
		return Uint64ToBytes(uint64(v)), nil
	case uint32:
		return Uint64ToBytes(uint64(v)), nil
	case uint64:
		return Uint64ToBytes(v), nil
	case uint:
		return Uint64ToBytes(uint64(v)), nil
	case float32:
		return Uint64ToBytes(math.Float64bits(float64(v))), nil
	case float64:
		return Uint64ToBytes(math.Float64bits(v)), nil
	case []byte:
		return v, nil
	case string:
		return utils.StringToByteSlice(v), nil
	case time.Time:
		return toBinaryDateTime(v)
	default:
		return nil, errors.Errorf("invalid type %T", value)
	}
}

func fieldType(value interface{}) (typ uint8, err error) {
	switch value.(type) {
	case int8, int16, int32, int64, int:
		typ = MYSQL_TYPE_LONGLONG
	case uint8, uint16, uint32, uint64, uint:
		typ = MYSQL_TYPE_LONGLONG
	case float32, float64:
		typ = MYSQL_TYPE_DOUBLE
	case string, []byte:
		typ = MYSQL_TYPE_VAR_STRING
	case time.Time:
		typ = MYSQL_TYPE_DATETIME
	case nil:
		typ = MYSQL_TYPE_NULL
	default:
		err = errors.Errorf("unsupport type %T for resultset", value)
	}
	return
}

func formatField(field *Field, value interface{}) error {
	switch value.(type) {
	case int8, int16, int32, int64, int:
		field.Charset = 63
		field.Flag = BINARY_FLAG | NOT_NULL_FLAG
	case uint8, uint16, uint32, uint64, uint:
		field.Charset = 63
		field.Flag = BINARY_FLAG | NOT_NULL_FLAG | UNSIGNED_FLAG
	case float32, float64:
		field.Charset = 63
		field.Flag = BINARY_FLAG | NOT_NULL_FLAG
	case string, []byte, time.Time:
		field.Charset = 33
	case nil:
		field.Charset = 33
	default:
		return errors.Errorf("unsupport type %T for resultset", value)
	}
	return nil
}

func BuildSimpleTextResultset(names []string, values [][]interface{}) (*Resultset, error) {
	r := NewResultset(len(names))

	var b []byte

	if len(values) == 0 {
		for i, name := range names {
			r.Fields[i] = &Field{Name: utils.StringToByteSlice(name), Charset: 33, Type: MYSQL_TYPE_NULL}
		}
		return r, nil
	}

	for i, vs := range values {
		if len(vs) != len(r.Fields) {
			return nil, errors.Errorf("row %d has %d column not equal %d", i, len(vs), len(r.Fields))
		}

		var row []byte
		for j, value := range vs {
			typ, err := fieldType(value)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if r.Fields[j] == nil {
				r.Fields[j] = &Field{Name: utils.StringToByteSlice(names[j]), Type: typ}
				err = formatField(r.Fields[j], value)
				if err != nil {
					return nil, errors.Trace(err)
				}
			} else if typ != r.Fields[j].Type {
				// we got another type in the same column. in general, we treat it as an error, except
				// the case, when old value was null, and the new one isn't null, so we can update
				// type info for fields.
				oldIsNull, newIsNull := r.Fields[j].Type == MYSQL_TYPE_NULL, typ == MYSQL_TYPE_NULL
				if oldIsNull && !newIsNull { // old is null, new isn't, update type info.
					r.Fields[j].Type = typ
					err = formatField(r.Fields[j], value)
					if err != nil {
						return nil, errors.Trace(err)
					}
				} else if !oldIsNull && !newIsNull { // different non-null types, that's an error.
					return nil, errors.Errorf("row types aren't consistent")
				}
			}
			b, err = FormatTextValue(value)

			if err != nil {
				return nil, errors.Trace(err)
			}

			if b == nil {
				// NULL value is encoded as 0xfb here (without additional info about length)
				row = append(row, 0xfb)
			} else {
				row = append(row, PutLengthEncodedString(b)...)
			}
		}

		r.RowDatas = append(r.RowDatas, row)
	}

	return r, nil
}

func BuildSimpleBinaryResultset(names []string, values [][]interface{}) (*Resultset, error) {
	r := NewResultset(len(names))

	var b []byte

	bitmapLen := (len(names) + 7 + 2) >> 3

	for i, vs := range values {
		if len(vs) != len(r.Fields) {
			return nil, errors.Errorf("row %d has %d column not equal %d", i, len(vs), len(r.Fields))
		}

		var row []byte
		nullBitmap := make([]byte, bitmapLen)

		row = append(row, 0)
		row = append(row, nullBitmap...)

		for j, value := range vs {
			typ, err := fieldType(value)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if i == 0 {
				field := &Field{Type: typ}
				r.Fields[j] = field
				field.Name = utils.StringToByteSlice(names[j])

				if err = formatField(field, value); err != nil {
					return nil, errors.Trace(err)
				}
			}
			if value == nil {
				nullBitmap[(j+2)/8] |= 1 << (uint(j+2) % 8)
				continue
			}

			b, err = formatBinaryValue(value)

			if err != nil {
				return nil, errors.Trace(err)
			}

			if r.Fields[j].Type == MYSQL_TYPE_VAR_STRING {
				row = append(row, PutLengthEncodedString(b)...)
			} else {
				row = append(row, b...)
			}
		}

		copy(row[1:], nullBitmap)

		r.RowDatas = append(r.RowDatas, row)
	}

	return r, nil
}

func BuildSimpleResultset(names []string, values [][]interface{}, binary bool) (*Resultset, error) {
	if binary {
		return BuildSimpleBinaryResultset(names, values)
	} else {
		return BuildSimpleTextResultset(names, values)
	}
}
