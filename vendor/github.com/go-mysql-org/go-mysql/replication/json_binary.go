package replication

import (
	"fmt"
	"math"
	"strconv"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/utils"
	"github.com/goccy/go-json"
	"github.com/pingcap/errors"
)

//nolint:revive // JSONB type tags mirror the upstream MySQL JSON binary format
const (
	JSONB_SMALL_OBJECT byte = iota // small JSON object
	JSONB_LARGE_OBJECT             // large JSON object
	JSONB_SMALL_ARRAY              // small JSON array
	JSONB_LARGE_ARRAY              // large JSON array
	JSONB_LITERAL                  // literal (true/false/null)
	JSONB_INT16                    // int16
	JSONB_UINT16                   // uint16
	JSONB_INT32                    // int32
	JSONB_UINT32                   // uint32
	JSONB_INT64                    // int64
	JSONB_UINT64                   // uint64
	JSONB_DOUBLE                   // double
	JSONB_STRING                   // string
	JSONB_OPAQUE       byte = 0x0f // custom data (any MySQL data type)
)

//nolint:revive // JSONB literal tags mirror the upstream MySQL JSON binary format
const (
	JSONB_NULL_LITERAL  byte = 0x00
	JSONB_TRUE_LITERAL  byte = 0x01
	JSONB_FALSE_LITERAL byte = 0x02
)

const (
	jsonbSmallOffsetSize = 2
	jsonbLargeOffsetSize = 4

	jsonbKeyEntrySizeSmall = 2 + jsonbSmallOffsetSize
	jsonbKeyEntrySizeLarge = 2 + jsonbLargeOffsetSize

	jsonbValueEntrySizeSmall = 1 + jsonbSmallOffsetSize
	jsonbValueEntrySizeLarge = 1 + jsonbLargeOffsetSize
)

var ErrCorruptedJSONDiff = fmt.Errorf("corrupted JSON diff") // ER_CORRUPTED_JSON_DIFF

//nolint:revive // exported type renamed would be a breaking API change
type (
	// JsonDiffOperation is an enum that describes what kind of operation a JsonDiff object represents.
	// https://github.com/mysql/mysql-server/blob/8.0/sql/json_diff.h
	JsonDiffOperation byte
)

type FloatWithTrailingZero float64

//nolint:revive // exported constants renamed would be a breaking API change
const (
	// The JSON value in the given path is replaced with a new value.
	//
	// It has the same effect as `JSON_REPLACE(col, path, value)`.
	JsonDiffOperationReplace = JsonDiffOperation(iota)

	// Add a new element at the given path.
	//
	//  If the path specifies an array element, it has the same effect as `JSON_ARRAY_INSERT(col, path, value)`.
	//
	//  If the path specifies an object member, it has the same effect as `JSON_INSERT(col, path, value)`.
	JsonDiffOperationInsert

	// The JSON value at the given path is removed from an array or object.
	//
	// It has the same effect as `JSON_REMOVE(col, path)`.
	JsonDiffOperationRemove
)

//nolint:revive // exported type renamed would be a breaking API change
type (
	JsonDiff struct {
		Op    JsonDiffOperation
		Path  string
		Value string
	}
)

func (op JsonDiffOperation) String() string {
	switch op {
	case JsonDiffOperationReplace:
		return "Replace"
	case JsonDiffOperationInsert:
		return "Insert"
	case JsonDiffOperationRemove:
		return "Remove"
	default:
		return fmt.Sprintf("Unknown(%d)", op)
	}
}

func (jd *JsonDiff) String() string {
	return fmt.Sprintf("json_diff(op:%s path:%s value:%s)", jd.Op, jd.Path, jd.Value)
}

func (f FloatWithTrailingZero) MarshalJSON() ([]byte, error) {
	if float64(f) == float64(int(f)) {
		return []byte(strconv.FormatFloat(float64(f), 'f', 1, 64)), nil
	}

	return []byte(strconv.FormatFloat(float64(f), 'f', -1, 64)), nil
}

func jsonbGetOffsetSize(isSmall bool) int {
	if isSmall {
		return jsonbSmallOffsetSize
	}

	return jsonbLargeOffsetSize
}

func jsonbGetKeyEntrySize(isSmall bool) int {
	if isSmall {
		return jsonbKeyEntrySizeSmall
	}

	return jsonbKeyEntrySizeLarge
}

func jsonbGetValueEntrySize(isSmall bool) int {
	if isSmall {
		return jsonbValueEntrySizeSmall
	}

	return jsonbValueEntrySizeLarge
}

// decodeJSONBinary decodes the JSON binary encoding data and returns
// the common JSON encoding data.
func (e *RowsEvent) decodeJSONBinary(data []byte) ([]byte, error) {
	d := jsonBinaryDecoder{
		useDecimal:               e.useDecimal,
		useFloatWithTrailingZero: e.useFloatWithTrailingZero,
		ignoreDecodeErr:          e.ignoreJSONDecodeErr,
	}

	if d.isDataShort(data, 1) {
		return nil, d.err
	}

	v := d.decodeValue(data[0], data[1:])
	if d.err != nil {
		return nil, d.err
	}

	return json.Marshal(v)
}

type jsonBinaryDecoder struct {
	useDecimal               bool
	useFloatWithTrailingZero bool
	ignoreDecodeErr          bool
	err                      error
}

func (d *jsonBinaryDecoder) decodeValue(tp byte, data []byte) any {
	if d.err != nil {
		return nil
	}

	switch tp {
	case JSONB_SMALL_OBJECT:
		return d.decodeObjectOrArray(data, true, true)
	case JSONB_LARGE_OBJECT:
		return d.decodeObjectOrArray(data, false, true)
	case JSONB_SMALL_ARRAY:
		return d.decodeObjectOrArray(data, true, false)
	case JSONB_LARGE_ARRAY:
		return d.decodeObjectOrArray(data, false, false)
	case JSONB_LITERAL:
		return d.decodeLiteral(data)
	case JSONB_INT16:
		return d.decodeInt16(data)
	case JSONB_UINT16:
		return d.decodeUint16(data)
	case JSONB_INT32:
		return d.decodeInt32(data)
	case JSONB_UINT32:
		return d.decodeUint32(data)
	case JSONB_INT64:
		return d.decodeInt64(data)
	case JSONB_UINT64:
		return d.decodeUint64(data)
	case JSONB_DOUBLE:
		if d.useFloatWithTrailingZero {
			return d.decodeDoubleWithTrailingZero(data)
		}
		return d.decodeDouble(data)
	case JSONB_STRING:
		return d.decodeString(data)
	case JSONB_OPAQUE:
		return d.decodeOpaque(data)
	default:
		d.err = errors.Errorf("invalid json type %d", tp)
	}

	return nil
}

func (d *jsonBinaryDecoder) decodeObjectOrArray(data []byte, isSmall bool, isObject bool) any {
	offsetSize := jsonbGetOffsetSize(isSmall)
	if d.isDataShort(data, 2*offsetSize) {
		return nil
	}

	count := d.decodeCount(data, isSmall)
	size := d.decodeCount(data[offsetSize:], isSmall)

	if d.isDataShort(data, size) {
		// Before MySQL 5.7.22, json type generated column may have invalid value,
		// bug ref: https://bugs.mysql.com/bug.php?id=88791
		// As generated column value is not used in replication, we can just ignore
		// this error and return a dummy value for this column.
		if d.ignoreDecodeErr {
			d.err = nil
		}
		return nil
	}

	keyEntrySize := jsonbGetKeyEntrySize(isSmall)
	valueEntrySize := jsonbGetValueEntrySize(isSmall)

	headerSize := 2*offsetSize + count*valueEntrySize

	if isObject {
		headerSize += count * keyEntrySize
	}

	if headerSize > size {
		d.err = errors.Errorf("header size %d > size %d", headerSize, size)
		return nil
	}

	var keys []string
	if isObject {
		keys = make([]string, count)
		for i := range count {
			// decode key
			entryOffset := 2*offsetSize + keyEntrySize*i
			keyOffset := d.decodeCount(data[entryOffset:], isSmall)
			keyLength := int(d.decodeUint16(data[entryOffset+offsetSize:]))

			// Key must start after value entry
			if keyOffset < headerSize {
				d.err = errors.Errorf("invalid key offset %d, must > %d", keyOffset, headerSize)
				return nil
			}

			if d.isDataShort(data, keyOffset+keyLength) {
				return nil
			}

			keys[i] = utils.ByteSliceToString(data[keyOffset : keyOffset+keyLength])
		}
	}

	if d.err != nil {
		return nil
	}

	values := make([]any, count)
	for i := range count {
		// decode value
		entryOffset := 2*offsetSize + valueEntrySize*i
		if isObject {
			entryOffset += keyEntrySize * count
		}

		tp := data[entryOffset]

		if isInlineValue(tp, isSmall) {
			values[i] = d.decodeValue(tp, data[entryOffset+1:entryOffset+valueEntrySize])
			continue
		}

		valueOffset := d.decodeCount(data[entryOffset+1:], isSmall)

		if d.isDataShort(data, valueOffset) {
			return nil
		}

		values[i] = d.decodeValue(tp, data[valueOffset:])
	}

	if d.err != nil {
		return nil
	}

	if !isObject {
		return values
	}

	m := make(map[string]any, count)
	for i := range count {
		m[keys[i]] = values[i]
	}

	return m
}

func isInlineValue(tp byte, isSmall bool) bool {
	switch tp {
	case JSONB_INT16, JSONB_UINT16, JSONB_LITERAL:
		return true
	case JSONB_INT32, JSONB_UINT32:
		return !isSmall
	}

	return false
}

func (d *jsonBinaryDecoder) decodeLiteral(data []byte) any {
	if d.isDataShort(data, 1) {
		return nil
	}

	tp := data[0]

	switch tp {
	case JSONB_NULL_LITERAL:
		return nil
	case JSONB_TRUE_LITERAL:
		return true
	case JSONB_FALSE_LITERAL:
		return false
	}

	d.err = errors.Errorf("invalid literal %c", tp)

	return nil
}

func (d *jsonBinaryDecoder) isDataShort(data []byte, expected int) bool {
	if d.err != nil {
		return true
	}

	if len(data) < expected {
		d.err = errors.Errorf("data len %d < expected %d", len(data), expected)
	}

	return d.err != nil
}

func (d *jsonBinaryDecoder) decodeInt16(data []byte) int16 {
	if d.isDataShort(data, 2) {
		return 0
	}

	v := mysql.ParseBinaryInt16(data[0:2])
	return v
}

func (d *jsonBinaryDecoder) decodeUint16(data []byte) uint16 {
	if d.isDataShort(data, 2) {
		return 0
	}

	v := mysql.ParseBinaryUint16(data[0:2])
	return v
}

func (d *jsonBinaryDecoder) decodeInt32(data []byte) int32 {
	if d.isDataShort(data, 4) {
		return 0
	}

	v := mysql.ParseBinaryInt32(data[0:4])
	return v
}

func (d *jsonBinaryDecoder) decodeUint32(data []byte) uint32 {
	if d.isDataShort(data, 4) {
		return 0
	}

	v := mysql.ParseBinaryUint32(data[0:4])
	return v
}

func (d *jsonBinaryDecoder) decodeInt64(data []byte) int64 {
	if d.isDataShort(data, 8) {
		return 0
	}

	v := mysql.ParseBinaryInt64(data[0:8])
	return v
}

func (d *jsonBinaryDecoder) decodeUint64(data []byte) uint64 {
	if d.isDataShort(data, 8) {
		return 0
	}

	v := mysql.ParseBinaryUint64(data[0:8])
	return v
}

func (d *jsonBinaryDecoder) decodeDouble(data []byte) float64 {
	if d.isDataShort(data, 8) {
		return 0
	}

	v := mysql.ParseBinaryFloat64(data[0:8])
	return v
}

func (d *jsonBinaryDecoder) decodeDoubleWithTrailingZero(data []byte) FloatWithTrailingZero {
	v := d.decodeDouble(data)
	return FloatWithTrailingZero(v)
}

func (d *jsonBinaryDecoder) decodeString(data []byte) string {
	if d.err != nil {
		return ""
	}

	l, n := d.decodeVariableLength(data)

	if d.isDataShort(data, l+n) {
		return ""
	}

	data = data[n:]

	v := utils.ByteSliceToString(data[0:l])
	return v
}

func (d *jsonBinaryDecoder) decodeOpaque(data []byte) any {
	if d.isDataShort(data, 1) {
		return nil
	}

	tp := data[0]
	data = data[1:]

	l, n := d.decodeVariableLength(data)

	if d.isDataShort(data, l+n) {
		return nil
	}

	data = data[n : l+n]

	switch tp {
	case mysql.MYSQL_TYPE_NEWDECIMAL:
		return d.decodeDecimal(data)
	case mysql.MYSQL_TYPE_TIME:
		return d.decodeTime(data)
	case mysql.MYSQL_TYPE_DATE, mysql.MYSQL_TYPE_DATETIME, mysql.MYSQL_TYPE_TIMESTAMP:
		return d.decodeDateTime(data)
	default:
		return utils.ByteSliceToString(data)
	}
}

func (d *jsonBinaryDecoder) decodeDecimal(data []byte) any {
	precision := int(data[0])
	scale := int(data[1])

	v, _, err := decodeDecimal(data[2:], precision, scale, d.useDecimal)
	d.err = err

	return v
}

func (d *jsonBinaryDecoder) decodeTime(data []byte) any {
	v := d.decodeInt64(data)

	if v == 0 {
		return "00:00:00"
	}

	sign := ""
	if v < 0 {
		sign = "-"
		v = -v
	}

	intPart := v >> 24
	hour := (intPart >> 12) % (1 << 10)
	minute := (intPart >> 6) % (1 << 6)
	sec := intPart % (1 << 6)
	frac := v % (1 << 24)

	return fmt.Sprintf("%s%02d:%02d:%02d.%06d", sign, hour, minute, sec, frac)
}

func (d *jsonBinaryDecoder) decodeDateTime(data []byte) any {
	v := d.decodeInt64(data)
	if v == 0 {
		return "0000-00-00 00:00:00"
	}

	// handle negative?
	if v < 0 {
		v = -v
	}

	intPart := v >> 24
	ymd := intPart >> 17
	ym := ymd >> 5
	hms := intPart % (1 << 17)

	year := ym / 13
	month := ym % 13
	day := ymd % (1 << 5)
	hour := hms >> 12
	minute := (hms >> 6) % (1 << 6)
	second := hms % (1 << 6)
	frac := v % (1 << 24)

	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d", year, month, day, hour, minute, second, frac)
}

func (d *jsonBinaryDecoder) decodeCount(data []byte, isSmall bool) int {
	if isSmall {
		v := d.decodeUint16(data)
		return int(v)
	}

	return int(d.decodeUint32(data))
}

func (d *jsonBinaryDecoder) decodeVariableLength(data []byte) (int, int) {
	// The max size for variable length is math.MaxUint32, so
	// here we can use 5 bytes to save it.
	maxCount := min(len(data), 5)

	pos := 0
	length := uint64(0)
	for ; pos < maxCount; pos++ {
		v := data[pos]
		length |= uint64(v&0x7F) << uint(7*pos)

		if v&0x80 == 0 {
			if length > math.MaxUint32 {
				d.err = errors.Errorf("variable length %d must <= %d", length, int64(math.MaxUint32))
				return 0, 0
			}

			pos++
			// TODO: should consider length overflow int here.
			return int(length), pos
		}
	}

	d.err = errors.New("decode variable length failed")

	return 0, 0
}

func (e *RowsEvent) decodeJSONPartialBinary(data []byte) (*JsonDiff, error) {
	// see Json_diff_vector::read_binary() in mysql-server/sql/json_diff.cc
	operationNumber := JsonDiffOperation(data[0])
	switch operationNumber {
	case JsonDiffOperationReplace:
	case JsonDiffOperationInsert:
	case JsonDiffOperationRemove:
	default:
		return nil, ErrCorruptedJSONDiff
	}
	data = data[1:]

	pathLength, _, n := mysql.LengthEncodedInt(data)
	data = data[n:]

	path := data[:pathLength]
	data = data[pathLength:]

	diff := &JsonDiff{
		Op:   operationNumber,
		Path: string(path),
		// Value will be filled below
	}

	if operationNumber == JsonDiffOperationRemove {
		return diff, nil
	}

	valueLength, _, n := mysql.LengthEncodedInt(data)
	data = data[n:]

	d, err := e.decodeJSONBinary(data[:valueLength])
	if err != nil {
		return nil, fmt.Errorf("cannot read json diff for field %q: %w", path, err)
	}
	diff.Value = string(d)

	return diff, nil
}
