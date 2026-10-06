package replication

import (
	"bytes"
	"math"
	"strconv"

	"github.com/goccy/go-json"
)

// This file holds the MySQL-text marshalers used by jsonBinaryDecoder
// when its mysqlTextMode flag is set. Wrapping the leaf decode returns
// in these types lets the existing json.Marshal pass produce JSON text
// that is faithful to each JSONB value's original type tag where the
// JSON text grammar can express it (DOUBLE 1.0 stays "1.0"; NEWDECIMAL
// stays unquoted; etc.) and preserves the JSONB key order.
//
// Caveats:
//   - The output is type-faithful, not byte-identical to MySQL's
//     "SELECT json_col" form. Inter-token whitespace is compact (no
//     space after ',' or ':') and floating-point text differs in some
//     exponent/precision corner cases (see jsonMySQLDouble).
//   - NEWDECIMAL is the one tag that cannot be preserved on text
//     round-trip: MySQL's JSON text grammar has no decimal literal, so
//     re-inserting the unquoted number yields a JSON DOUBLE, not the
//     original JSONB_OPAQUE NEWDECIMAL. The numeric value still
//     round-trips; only the opaque type tag is lost. All other tags
//     covered here do reproduce the original JSONB binary on re-insert.

// jsonString carries a JSONB string payload as raw bytes so MarshalJSON
// can pass non-ASCII bytes through verbatim. MySQL JSON is byte-
// transparent, so bytes >= 0x20 (other than '"' and '\\') are written
// without UTF-8 validation -- unlike the default encoding/json path which
// replaces invalid UTF-8 with U+FFFD.
type jsonString string

func (s jsonString) MarshalJSON() ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, len(s)+2))
	buf.WriteByte('"')
	writeJSONString(buf, []byte(s))
	buf.WriteByte('"')
	return buf.Bytes(), nil
}

// jsonRawNumber emits its bytes unquoted. Used for JSONB OPAQUE
// NEWDECIMAL values: MySQL renders these as plain numbers in JSON text,
// not as quoted strings.
type jsonRawNumber string

func (n jsonRawNumber) MarshalJSON() ([]byte, error) {
	return []byte(n), nil
}

// jsonMySQLDouble formats a float64 close to the way MySQL does in JSON
// text: whole-number doubles keep a trailing ".0" so MySQL re-stores
// them as JSON DOUBLE rather than JSON INTEGER, and non-integer values
// use the shortest round-trippable form. We can't reuse
// FloatWithTrailingZero here because it formats non-integers with 'f'
// (always plain decimal); MySQL uses scientific notation for some
// magnitudes, which 'g' matches more closely.
//
// The output is NOT guaranteed to be byte-identical to MySQL's
// my_gcvt-formatted text: Go's 'g' verb emits exponents as e.g.
// "1.5e-05" where MySQL emits "1.5e-5", and the integer/scientific
// crossover threshold differs. Binary round-trip through a MySQL JSON
// column is unaffected (the same float64 produces the same JSONB
// DOUBLE bytes); only the visible text form may differ.
type jsonMySQLDouble float64

func (f jsonMySQLDouble) MarshalJSON() ([]byte, error) {
	return []byte(formatMySQLDouble(float64(f))), nil
}

// jsonObject preserves JSONB key order (length-then-bytes, which is what
// MySQL emits) instead of going through map[string]any, which json.Marshal
// would sort lexicographically.
type jsonObject struct {
	keys   []string
	values []any
}

func (o jsonObject) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, k := range o.keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteByte('"')
		writeJSONString(&buf, []byte(k))
		buf.WriteString(`":`)
		vb, err := json.Marshal(o.values[i])
		if err != nil {
			return nil, err
		}
		buf.Write(vb)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

func formatMySQLDouble(f float64) string {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		// MySQL refuses to store NaN/Inf in JSON; emit a safe fallback
		// rather than corrupt the surrounding document.
		return "null"
	}
	if f == math.Trunc(f) {
		return strconv.FormatFloat(f, 'f', 1, 64)
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// writeJSONString writes s as the contents of a JSON string (no
// surrounding quotes), with byte-transparent semantics: bytes >= 0x20
// other than '"' and '\\' are written verbatim, including high-bit bytes
// that may not form valid UTF-8.
func writeJSONString(buf *bytes.Buffer, s []byte) {
	const hexdigits = "0123456789abcdef"
	for i := range len(s) {
		c := s[i]
		if c < 0x20 || c == '"' || c == '\\' {
			switch c {
			case '"':
				buf.WriteString(`\"`)
			case '\\':
				buf.WriteString(`\\`)
			case '\b':
				buf.WriteString(`\b`)
			case '\f':
				buf.WriteString(`\f`)
			case '\n':
				buf.WriteString(`\n`)
			case '\r':
				buf.WriteString(`\r`)
			case '\t':
				buf.WriteString(`\t`)
			default:
				buf.WriteString(`\u00`)
				buf.WriteByte(hexdigits[c>>4])
				buf.WriteByte(hexdigits[c&0xF])
			}
			continue
		}
		buf.WriteByte(c)
	}
}
