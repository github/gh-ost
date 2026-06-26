/*
   Copyright 2025 GitHub Inc.
         See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/github/gh-ost/go/sql"
)

// TestSerializeRangeValues covers the table-agnostic, hex-per-value encoding used
// to store a move-table's unique-key range in the single checkpoint table.
func TestSerializeRangeValues(t *testing.T) {
	// nil ColumnValues serializes to the empty string.
	require.Equal(t, "", serializeRangeValues(nil))

	// A single integer key: hex of its decimal text ("172" -> 31 37 32).
	require.Equal(t, "313732", serializeRangeValues(sql.ToColumnValues([]interface{}{172})))

	// A varchar key: hex of the UTF-8 bytes ("code_8" -> 63 6f 64 65 5f 38).
	require.Equal(t, "636f64655f38", serializeRangeValues(sql.ToColumnValues([]interface{}{"code_8"})))

	// A compound key of heterogeneous types is comma-joined.
	require.Equal(t, "3235,636f64655f38",
		serializeRangeValues(sql.ToColumnValues([]interface{}{25, "code_8"})))

	// Raw bytes are hex-encoded as-is.
	require.Equal(t, "e590",
		serializeRangeValues(sql.ToColumnValues([]interface{}{[]byte{0xe5, 0x90}})))

	// A nil column value is encoded with the unambiguous NULL token.
	require.Equal(t, moveTableCheckpointNullToken,
		serializeRangeValues(sql.ToColumnValues([]interface{}{nil})))
}

// TestDeserializeRangeValuesRoundTrip verifies the encode->store->decode cycle.
// Values come back as []byte (accepted directly as prepared-statement args), so
// the round trip is checked on the serialized (canonical) form, which is what a
// resumed run actually compares.
func TestDeserializeRangeValuesRoundTrip(t *testing.T) {
	orig := sql.ToColumnValues([]interface{}{172, "code_8"})
	s := serializeRangeValues(orig)

	got := deserializeRangeValues(s, 2)
	require.Equal(t, s, serializeRangeValues(got), "re-serializing the decoded range must reproduce the stored text")

	vals := got.AbstractValues()
	require.Len(t, vals, 2)
	require.Equal(t, []byte("172"), vals[0])
	require.Equal(t, []byte("code_8"), vals[1])
}

// TestDeserializeRangeValuesNullToken verifies the NULL marker decodes back to a
// nil column value while other columns decode normally.
func TestDeserializeRangeValuesNullToken(t *testing.T) {
	got := deserializeRangeValues(moveTableCheckpointNullToken+",3235", 2)
	vals := got.AbstractValues()
	require.Len(t, vals, 2)
	require.Nil(t, vals[0])
	require.Equal(t, []byte("25"), vals[1])
}

// TestIsEmptyRange verifies the predicate that tells a resumed run a table had no
// completed chunk yet (so it must restart from the table minimum).
func TestIsEmptyRange(t *testing.T) {
	require.True(t, isEmptyRange(nil), "nil range is empty")
	require.True(t, isEmptyRange(sql.NewColumnValues(0)), "zero-column range is empty")
	require.True(t, isEmptyRange(deserializeRangeValues(moveTableCheckpointNullToken, 1)), "all-nil range is empty")
	require.False(t, isEmptyRange(sql.ToColumnValues([]interface{}{1})), "a range with a value is not empty")
}
