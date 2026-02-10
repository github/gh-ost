/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package sql

import (
	"testing"

	"github.com/openark/golib/log"
	"github.com/stretchr/testify/require"
)

func init() {
	log.SetLevel(log.ERROR)
}

func TestParseColumnList(t *testing.T) {
	names := "id,category,max_len"

	columnList := ParseColumnList(names)
	require.Equal(t, 3, columnList.Len())
	require.Equal(t, []string{"id", "category", "max_len"}, columnList.Names())
	require.Equal(t, 0, columnList.Ordinals["id"])
	require.Equal(t, 1, columnList.Ordinals["category"])
	require.Equal(t, 2, columnList.Ordinals["max_len"])
}

func TestGetColumn(t *testing.T) {
	names := "id,category,max_len"
	columnList := ParseColumnList(names)
	{
		column := columnList.GetColumn("category")
		require.NotNil(t, column)
		require.Equal(t, column.Name, "category")
	}
	{
		column := columnList.GetColumn("no_such_column")
		require.Nil(t, column)
	}
}

func TestBinaryToString(t *testing.T) {
	id := []uint8{0x1b, 0x99}
	col := make([]interface{}, 1)
	col[0] = id
	cv := ToColumnValues(col)

	require.Equal(t, "1b99", cv.StringColumn(0))
}

func TestConvertArgCharsetDecoding(t *testing.T) {
	latin1Bytes := []uint8{0x47, 0x61, 0x72, 0xe7, 0x6f, 0x6e, 0x20, 0x21}

	col := Column{
		Charset: "latin1",
		charsetConversion: &CharacterSetConversion{
			FromCharset: "latin1",
			ToCharset:   "utf8mb4",
		},
	}

	// Should decode []uint8
	str := col.convertArg(latin1Bytes)
	require.Equal(t, "Garçon !", str)
}

func TestConvertArgBinaryColumnPadding(t *testing.T) {
	// Test that binary columns are padded with trailing zeros to their declared length.
	// This is needed because MySQL's binlog strips trailing 0x00 bytes from binary values.
	// See https://github.com/github/gh-ost/issues/909

	// Simulates a binary(20) column where binlog delivered only 18 bytes
	// (trailing zeros were stripped)
	truncatedValue := []uint8{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
		0x11, 0x12, // 18 bytes, missing 2 trailing zeros
	}

	col := Column{
		Name:              "bin_col",
		Type:              BinaryColumnType,
		BinaryOctetLength: 20,
	}

	result := col.convertArg(truncatedValue)
	resultBytes := result.([]byte)

	require.Equal(t, 20, len(resultBytes), "binary column should be padded to declared length")
	// First 18 bytes should be unchanged
	require.Equal(t, truncatedValue, resultBytes[:18])
	// Last 2 bytes should be zeros
	require.Equal(t, []byte{0x00, 0x00}, resultBytes[18:])
}

func TestConvertArgBinaryColumnNoPaddingWhenFull(t *testing.T) {
	// When binary value is already at full length, no padding should occur
	fullValue := []uint8{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
		0x11, 0x12, 0x13, 0x14, // 20 bytes
	}

	col := Column{
		Name:              "bin_col",
		Type:              BinaryColumnType,
		BinaryOctetLength: 20,
	}

	result := col.convertArg(fullValue)
	resultBytes := result.([]byte)

	require.Equal(t, 20, len(resultBytes))
	require.Equal(t, fullValue, resultBytes)
}
