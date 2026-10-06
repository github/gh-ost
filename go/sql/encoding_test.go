/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package sql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCharsetEncodingMap(t *testing.T) {
	testCases := []struct {
		name    string
		charset string
		encoded []byte
		decoded string
	}{
		{name: "Big5", charset: "big5", encoded: []byte{0xa4, 0xa4, 0xa4, 0xe5}, decoded: "中文"},
		{name: "Shift-JIS", charset: "sjis", encoded: []byte{0x93, 0xfa, 0x96, 0x7b}, decoded: "日本"},
		{name: "EUC-JP", charset: "ujis", encoded: []byte{0xc6, 0xfc, 0xcb, 0xdc}, decoded: "日本"},
		{name: "EUC-KR", charset: "euckr", encoded: []byte{0xc7, 0xd1, 0xb1, 0xb9}, decoded: "한국"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			charset, ok := charsetEncodingMap[testCase.charset]
			require.True(t, ok)

			decoded, err := charset.NewDecoder().Bytes(testCase.encoded)
			require.NoError(t, err)
			require.Equal(t, testCase.decoded, string(decoded))
		})
	}
}
