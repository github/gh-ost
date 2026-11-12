/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package sql

import (
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/encoding/korean"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/encoding/traditionalchinese"
)

type charsetEncoding map[string]encoding.Encoding

var charsetEncodingMap charsetEncoding

func init() {
	charsetEncodingMap = make(map[string]encoding.Encoding)
	// Begin mappings
	charsetEncodingMap["latin1"] = charmap.Windows1252
	charsetEncodingMap["gbk"] = simplifiedchinese.GBK
	charsetEncodingMap["big5"] = traditionalchinese.Big5
	charsetEncodingMap["sjis"] = japanese.ShiftJIS
	charsetEncodingMap["ujis"] = japanese.EUCJP
	charsetEncodingMap["euckr"] = korean.EUCKR
}
