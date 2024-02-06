/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package mysql

import (
	"math"
	"testing"

	"github.com/openark/golib/log"
	test "github.com/openark/golib/tests"
)

func init() {
	log.SetLevel(log.ERROR)
}

func TestBinlogCoordinates(t *testing.T) {
	c1 := BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 104}
	c2 := BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 104}
	c3 := BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 5000}
	c4 := BinlogCoordinates{LogFile: "mysql-bin.00112", LogPos: 104}

	test.S(t).ExpectTrue(c1.Equals(&c2))
	test.S(t).ExpectFalse(c1.Equals(&c3))
	test.S(t).ExpectFalse(c1.Equals(&c4))
	test.S(t).ExpectFalse(c1.SmallerThan(&c2))
	test.S(t).ExpectTrue(c1.SmallerThan(&c3))
	test.S(t).ExpectTrue(c1.SmallerThan(&c4))
	test.S(t).ExpectTrue(c3.SmallerThan(&c4))
	test.S(t).ExpectFalse(c3.SmallerThan(&c2))
	test.S(t).ExpectFalse(c4.SmallerThan(&c2))
	test.S(t).ExpectFalse(c4.SmallerThan(&c3))

	test.S(t).ExpectTrue(c1.SmallerThanOrEquals(&c2))
	test.S(t).ExpectTrue(c1.SmallerThanOrEquals(&c3))
}

func TestBinlogCoordinatesAsKey(t *testing.T) {
	m := make(map[BinlogCoordinates]bool)

	c1 := BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 104}
	c2 := BinlogCoordinates{LogFile: "mysql-bin.00022", LogPos: 104}
	c3 := BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 104}
	c4 := BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 222}

	m[c1] = true
	m[c2] = true
	m[c3] = true
	m[c4] = true

	test.S(t).ExpectEquals(len(m), 3)
}

func TestIsEndposOverflowBeyond4Bytes(t *testing.T) {
	{
		var preCoordinates *BinlogCoordinates
		curCoordinates := &BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 10321, EventSize: 1100}
		test.S(t).ExpectFalse(curCoordinates.IsLogPosOverflowBeyond4Bytes(preCoordinates))
	}
	{
		preCoordinates := &BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 1100, EventSize: 1100}
		curCoordinates := &BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: int64(uint32(preCoordinates.LogPos + 1100)), EventSize: 1100}
		test.S(t).ExpectFalse(curCoordinates.IsLogPosOverflowBeyond4Bytes(preCoordinates))
	}
	{
		preCoordinates := &BinlogCoordinates{LogFile: "mysql-bin.00016", LogPos: 1100, EventSize: 1100}
		curCoordinates := &BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: int64(uint32(preCoordinates.LogPos + 1100)), EventSize: 1100}
		test.S(t).ExpectFalse(curCoordinates.IsLogPosOverflowBeyond4Bytes(preCoordinates))
	}
	{
		preCoordinates := &BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: math.MaxUint32 - 1001, EventSize: 1000}
		curCoordinates := &BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: int64(uint32(preCoordinates.LogPos + 1000)), EventSize: 1000}
		test.S(t).ExpectFalse(curCoordinates.IsLogPosOverflowBeyond4Bytes(preCoordinates))
	}
	{
		preCoordinates := &BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: math.MaxUint32 - 1000, EventSize: 1000}
		curCoordinates := &BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: int64(uint32(preCoordinates.LogPos + 1000)), EventSize: 1000}
		test.S(t).ExpectFalse(curCoordinates.IsLogPosOverflowBeyond4Bytes(preCoordinates))
	}
	{
		preCoordinates := &BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: math.MaxUint32 - 999, EventSize: 1000}
		curCoordinates := &BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: int64(uint32(preCoordinates.LogPos + 1000)), EventSize: 1000}
		test.S(t).ExpectTrue(curCoordinates.IsLogPosOverflowBeyond4Bytes(preCoordinates))
	}
	{
		preCoordinates := &BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: int64(uint32(math.MaxUint32 - 500)), EventSize: 1000}
		curCoordinates := &BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: int64(uint32(preCoordinates.LogPos + 1000)), EventSize: 1000}
		test.S(t).ExpectTrue(curCoordinates.IsLogPosOverflowBeyond4Bytes(preCoordinates))
	}
	{
		preCoordinates := &BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: math.MaxUint32, EventSize: 1000}
		curCoordinates := &BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: int64(uint32(preCoordinates.LogPos + 1000)), EventSize: 1000}
		test.S(t).ExpectTrue(curCoordinates.IsLogPosOverflowBeyond4Bytes(preCoordinates))
	}
}
