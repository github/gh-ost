/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package mysql

import (
	"math"
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/openark/golib/log"
	"github.com/stretchr/testify/require"
)

func init() {
	log.SetLevel(log.ERROR)
}

func TestBinlogCoordinates(t *testing.T) {
	c1 := BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 104}
	c2 := BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 104}
	c3 := BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 5000}
	c4 := BinlogCoordinates{LogFile: "mysql-bin.00112", LogPos: 104}

	gtidSet1, _ := gomysql.ParseMysqlGTIDSet("3E11FA47-71CA-11E1-9E33-C80AA9429562:23")
	//gtidSet2, _ := gomysql.ParseMysqlGTIDSet("3E11FA47-71CA-11E1-9E33-C80AA9429562:100")
	c5 := BinlogCoordinates{GTIDSet: gtidSet1}
	c6 := BinlogCoordinates{GTIDSet: gtidSet1}
	//c7 := BinlogCoordinates{GTIDSet: gtidSet2}

	require.True(t, c5.Equals(&c6))
	require.True(t, c1.Equals(&c2))
	require.False(t, c1.Equals(&c3))
	require.False(t, c1.Equals(&c4))
	require.False(t, c1.SmallerThan(&c2))
	require.True(t, c1.SmallerThan(&c3))
	require.True(t, c1.SmallerThan(&c4))
	require.True(t, c3.SmallerThan(&c4))
	require.False(t, c3.SmallerThan(&c2))
	require.False(t, c4.SmallerThan(&c2))
	require.False(t, c4.SmallerThan(&c3))
	require.True(t, c1.SmallerThanOrEquals(&c2))
	require.True(t, c1.SmallerThanOrEquals(&c3))
	require.True(t, c1.SmallerThanOrEquals(&c2))
	require.True(t, c1.SmallerThanOrEquals(&c3))
	require.True(t, c6.SmallerThanOrEquals(&c7))
}

func TestBinlogNext(t *testing.T) {
	c1 := BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 104}
	cres, err := c1.NextFileCoordinates()

	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(c1.Type, cres.Type)
	test.S(t).ExpectEquals(cres.LogFile, "mysql-bin.00018")

	c2 := BinlogCoordinates{LogFile: "mysql-bin.00099", LogPos: 104}
	cres, err = c2.NextFileCoordinates()

	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(c1.Type, cres.Type)
	test.S(t).ExpectEquals(cres.LogFile, "mysql-bin.00100")

	c3 := BinlogCoordinates{LogFile: "mysql.00.prod.com.00099", LogPos: 104}
	cres, err = c3.NextFileCoordinates()

	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(c1.Type, cres.Type)
	test.S(t).ExpectEquals(cres.LogFile, "mysql.00.prod.com.00100")
}

func TestBinlogPrevious(t *testing.T) {
	c1 := BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 104}
	cres, err := c1.PreviousFileCoordinates()

	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(c1.Type, cres.Type)
	test.S(t).ExpectEquals(cres.LogFile, "mysql-bin.00016")

	c2 := BinlogCoordinates{LogFile: "mysql-bin.00100", LogPos: 104}
	cres, err = c2.PreviousFileCoordinates()

	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(c1.Type, cres.Type)
	test.S(t).ExpectEquals(cres.LogFile, "mysql-bin.00099")

	c3 := BinlogCoordinates{LogFile: "mysql.00.prod.com.00100", LogPos: 104}
	cres, err = c3.PreviousFileCoordinates()

	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(c1.Type, cres.Type)
	test.S(t).ExpectEquals(cres.LogFile, "mysql.00.prod.com.00099")

	c4 := BinlogCoordinates{LogFile: "mysql.00.prod.com.00000", LogPos: 104}
	_, err = c4.PreviousFileCoordinates()

	test.S(t).ExpectNotNil(err)
>>>>>>> 967ced57 (Comment-out WIP test)
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

	require.Len(t, m, 3)
}

func TestIsLogPosOverflowBeyond4Bytes(t *testing.T) {
	{
		var preCoordinates *BinlogCoordinates
		curCoordinates := &BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 10321, EventSize: 1100}
		require.False(t, curCoordinates.IsLogPosOverflowBeyond4Bytes(preCoordinates))
	}
	{
		preCoordinates := &BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 1100, EventSize: 1100}
		curCoordinates := &BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: int64(uint32(preCoordinates.LogPos + 1100)), EventSize: 1100}
		require.False(t, curCoordinates.IsLogPosOverflowBeyond4Bytes(preCoordinates))
	}
	{
		preCoordinates := &BinlogCoordinates{LogFile: "mysql-bin.00016", LogPos: 1100, EventSize: 1100}
		curCoordinates := &BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: int64(uint32(preCoordinates.LogPos + 1100)), EventSize: 1100}
		require.False(t, curCoordinates.IsLogPosOverflowBeyond4Bytes(preCoordinates))
	}
	{
		preCoordinates := &BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: math.MaxUint32 - 1001, EventSize: 1000}
		curCoordinates := &BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: int64(uint32(preCoordinates.LogPos + 1000)), EventSize: 1000}
		require.False(t, curCoordinates.IsLogPosOverflowBeyond4Bytes(preCoordinates))
	}
	{
		preCoordinates := &BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: math.MaxUint32 - 1000, EventSize: 1000}
		curCoordinates := &BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: int64(uint32(preCoordinates.LogPos + 1000)), EventSize: 1000}
		require.False(t, curCoordinates.IsLogPosOverflowBeyond4Bytes(preCoordinates))
	}
	{
		preCoordinates := &BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: math.MaxUint32 - 999, EventSize: 1000}
		curCoordinates := &BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: int64(uint32(preCoordinates.LogPos + 1000)), EventSize: 1000}
		require.True(t, curCoordinates.IsLogPosOverflowBeyond4Bytes(preCoordinates))
	}
	{
		preCoordinates := &BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: int64(uint32(math.MaxUint32 - 500)), EventSize: 1000}
		curCoordinates := &BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: int64(uint32(preCoordinates.LogPos + 1000)), EventSize: 1000}
		require.True(t, curCoordinates.IsLogPosOverflowBeyond4Bytes(preCoordinates))
	}
	{
		preCoordinates := &BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: math.MaxUint32, EventSize: 1000}
		curCoordinates := &BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: int64(uint32(preCoordinates.LogPos + 1000)), EventSize: 1000}
		require.True(t, curCoordinates.IsLogPosOverflowBeyond4Bytes(preCoordinates))
	}
}
