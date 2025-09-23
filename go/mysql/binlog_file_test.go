/*
   Copyright 2022 GitHub Inc.
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
	c1 := FileBinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 104}
	c2 := FileBinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 104}
	c3 := FileBinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 5000}
	c4 := FileBinlogCoordinates{LogFile: "mysql-bin.00112", LogPos: 104}

	gtidSet1, _ := gomysql.ParseMysqlGTIDSet("3E11FA47-71CA-11E1-9E33-C80AA9429562:23")
	gtidSet2, _ := gomysql.ParseMysqlGTIDSet("3E11FA47-71CA-11E1-9E33-C80AA9429562:100")
	gtidSet3, _ := gomysql.ParseMysqlGTIDSet("7F80FA47-FF33-71A1-AE01-B80CC7823548:100")
	gtidSetBig1, _ := gomysql.ParseMysqlGTIDSet(`08dc06d7-c27c-11ea-b204-e4434b77a5ce:1-1497873603,
0b4ff540-a712-11ea-9857-e4434b2a1c98:1-4315312982,
19636248-246d-11e9-ab0d-0263df733a8e:1,
1c8cd5dd-8c79-11eb-ae94-e4434b27ee9c:1-18850436,
3342d1ad-bda0-11ea-ba96-e4434b28e6e0:1-475232304,
3bcd300c-c811-11e9-9970-e4434b714c24:1-6209943929,
418b92ed-d6f6-11e8-b18f-246e961e5ed0:1-3299395227,
4465ebe1-2bcc-11e9-8913-e4434b21c560:1-4724945648,
48e2bc1d-d66d-11e8-bf56-a0369f9437b8:1,
492e2980-4518-11e9-92c6-e4434b3eca94:1-4926754392`)
	gtidSetBig2, _ := gomysql.ParseMysqlGTIDSet(`08dc06d7-c27c-11ea-b204-e4434b77a5ce:1-1497873603,
0b4ff540-a712-11ea-9857-e4434b2a1c98:1-4315312982,
19636248-246d-11e9-ab0d-0263df733a8e:1,
1c8cd5dd-8c79-11eb-ae94-e4434b27ee9c:1-18850436,
3342d1ad-bda0-11ea-ba96-e4434b28e6e0:1-475232304,
3bcd300c-c811-11e9-9970-e4434b714c24:1-6209943929,
418b92ed-d6f6-11e8-b18f-246e961e5ed0:1-3299395227,
4465ebe1-2bcc-11e9-8913-e4434b21c560:1-4724945648,
48e2bc1d-d66d-11e8-bf56-a0369f9437b8:1,
492e2980-4518-11e9-92c6-e4434b3eca94:1-4926754399`)

	c5 := GTIDBinlogCoordinates{GTIDSet: gtidSet1.(*gomysql.MysqlGTIDSet)}
	c6 := GTIDBinlogCoordinates{GTIDSet: gtidSet1.(*gomysql.MysqlGTIDSet)}
	c7 := GTIDBinlogCoordinates{GTIDSet: gtidSet2.(*gomysql.MysqlGTIDSet)}
	c8 := GTIDBinlogCoordinates{GTIDSet: gtidSet3.(*gomysql.MysqlGTIDSet)}
	c9 := GTIDBinlogCoordinates{GTIDSet: gtidSetBig1.(*gomysql.MysqlGTIDSet)}
	c10 := GTIDBinlogCoordinates{GTIDSet: gtidSetBig2.(*gomysql.MysqlGTIDSet)}

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
	require.True(t, c7.SmallerThanOrEquals(&c8))
	require.True(t, c9.SmallerThanOrEquals(&c9))
	require.True(t, c9.SmallerThanOrEquals(&c10))
}

func TestBinlogCoordinatesAsKey(t *testing.T) {
	m := make(map[BinlogCoordinates]bool)

	c1 := &FileBinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 104}
	c2 := &FileBinlogCoordinates{LogFile: "mysql-bin.00022", LogPos: 104}
	c3 := &FileBinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 104}
	c4 := &FileBinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 222}

	m[c1] = true
	m[c2] = true
	m[c3] = true
	m[c4] = true

	require.Len(t, m, 4)
}

func TestIsLogPosOverflowBeyond4Bytes(t *testing.T) {
	{
		var preCoordinates *FileBinlogCoordinates
		curCoordinates := &FileBinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 10321, EventSize: 1100}
		require.False(t, curCoordinates.IsLogPosOverflowBeyond4Bytes(preCoordinates))
	}
	{
		preCoordinates := &FileBinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 1100, EventSize: 1100}
		curCoordinates := &FileBinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: int64(uint32(preCoordinates.LogPos + 1100)), EventSize: 1100}
		require.False(t, curCoordinates.IsLogPosOverflowBeyond4Bytes(preCoordinates))
	}
	{
		preCoordinates := &FileBinlogCoordinates{LogFile: "mysql-bin.00016", LogPos: 1100, EventSize: 1100}
		curCoordinates := &FileBinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: int64(uint32(preCoordinates.LogPos + 1100)), EventSize: 1100}
		require.False(t, curCoordinates.IsLogPosOverflowBeyond4Bytes(preCoordinates))
	}
	{
		preCoordinates := &FileBinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: math.MaxUint32 - 1001, EventSize: 1000}
		curCoordinates := &FileBinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: int64(uint32(preCoordinates.LogPos + 1000)), EventSize: 1000}
		require.False(t, curCoordinates.IsLogPosOverflowBeyond4Bytes(preCoordinates))
	}
	{
		preCoordinates := &FileBinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: math.MaxUint32 - 1000, EventSize: 1000}
		curCoordinates := &FileBinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: int64(uint32(preCoordinates.LogPos + 1000)), EventSize: 1000}
		require.False(t, curCoordinates.IsLogPosOverflowBeyond4Bytes(preCoordinates))
	}
	{
		preCoordinates := &FileBinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: math.MaxUint32 - 999, EventSize: 1000}
		curCoordinates := &FileBinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: int64(uint32(preCoordinates.LogPos + 1000)), EventSize: 1000}
		require.True(t, curCoordinates.IsLogPosOverflowBeyond4Bytes(preCoordinates))
	}
	{
		preCoordinates := &FileBinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: int64(uint32(math.MaxUint32 - 500)), EventSize: 1000}
		curCoordinates := &FileBinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: int64(uint32(preCoordinates.LogPos + 1000)), EventSize: 1000}
		require.True(t, curCoordinates.IsLogPosOverflowBeyond4Bytes(preCoordinates))
	}
	{
		preCoordinates := &FileBinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: math.MaxUint32, EventSize: 1000}
		curCoordinates := &FileBinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: int64(uint32(preCoordinates.LogPos + 1000)), EventSize: 1000}
		require.True(t, curCoordinates.IsLogPosOverflowBeyond4Bytes(preCoordinates))
	}
}
