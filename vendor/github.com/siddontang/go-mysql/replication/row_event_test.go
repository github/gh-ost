package replication

import (
	"fmt"
	"strconv"

	. "github.com/pingcap/check"
	"github.com/shopspring/decimal"
)

type testDecodeSuite struct{}

var _ = Suite(&testDecodeSuite{})

type decodeDecimalChecker struct {
	*CheckerInfo
}

func (_ *decodeDecimalChecker) Check(params []interface{}, names []string) (bool, string) {
	var test int
	val := struct {
		Value  decimal.Decimal
		Pos    int
		Err    error
		EValue decimal.Decimal
		EPos   int
		EErr   error
	}{}

	for i, name := range names {
		switch name {
		case "obtainedValue":
			val.Value, _ = params[i].(decimal.Decimal)
		case "obtainedPos":
			val.Pos, _ = params[i].(int)
		case "obtainedErr":
			val.Err, _ = params[i].(error)
		case "expectedValue":
			val.EValue, _ = params[i].(decimal.Decimal)
		case "expectedPos":
			val.EPos, _ = params[i].(int)
		case "expectedErr":
			val.EErr, _ = params[i].(error)
		case "caseNumber":
			test = params[i].(int)
		}
	}
	errorMsgFmt := fmt.Sprintf("For Test %v: ", test) + "Did not get expected %v(%v), got %v instead."
	if val.Err != val.EErr {
		return false, fmt.Sprintf(errorMsgFmt, "error", val.EErr, val.Err)
	}
	if val.Pos != val.EPos {
		return false, fmt.Sprintf(errorMsgFmt, "position", val.EPos, val.Pos)
	}
	if !val.Value.Equal(val.EValue) {
		return false, fmt.Sprintf(errorMsgFmt, "value", val.EValue, val.Value)
	}
	return true, ""
}

var DecodeDecimalsEquals = &decodeDecimalChecker{
	&CheckerInfo{Name: "Equals", Params: []string{"obtainedValue", "obtainedPos", "obtainedErr", "expectedValue", "expectedPos", "expectedErr", "caseNumber"}},
}

func (_ *testDecodeSuite) TestDecodeDecimal(c *C) {
	// _PLACEHOLDER_ := 0
	testcases := []struct {
		Data        []byte
		Precision   int
		Decimals    int
		Expected    string
		ExpectedPos int
		ExpectedErr error
	}{
		// These are cases from the mysql test cases
		/*
			-- Generated with gentestsql.go --
			DROP TABLE IF EXISTS decodedecimal;
			CREATE TABLE decodedecimal (
			    id     int(11) not null auto_increment,
			    v4_2 decimal(4,2),
			    v5_0 decimal(5,0),
			    v7_3 decimal(7,3),
			    v10_2 decimal(10,2),
			    v10_3 decimal(10,3),
			    v13_2 decimal(13,2),
			    v15_14 decimal(15,14),
			    v20_10 decimal(20,10),
			    v30_5 decimal(30,5),
			    v30_20 decimal(30,20),
			    v30_25 decimal(30,25),
			    prec   int(11),
			    scale  int(11),
			    PRIMARY KEY(id)
			) engine=InnoDB;
			INSERT INTO decodedecimal (v4_2,v5_0,v7_3,v10_2,v10_3,v13_2,v15_14,v20_10,v30_5,v30_20,v30_25,prec,scale) VALUES
			("-10.55","-10.55","-10.55","-10.55","-10.55","-10.55","-10.55","-10.55","-10.55","-10.55","-10.55",4,2),
			("0.0123456789012345678912345","0.0123456789012345678912345","0.0123456789012345678912345","0.0123456789012345678912345","0.0123456789012345678912345","0.0123456789012345678912345","0.0123456789012345678912345","0.0123456789012345678912345","0.0123456789012345678912345","0.0123456789012345678912345","0.0123456789012345678912345",30,25),
			("12345","12345","12345","12345","12345","12345","12345","12345","12345","12345","12345",5,0),
			("12345","12345","12345","12345","12345","12345","12345","12345","12345","12345","12345",10,3),
			("123.45","123.45","123.45","123.45","123.45","123.45","123.45","123.45","123.45","123.45","123.45",10,3),
			("-123.45","-123.45","-123.45","-123.45","-123.45","-123.45","-123.45","-123.45","-123.45","-123.45","-123.45",20,10),
			(".00012345000098765",".00012345000098765",".00012345000098765",".00012345000098765",".00012345000098765",".00012345000098765",".00012345000098765",".00012345000098765",".00012345000098765",".00012345000098765",".00012345000098765",15,14),
			(".00012345000098765",".00012345000098765",".00012345000098765",".00012345000098765",".00012345000098765",".00012345000098765",".00012345000098765",".00012345000098765",".00012345000098765",".00012345000098765",".00012345000098765",22,20),
			(".12345000098765",".12345000098765",".12345000098765",".12345000098765",".12345000098765",".12345000098765",".12345000098765",".12345000098765",".12345000098765",".12345000098765",".12345000098765",30,20),
			("-.000000012345000098765","-.000000012345000098765","-.000000012345000098765","-.000000012345000098765","-.000000012345000098765","-.000000012345000098765","-.000000012345000098765","-.000000012345000098765","-.000000012345000098765","-.000000012345000098765","-.000000012345000098765",30,20),
			("1234500009876.5","1234500009876.5","1234500009876.5","1234500009876.5","1234500009876.5","1234500009876.5","1234500009876.5","1234500009876.5","1234500009876.5","1234500009876.5","1234500009876.5",30,5),
			("111111111.11","111111111.11","111111111.11","111111111.11","111111111.11","111111111.11","111111111.11","111111111.11","111111111.11","111111111.11","111111111.11",10,2),
			("000000000.01","000000000.01","000000000.01","000000000.01","000000000.01","000000000.01","000000000.01","000000000.01","000000000.01","000000000.01","000000000.01",7,3),
			("123.4","123.4","123.4","123.4","123.4","123.4","123.4","123.4","123.4","123.4","123.4",10,2),
			("-562.58","-562.58","-562.58","-562.58","-562.58","-562.58","-562.58","-562.58","-562.58","-562.58","-562.58",13,2),
			("-3699.01","-3699.01","-3699.01","-3699.01","-3699.01","-3699.01","-3699.01","-3699.01","-3699.01","-3699.01","-3699.01",13,2),
			("-1948.14","-1948.14","-1948.14","-1948.14","-1948.14","-1948.14","-1948.14","-1948.14","-1948.14","-1948.14","-1948.14",13,2)
			;
			select * from decodedecimal;
			+----+--------+-------+-----------+-------------+-------------+----------------+-------------------+-----------------------+---------------------+---------------------------------+---------------------------------+------+-------+
			| id | v4_2   | v5_0  | v7_3      | v10_2       | v10_3       | v13_2          | v15_14            | v20_10                | v30_5               | v30_20                          | v30_25                          | prec | scale |
			+----+--------+-------+-----------+-------------+-------------+----------------+-------------------+-----------------------+---------------------+---------------------------------+---------------------------------+------+-------+
			|  1 | -10.55 |   -11 |   -10.550 |      -10.55 |     -10.550 |         -10.55 | -9.99999999999999 |        -10.5500000000 |           -10.55000 |        -10.55000000000000000000 |   -10.5500000000000000000000000 |    4 |     2 |
			|  2 |   0.01 |     0 |     0.012 |        0.01 |       0.012 |           0.01 |  0.01234567890123 |          0.0123456789 |             0.01235 |          0.01234567890123456789 |     0.0123456789012345678912345 |   30 |    25 |
			|  3 |  99.99 | 12345 |  9999.999 |    12345.00 |   12345.000 |       12345.00 |  9.99999999999999 |      12345.0000000000 |         12345.00000 |      12345.00000000000000000000 | 12345.0000000000000000000000000 |    5 |     0 |
			|  4 |  99.99 | 12345 |  9999.999 |    12345.00 |   12345.000 |       12345.00 |  9.99999999999999 |      12345.0000000000 |         12345.00000 |      12345.00000000000000000000 | 12345.0000000000000000000000000 |   10 |     3 |
			|  5 |  99.99 |   123 |   123.450 |      123.45 |     123.450 |         123.45 |  9.99999999999999 |        123.4500000000 |           123.45000 |        123.45000000000000000000 |   123.4500000000000000000000000 |   10 |     3 |
			|  6 | -99.99 |  -123 |  -123.450 |     -123.45 |    -123.450 |        -123.45 | -9.99999999999999 |       -123.4500000000 |          -123.45000 |       -123.45000000000000000000 |  -123.4500000000000000000000000 |   20 |    10 |
			|  7 |   0.00 |     0 |     0.000 |        0.00 |       0.000 |           0.00 |  0.00012345000099 |          0.0001234500 |             0.00012 |          0.00012345000098765000 |     0.0001234500009876500000000 |   15 |    14 |
			|  8 |   0.00 |     0 |     0.000 |        0.00 |       0.000 |           0.00 |  0.00012345000099 |          0.0001234500 |             0.00012 |          0.00012345000098765000 |     0.0001234500009876500000000 |   22 |    20 |
			|  9 |   0.12 |     0 |     0.123 |        0.12 |       0.123 |           0.12 |  0.12345000098765 |          0.1234500010 |             0.12345 |          0.12345000098765000000 |     0.1234500009876500000000000 |   30 |    20 |
			| 10 |   0.00 |     0 |     0.000 |        0.00 |       0.000 |           0.00 | -0.00000001234500 |         -0.0000000123 |             0.00000 |         -0.00000001234500009877 |    -0.0000000123450000987650000 |   30 |    20 |
			| 11 |  99.99 | 99999 |  9999.999 | 99999999.99 | 9999999.999 | 99999999999.99 |  9.99999999999999 | 9999999999.9999999999 | 1234500009876.50000 | 9999999999.99999999999999999999 | 99999.9999999999999999999999999 |   30 |     5 |
			| 12 |  99.99 | 99999 |  9999.999 | 99999999.99 | 9999999.999 |   111111111.11 |  9.99999999999999 |  111111111.1100000000 |     111111111.11000 |  111111111.11000000000000000000 | 99999.9999999999999999999999999 |   10 |     2 |
			| 13 |   0.01 |     0 |     0.010 |        0.01 |       0.010 |           0.01 |  0.01000000000000 |          0.0100000000 |             0.01000 |          0.01000000000000000000 |     0.0100000000000000000000000 |    7 |     3 |
			| 14 |  99.99 |   123 |   123.400 |      123.40 |     123.400 |         123.40 |  9.99999999999999 |        123.4000000000 |           123.40000 |        123.40000000000000000000 |   123.4000000000000000000000000 |   10 |     2 |
			| 15 | -99.99 |  -563 |  -562.580 |     -562.58 |    -562.580 |        -562.58 | -9.99999999999999 |       -562.5800000000 |          -562.58000 |       -562.58000000000000000000 |  -562.5800000000000000000000000 |   13 |     2 |
			| 16 | -99.99 | -3699 | -3699.010 |    -3699.01 |   -3699.010 |       -3699.01 | -9.99999999999999 |      -3699.0100000000 |         -3699.01000 |      -3699.01000000000000000000 | -3699.0100000000000000000000000 |   13 |     2 |
			| 17 | -99.99 | -1948 | -1948.140 |    -1948.14 |   -1948.140 |       -1948.14 | -9.99999999999999 |      -1948.1400000000 |         -1948.14000 |      -1948.14000000000000000000 | -1948.1400000000000000000000000 |   13 |     2 |
			+----+--------+-------+-----------+-------------+-------------+----------------+-------------------+-----------------------+---------------------+---------------------------------+---------------------------------+------+-------+
		*/
		{[]byte{117, 200, 127, 255}, 4, 2, "-10.55", 2, nil},
		{[]byte{127, 255, 244, 127, 245}, 5, 0, "-11", 3, nil},
		{[]byte{127, 245, 253, 217, 127, 255}, 7, 3, "-10.550", 4, nil},
		{[]byte{127, 255, 255, 245, 200, 127, 255}, 10, 2, "-10.55", 5, nil},
		{[]byte{127, 255, 255, 245, 253, 217, 127, 255}, 10, 3, "-10.550", 6, nil},
		{[]byte{127, 255, 255, 255, 245, 200, 118, 196}, 13, 2, "-10.55", 6, nil},
		{[]byte{118, 196, 101, 54, 0, 254, 121, 96, 127, 255}, 15, 14, "-9.99999999999999", 8, nil},
		{[]byte{127, 255, 255, 255, 245, 223, 55, 170, 127, 255, 127, 255}, 20, 10, "-10.5500000000", 10, nil},
		{[]byte{127, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 245, 255, 41, 39, 127, 255}, 30, 5, "-10.55000", 15, nil},
		{[]byte{127, 255, 255, 255, 245, 223, 55, 170, 127, 255, 255, 255, 255, 255, 127, 255}, 30, 20, "-10.55000000000000000000", 14, nil},
		{[]byte{127, 255, 245, 223, 55, 170, 127, 255, 255, 255, 255, 255, 255, 255, 255, 4, 0}, 30, 25, "-10.5500000000000000000000000", 15, nil},
		{[]byte{128, 1, 128, 0}, 4, 2, "0.01", 2, nil},
		{[]byte{128, 0, 0, 128, 0}, 5, 0, "0", 3, nil},
		{[]byte{128, 0, 0, 12, 128, 0}, 7, 3, "0.012", 4, nil},
		{[]byte{128, 0, 0, 0, 1, 128, 0}, 10, 2, "0.01", 5, nil},
		{[]byte{128, 0, 0, 0, 0, 12, 128, 0}, 10, 3, "0.012", 6, nil},
		{[]byte{128, 0, 0, 0, 0, 1, 128, 0}, 13, 2, "0.01", 6, nil},
		{[]byte{128, 0, 188, 97, 78, 1, 96, 11, 128, 0}, 15, 14, "0.01234567890123", 8, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 188, 97, 78, 9, 128, 0}, 20, 10, "0.0123456789", 10, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 211, 128, 0}, 30, 5, "0.01235", 15, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 188, 97, 78, 53, 183, 191, 135, 89, 128, 0}, 30, 20, "0.01234567890123456789", 14, nil},
		{[]byte{128, 0, 0, 0, 188, 97, 78, 53, 183, 191, 135, 0, 135, 253, 217, 30, 0}, 30, 25, "0.0123456789012345678912345", 15, nil},
		{[]byte{227, 99, 128, 48}, 4, 2, "99.99", 2, nil},
		{[]byte{128, 48, 57, 167, 15}, 5, 0, "12345", 3, nil},
		{[]byte{167, 15, 3, 231, 128, 0}, 7, 3, "9999.999", 4, nil},
		{[]byte{128, 0, 48, 57, 0, 128, 0}, 10, 2, "12345.00", 5, nil},
		{[]byte{128, 0, 48, 57, 0, 0, 128, 0}, 10, 3, "12345.000", 6, nil},
		{[]byte{128, 0, 0, 48, 57, 0, 137, 59}, 13, 2, "12345.00", 6, nil},
		{[]byte{137, 59, 154, 201, 255, 1, 134, 159, 128, 0}, 15, 14, "9.99999999999999", 8, nil},
		{[]byte{128, 0, 0, 48, 57, 0, 0, 0, 0, 0, 128, 0}, 20, 10, "12345.0000000000", 10, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 48, 57, 0, 0, 0, 128, 0}, 30, 5, "12345.00000", 15, nil},
		{[]byte{128, 0, 0, 48, 57, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 48}, 30, 20, "12345.00000000000000000000", 14, nil},
		{[]byte{128, 48, 57, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 0}, 30, 25, "12345.0000000000000000000000000", 15, nil},
		{[]byte{227, 99, 128, 48}, 4, 2, "99.99", 2, nil},
		{[]byte{128, 48, 57, 167, 15}, 5, 0, "12345", 3, nil},
		{[]byte{167, 15, 3, 231, 128, 0}, 7, 3, "9999.999", 4, nil},
		{[]byte{128, 0, 48, 57, 0, 128, 0}, 10, 2, "12345.00", 5, nil},
		{[]byte{128, 0, 48, 57, 0, 0, 128, 0}, 10, 3, "12345.000", 6, nil},
		{[]byte{128, 0, 0, 48, 57, 0, 137, 59}, 13, 2, "12345.00", 6, nil},
		{[]byte{137, 59, 154, 201, 255, 1, 134, 159, 128, 0}, 15, 14, "9.99999999999999", 8, nil},
		{[]byte{128, 0, 0, 48, 57, 0, 0, 0, 0, 0, 128, 0}, 20, 10, "12345.0000000000", 10, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 48, 57, 0, 0, 0, 128, 0}, 30, 5, "12345.00000", 15, nil},
		{[]byte{128, 0, 0, 48, 57, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 48}, 30, 20, "12345.00000000000000000000", 14, nil},
		{[]byte{128, 48, 57, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0}, 30, 25, "12345.0000000000000000000000000", 15, nil},
		{[]byte{227, 99, 128, 0}, 4, 2, "99.99", 2, nil},
		{[]byte{128, 0, 123, 128, 123}, 5, 0, "123", 3, nil},
		{[]byte{128, 123, 1, 194, 128, 0}, 7, 3, "123.450", 4, nil},
		{[]byte{128, 0, 0, 123, 45, 128, 0}, 10, 2, "123.45", 5, nil},
		{[]byte{128, 0, 0, 123, 1, 194, 128, 0}, 10, 3, "123.450", 6, nil},
		{[]byte{128, 0, 0, 0, 123, 45, 137, 59}, 13, 2, "123.45", 6, nil},
		{[]byte{137, 59, 154, 201, 255, 1, 134, 159, 128, 0}, 15, 14, "9.99999999999999", 8, nil},
		{[]byte{128, 0, 0, 0, 123, 26, 210, 116, 128, 0, 128, 0}, 20, 10, "123.4500000000", 10, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 123, 0, 175, 200, 128, 0}, 30, 5, "123.45000", 15, nil},
		{[]byte{128, 0, 0, 0, 123, 26, 210, 116, 128, 0, 0, 0, 0, 0, 128, 0}, 30, 20, "123.45000000000000000000", 14, nil},
		{[]byte{128, 0, 123, 26, 210, 116, 128, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0}, 30, 25, "123.4500000000000000000000000", 15, nil},
		{[]byte{28, 156, 127, 255}, 4, 2, "-99.99", 2, nil},
		{[]byte{127, 255, 132, 127, 132}, 5, 0, "-123", 3, nil},
		{[]byte{127, 132, 254, 61, 127, 255}, 7, 3, "-123.450", 4, nil},
		{[]byte{127, 255, 255, 132, 210, 127, 255}, 10, 2, "-123.45", 5, nil},
		{[]byte{127, 255, 255, 132, 254, 61, 127, 255}, 10, 3, "-123.450", 6, nil},
		{[]byte{127, 255, 255, 255, 132, 210, 118, 196}, 13, 2, "-123.45", 6, nil},
		{[]byte{118, 196, 101, 54, 0, 254, 121, 96, 127, 255}, 15, 14, "-9.99999999999999", 8, nil},
		{[]byte{127, 255, 255, 255, 132, 229, 45, 139, 127, 255, 127, 255}, 20, 10, "-123.4500000000", 10, nil},
		{[]byte{127, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 132, 255, 80, 55, 127, 255}, 30, 5, "-123.45000", 15, nil},
		{[]byte{127, 255, 255, 255, 132, 229, 45, 139, 127, 255, 255, 255, 255, 255, 127, 255}, 30, 20, "-123.45000000000000000000", 14, nil},
		{[]byte{127, 255, 132, 229, 45, 139, 127, 255, 255, 255, 255, 255, 255, 255, 255, 20, 0}, 30, 25, "-123.4500000000000000000000000", 15, nil},
		{[]byte{128, 0, 128, 0}, 4, 2, "0.00", 2, nil},
		{[]byte{128, 0, 0, 128, 0}, 5, 0, "0", 3, nil},
		{[]byte{128, 0, 0, 0, 128, 0}, 7, 3, "0.000", 4, nil},
		{[]byte{128, 0, 0, 0, 0, 128, 0}, 10, 2, "0.00", 5, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 128, 0}, 10, 3, "0.000", 6, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 128, 0}, 13, 2, "0.00", 6, nil},
		{[]byte{128, 0, 1, 226, 58, 0, 0, 99, 128, 0}, 15, 14, "0.00012345000099", 8, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 1, 226, 58, 0, 128, 0}, 20, 10, "0.0001234500", 10, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12, 128, 0}, 30, 5, "0.00012", 15, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 1, 226, 58, 0, 15, 18, 2, 0, 128, 0}, 30, 20, "0.00012345000098765000", 14, nil},
		{[]byte{128, 0, 0, 0, 1, 226, 58, 0, 15, 18, 2, 0, 0, 0, 0, 15, 0}, 30, 25, "0.0001234500009876500000000", 15, nil},
		{[]byte{128, 0, 128, 0}, 4, 2, "0.00", 2, nil},
		{[]byte{128, 0, 0, 128, 0}, 5, 0, "0", 3, nil},
		{[]byte{128, 0, 0, 0, 128, 0}, 7, 3, "0.000", 4, nil},
		{[]byte{128, 0, 0, 0, 0, 128, 0}, 10, 2, "0.00", 5, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 128, 0}, 10, 3, "0.000", 6, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 128, 0}, 13, 2, "0.00", 6, nil},
		{[]byte{128, 0, 1, 226, 58, 0, 0, 99, 128, 0}, 15, 14, "0.00012345000099", 8, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 1, 226, 58, 0, 128, 0}, 20, 10, "0.0001234500", 10, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12, 128, 0}, 30, 5, "0.00012", 15, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 1, 226, 58, 0, 15, 18, 2, 0, 128, 0}, 30, 20, "0.00012345000098765000", 14, nil},
		{[]byte{128, 0, 0, 0, 1, 226, 58, 0, 15, 18, 2, 0, 0, 0, 0, 22, 0}, 30, 25, "0.0001234500009876500000000", 15, nil},
		{[]byte{128, 12, 128, 0}, 4, 2, "0.12", 2, nil},
		{[]byte{128, 0, 0, 128, 0}, 5, 0, "0", 3, nil},
		{[]byte{128, 0, 0, 123, 128, 0}, 7, 3, "0.123", 4, nil},
		{[]byte{128, 0, 0, 0, 12, 128, 0}, 10, 2, "0.12", 5, nil},
		{[]byte{128, 0, 0, 0, 0, 123, 128, 0}, 10, 3, "0.123", 6, nil},
		{[]byte{128, 0, 0, 0, 0, 12, 128, 7}, 13, 2, "0.12", 6, nil},
		{[]byte{128, 7, 91, 178, 144, 1, 129, 205, 128, 0}, 15, 14, "0.12345000098765", 8, nil},
		{[]byte{128, 0, 0, 0, 0, 7, 91, 178, 145, 0, 128, 0}, 20, 10, "0.1234500010", 10, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 48, 57, 128, 0}, 30, 5, "0.12345", 15, nil},
		{[]byte{128, 0, 0, 0, 0, 7, 91, 178, 144, 58, 222, 87, 208, 0, 128, 0}, 30, 20, "0.12345000098765000000", 14, nil},
		{[]byte{128, 0, 0, 7, 91, 178, 144, 58, 222, 87, 208, 0, 0, 0, 0, 30, 0}, 30, 25, "0.1234500009876500000000000", 15, nil},
		{[]byte{128, 0, 128, 0}, 4, 2, "0.00", 2, nil},
		{[]byte{128, 0, 0, 128, 0}, 5, 0, "0", 3, nil},
		{[]byte{128, 0, 0, 0, 128, 0}, 7, 3, "0.000", 4, nil},
		{[]byte{128, 0, 0, 0, 0, 128, 0}, 10, 2, "0.00", 5, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 128, 0}, 10, 3, "0.000", 6, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 127, 255}, 13, 2, "0.00", 6, nil},
		{[]byte{127, 255, 255, 255, 243, 255, 121, 59, 127, 255}, 15, 14, "-0.00000001234500", 8, nil},
		{[]byte{127, 255, 255, 255, 255, 255, 255, 255, 243, 252, 128, 0}, 20, 10, "-0.0000000123", 10, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 127, 255}, 30, 5, "0.00000", 15, nil},
		{[]byte{127, 255, 255, 255, 255, 255, 255, 255, 243, 235, 111, 183, 93, 178, 127, 255}, 30, 20, "-0.00000001234500009877", 14, nil},
		{[]byte{127, 255, 255, 255, 255, 255, 243, 235, 111, 183, 93, 255, 139, 69, 47, 30, 0}, 30, 25, "-0.0000000123450000987650000", 15, nil},
		{[]byte{227, 99, 129, 134}, 4, 2, "99.99", 2, nil},
		{[]byte{129, 134, 159, 167, 15}, 5, 0, "99999", 3, nil},
		{[]byte{167, 15, 3, 231, 133, 245}, 7, 3, "9999.999", 4, nil},
		{[]byte{133, 245, 224, 255, 99, 128, 152}, 10, 2, "99999999.99", 5, nil},
		{[]byte{128, 152, 150, 127, 3, 231, 227, 59}, 10, 3, "9999999.999", 6, nil},
		{[]byte{227, 59, 154, 201, 255, 99, 137, 59}, 13, 2, "99999999999.99", 6, nil},
		{[]byte{137, 59, 154, 201, 255, 1, 134, 159, 137, 59}, 15, 14, "9.99999999999999", 8, nil},
		{[]byte{137, 59, 154, 201, 255, 59, 154, 201, 255, 9, 128, 0}, 20, 10, "9999999999.9999999999", 10, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 4, 210, 29, 205, 139, 148, 0, 195, 80, 137, 59}, 30, 5, "1234500009876.50000", 15, nil},
		{[]byte{137, 59, 154, 201, 255, 59, 154, 201, 255, 59, 154, 201, 255, 99, 129, 134}, 30, 20, "9999999999.99999999999999999999", 14, nil},
		{[]byte{129, 134, 159, 59, 154, 201, 255, 59, 154, 201, 255, 0, 152, 150, 127, 30, 0}, 30, 25, "99999.9999999999999999999999999", 15, nil},
		{[]byte{227, 99, 129, 134}, 4, 2, "99.99", 2, nil},
		{[]byte{129, 134, 159, 167, 15}, 5, 0, "99999", 3, nil},
		{[]byte{167, 15, 3, 231, 133, 245}, 7, 3, "9999.999", 4, nil},
		{[]byte{133, 245, 224, 255, 99, 128, 152}, 10, 2, "99999999.99", 5, nil},
		{[]byte{128, 152, 150, 127, 3, 231, 128, 6}, 10, 3, "9999999.999", 6, nil},
		{[]byte{128, 6, 159, 107, 199, 11, 137, 59}, 13, 2, "111111111.11", 6, nil},
		{[]byte{137, 59, 154, 201, 255, 1, 134, 159, 128, 6}, 15, 14, "9.99999999999999", 8, nil},
		{[]byte{128, 6, 159, 107, 199, 6, 142, 119, 128, 0, 128, 0}, 20, 10, "111111111.1100000000", 10, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 0, 6, 159, 107, 199, 0, 42, 248, 128, 6}, 30, 5, "111111111.11000", 15, nil},
		{[]byte{128, 6, 159, 107, 199, 6, 142, 119, 128, 0, 0, 0, 0, 0, 129, 134}, 30, 20, "111111111.11000000000000000000", 14, nil},
		{[]byte{129, 134, 159, 59, 154, 201, 255, 59, 154, 201, 255, 0, 152, 150, 127, 10, 0}, 30, 25, "99999.9999999999999999999999999", 15, nil},
		{[]byte{128, 1, 128, 0}, 4, 2, "0.01", 2, nil},
		{[]byte{128, 0, 0, 128, 0}, 5, 0, "0", 3, nil},
		{[]byte{128, 0, 0, 10, 128, 0}, 7, 3, "0.010", 4, nil},
		{[]byte{128, 0, 0, 0, 1, 128, 0}, 10, 2, "0.01", 5, nil},
		{[]byte{128, 0, 0, 0, 0, 10, 128, 0}, 10, 3, "0.010", 6, nil},
		{[]byte{128, 0, 0, 0, 0, 1, 128, 0}, 13, 2, "0.01", 6, nil},
		{[]byte{128, 0, 152, 150, 128, 0, 0, 0, 128, 0}, 15, 14, "0.01000000000000", 8, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 152, 150, 128, 0, 128, 0}, 20, 10, "0.0100000000", 10, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 232, 128, 0}, 30, 5, "0.01000", 15, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 152, 150, 128, 0, 0, 0, 0, 0, 128, 0}, 30, 20, "0.01000000000000000000", 14, nil},
		{[]byte{128, 0, 0, 0, 152, 150, 128, 0, 0, 0, 0, 0, 0, 0, 0, 7, 0}, 30, 25, "0.0100000000000000000000000", 15, nil},
		{[]byte{227, 99, 128, 0}, 4, 2, "99.99", 2, nil},
		{[]byte{128, 0, 123, 128, 123}, 5, 0, "123", 3, nil},
		{[]byte{128, 123, 1, 144, 128, 0}, 7, 3, "123.400", 4, nil},
		{[]byte{128, 0, 0, 123, 40, 128, 0}, 10, 2, "123.40", 5, nil},
		{[]byte{128, 0, 0, 123, 1, 144, 128, 0}, 10, 3, "123.400", 6, nil},
		{[]byte{128, 0, 0, 0, 123, 40, 137, 59}, 13, 2, "123.40", 6, nil},
		{[]byte{137, 59, 154, 201, 255, 1, 134, 159, 128, 0}, 15, 14, "9.99999999999999", 8, nil},
		{[]byte{128, 0, 0, 0, 123, 23, 215, 132, 0, 0, 128, 0}, 20, 10, "123.4000000000", 10, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 123, 0, 156, 64, 128, 0}, 30, 5, "123.40000", 15, nil},
		{[]byte{128, 0, 0, 0, 123, 23, 215, 132, 0, 0, 0, 0, 0, 0, 128, 0}, 30, 20, "123.40000000000000000000", 14, nil},
		{[]byte{128, 0, 123, 23, 215, 132, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0}, 30, 25, "123.4000000000000000000000000", 15, nil},
		{[]byte{28, 156, 127, 253}, 4, 2, "-99.99", 2, nil},
		{[]byte{127, 253, 204, 125, 205}, 5, 0, "-563", 3, nil},
		{[]byte{125, 205, 253, 187, 127, 255}, 7, 3, "-562.580", 4, nil},
		{[]byte{127, 255, 253, 205, 197, 127, 255}, 10, 2, "-562.58", 5, nil},
		{[]byte{127, 255, 253, 205, 253, 187, 127, 255}, 10, 3, "-562.580", 6, nil},
		{[]byte{127, 255, 255, 253, 205, 197, 118, 196}, 13, 2, "-562.58", 6, nil},
		{[]byte{118, 196, 101, 54, 0, 254, 121, 96, 127, 255}, 15, 14, "-9.99999999999999", 8, nil},
		{[]byte{127, 255, 255, 253, 205, 221, 109, 230, 255, 255, 127, 255}, 20, 10, "-562.5800000000", 10, nil},
		{[]byte{127, 255, 255, 255, 255, 255, 255, 255, 255, 255, 253, 205, 255, 29, 111, 127, 255}, 30, 5, "-562.58000", 15, nil},
		{[]byte{127, 255, 255, 253, 205, 221, 109, 230, 255, 255, 255, 255, 255, 255, 127, 253}, 30, 20, "-562.58000000000000000000", 14, nil},
		{[]byte{127, 253, 205, 221, 109, 230, 255, 255, 255, 255, 255, 255, 255, 255, 255, 13, 0}, 30, 25, "-562.5800000000000000000000000", 15, nil},
		{[]byte{28, 156, 127, 241}, 4, 2, "-99.99", 2, nil},
		{[]byte{127, 241, 140, 113, 140}, 5, 0, "-3699", 3, nil},
		{[]byte{113, 140, 255, 245, 127, 255}, 7, 3, "-3699.010", 4, nil},
		{[]byte{127, 255, 241, 140, 254, 127, 255}, 10, 2, "-3699.01", 5, nil},
		{[]byte{127, 255, 241, 140, 255, 245, 127, 255}, 10, 3, "-3699.010", 6, nil},
		{[]byte{127, 255, 255, 241, 140, 254, 118, 196}, 13, 2, "-3699.01", 6, nil},
		{[]byte{118, 196, 101, 54, 0, 254, 121, 96, 127, 255}, 15, 14, "-9.99999999999999", 8, nil},
		{[]byte{127, 255, 255, 241, 140, 255, 103, 105, 127, 255, 127, 255}, 20, 10, "-3699.0100000000", 10, nil},
		{[]byte{127, 255, 255, 255, 255, 255, 255, 255, 255, 255, 241, 140, 255, 252, 23, 127, 255}, 30, 5, "-3699.01000", 15, nil},
		{[]byte{127, 255, 255, 241, 140, 255, 103, 105, 127, 255, 255, 255, 255, 255, 127, 241}, 30, 20, "-3699.01000000000000000000", 14, nil},
		{[]byte{127, 241, 140, 255, 103, 105, 127, 255, 255, 255, 255, 255, 255, 255, 255, 13, 0}, 30, 25, "-3699.0100000000000000000000000", 15, nil},
		{[]byte{28, 156, 127, 248}, 4, 2, "-99.99", 2, nil},
		{[]byte{127, 248, 99, 120, 99}, 5, 0, "-1948", 3, nil},
		{[]byte{120, 99, 255, 115, 127, 255}, 7, 3, "-1948.140", 4, nil},
		{[]byte{127, 255, 248, 99, 241, 127, 255}, 10, 2, "-1948.14", 5, nil},
		{[]byte{127, 255, 248, 99, 255, 115, 127, 255}, 10, 3, "-1948.140", 6, nil},
		{[]byte{127, 255, 255, 248, 99, 241, 118, 196}, 13, 2, "-1948.14", 6, nil},
		{[]byte{118, 196, 101, 54, 0, 254, 121, 96, 127, 255}, 15, 14, "-9.99999999999999", 8, nil},
		{[]byte{127, 255, 255, 248, 99, 247, 167, 196, 255, 255, 127, 255}, 20, 10, "-1948.1400000000", 10, nil},
		{[]byte{127, 255, 255, 255, 255, 255, 255, 255, 255, 255, 248, 99, 255, 201, 79, 127, 255}, 30, 5, "-1948.14000", 15, nil},
		{[]byte{127, 255, 255, 248, 99, 247, 167, 196, 255, 255, 255, 255, 255, 255, 127, 248}, 30, 20, "-1948.14000000000000000000", 14, nil},
		{[]byte{127, 248, 99, 247, 167, 196, 255, 255, 255, 255, 255, 255, 255, 255, 255, 13, 0}, 30, 25, "-1948.1400000000000000000000000", 15, nil},
	}
	for i, tc := range testcases {
		value, pos, err := decodeDecimal(tc.Data, tc.Precision, tc.Decimals, false)
		expectedFloat, _ := strconv.ParseFloat(tc.Expected, 64)
		c.Assert(value.(float64), DecodeDecimalsEquals, pos, err, expectedFloat, tc.ExpectedPos, tc.ExpectedErr, i)

		value, pos, err = decodeDecimal(tc.Data, tc.Precision, tc.Decimals, true)
		expectedDecimal, _ := decimal.NewFromString(tc.Expected)
		c.Assert(value.(decimal.Decimal), DecodeDecimalsEquals, pos, err, expectedDecimal, tc.ExpectedPos, tc.ExpectedErr, i)
	}
}

func (_ *testDecodeSuite) TestLastNull(c *C) {
	// Table format:
	// desc funnytable;
	// +-------+------------+------+-----+---------+-------+
	// | Field | Type       | Null | Key | Default | Extra |
	// +-------+------------+------+-----+---------+-------+
	// | value | tinyint(4) | YES  |     | NULL    |       |
	// +-------+------------+------+-----+---------+-------+

	// insert into funnytable values (1), (2), (null);
	// insert into funnytable values (1), (null), (2);
	// all must get 3 rows

	tableMapEventData := []byte("\xd3\x01\x00\x00\x00\x00\x01\x00\x04test\x00\nfunnytable\x00\x01\x01\x00\x01")

	tableMapEvent := new(TableMapEvent)
	tableMapEvent.tableIDSize = 6
	err := tableMapEvent.Decode(tableMapEventData)
	c.Assert(err, IsNil)

	rows := new(RowsEvent)
	rows.tableIDSize = 6
	rows.tables = make(map[uint64]*TableMapEvent)
	rows.tables[tableMapEvent.TableID] = tableMapEvent
	rows.Version = 2

	tbls := [][]byte{
		[]byte("\xd3\x01\x00\x00\x00\x00\x01\x00\x02\x00\x01\xff\xfe\x01\xff\xfe\x02"),
		[]byte("\xd3\x01\x00\x00\x00\x00\x01\x00\x02\x00\x01\xff\xfe\x01\xfe\x02\xff"),
	}

	for _, tbl := range tbls {
		rows.Rows = nil
		err = rows.Decode(tbl)
		c.Assert(err, IsNil)
		c.Assert(rows.Rows, HasLen, 3)
	}
}

func (_ *testDecodeSuite) TestParseRowPanic(c *C) {
	tableMapEvent := new(TableMapEvent)
	tableMapEvent.tableIDSize = 6
	tableMapEvent.TableID = 1810
	tableMapEvent.ColumnType = []byte{3, 15, 15, 15, 9, 15, 15, 252, 3, 3, 3, 15, 3, 3, 3, 15, 3, 15, 1, 15, 3, 1, 252, 15, 15, 15}
	tableMapEvent.ColumnMeta = []uint16{0, 108, 60, 765, 0, 765, 765, 4, 0, 0, 0, 765, 0, 0, 0, 3, 0, 3, 0, 765, 0, 0, 2, 108, 108, 108}

	rows := new(RowsEvent)
	rows.tableIDSize = 6
	rows.tables = make(map[uint64]*TableMapEvent)
	rows.tables[tableMapEvent.TableID] = tableMapEvent
	rows.Version = 2

	data := []byte{18, 7, 0, 0, 0, 0, 1, 0, 2, 0, 26, 1, 1, 16, 252, 248, 142, 63, 0, 0, 13, 0, 0, 0, 13, 0, 0, 0}

	err := rows.Decode(data)
	c.Assert(err, IsNil)
	c.Assert(rows.Rows[0][0], Equals, int32(16270))
}

type simpleDecimalEqualsChecker struct {
	*CheckerInfo
}

var SimpleDecimalEqualsChecker Checker = &simpleDecimalEqualsChecker{
	&CheckerInfo{Name: "Equals", Params: []string{"obtained", "expected"}},
}

func (checker *simpleDecimalEqualsChecker) Check(params []interface{}, names []string) (result bool, error string) {
	defer func() {
		if v := recover(); v != nil {
			result = false
			error = fmt.Sprint(v)
		}
	}()

	return params[0].(decimal.Decimal).Equal(params[1].(decimal.Decimal)), ""
}

func (_ *testDecodeSuite) TestParseJson(c *C) {
	// Table format:
	// mysql> desc t10;
	// +-------+---------------+------+-----+---------+-------+
	// | Field | Type          | Null | Key | Default | Extra |
	// +-------+---------------+------+-----+---------+-------+
	// | c1    | json          | YES  |     | NULL    |       |
	// | c2    | decimal(10,0) | YES  |     | NULL    |       |
	// +-------+---------------+------+-----+---------+-------+

	// CREATE TABLE `t10` (
	//   `c1` json DEFAULT NULL,
	//   `c2` decimal(10,0)
	// ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

	// INSERT INTO `t10` (`c2`) VALUES (1);
	// INSERT INTO `t10` (`c1`, `c2`) VALUES ('{"key1": "value1", "key2": "value2"}', 1);
	// test json deserialization
	// INSERT INTO `t10`(`c1`,`c2`) VALUES ('{"text":"Lorem ipsum dolor sit amet, consectetuer adipiscing elit. Aenean commodo ligula eget dolor. Aenean massa. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Donec quam felis, ultricies nec, pellentesque eu, pretium quis, sem. Nulla consequat massa quis enim. Donec pede justo, fringilla vel, aliquet nec, vulputate eget, arcu. In enim justo, rhoncus ut, imperdiet a, venenatis vitae, justo. Nullam dictum felis eu pede mollis pretium. Integer tincidunt. Cras dapibus. Vivamus elementum semper nisi. Aenean vulputate eleifend tellus. Aenean leo ligula, porttitor eu, consequat vitae, eleifend ac, enim. Aliquam lorem ante, dapibus in, viverra quis, feugiat a, tellus. Phasellus viverra nulla ut metus varius laoreet. Quisque rutrum. Aenean imperdiet. Etiam ultricies nisi vel augue. Curabitur ullamcorper ultricies nisi. Nam eget dui. Etiam rhoncus. Maecenas tempus, tellus eget condimentum rhoncus, sem quam semper libero, sit amet adipiscing sem neque sed ipsum. Nam quam nunc, blandit vel, luctus pulvinar, hendrerit id, lorem. Maecenas nec odio et ante tincidunt tempus. Donec vitae sapien ut libero venenatis faucibus. Nullam quis ante. Etiam sit amet orci eget eros faucibus tincidunt. Duis leo. Sed fringilla mauris sit amet nibh. Donec sodales sagittis magna. Sed consequat, leo eget bibendum sodales, augue velit cursus nunc, quis gravida magna mi a libero. Fusce vulputate eleifend sapien. Vestibulum purus quam, scelerisque ut, mollis sed, nonummy id, metus. Nullam accumsan lorem in dui. Cras ultricies mi eu turpis hendrerit fringilla. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; In ac dui quis mi consectetuer lacinia. Nam pretium turpis et arcu. Duis arcu tortor, suscipit eget, imperdiet nec, imperdiet iaculis, ipsum. Sed aliquam ultrices mauris. Integer ante arcu, accumsan a, consectetuer eget, posuere ut, mauris. Praesent adipiscing. Phasellus ullamcorper ipsum rutrum nunc. Nunc nonummy metus. Vestibulum volutpat pretium libero. Cras id dui. Aenean ut eros et nisl sagittis vestibulum. Nullam nulla eros, ultricies sit amet, nonummy id, imperdiet feugiat, pede. Sed lectus. Donec mollis hendrerit risus. Phasellus nec sem in justo pellentesque facilisis. Etiam imperdiet imperdiet orci. Nunc nec neque. Phasellus leo dolor, tempus non, auctor et, hendrerit quis, nisi. Curabitur ligula sapien, tincidunt non, euismod vitae, posuere imperdiet, leo. Maecenas malesuada. Praesent congue erat at massa. Sed cursus turpis vitae tortor. Donec posuere vulputate arcu. Phasellus accumsan cursus velit. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Sed aliquam, nisi quis porttitor congue, elit erat euismod orci, ac"}',101);
	tableMapEventData := []byte("m\x00\x00\x00\x00\x00\x01\x00\x04test\x00\x03t10\x00\x02\xf5\xf6\x03\x04\n\x00\x03")

	tableMapEvent := new(TableMapEvent)
	tableMapEvent.tableIDSize = 6
	err := tableMapEvent.Decode(tableMapEventData)
	c.Assert(err, IsNil)

	rows := new(RowsEvent)
	rows.tableIDSize = 6
	rows.tables = make(map[uint64]*TableMapEvent)
	rows.tables[tableMapEvent.TableID] = tableMapEvent
	rows.Version = 2

	tbls := [][]byte{
		[]byte("m\x00\x00\x00\x00\x00\x01\x00\x02\x00\x02\xff\xfd\x80\x00\x00\x00\x01"),
		[]byte("m\x00\x00\x00\x00\x00\x01\x00\x02\x00\x02\xff\xfc)\x00\x00\x00\x00\x02\x00(\x00\x12\x00\x04\x00\x16\x00\x04\x00\f\x1a\x00\f!\x00key1key2\x06value1\x06value2\x80\x00\x00\x00\x01"),
	}

	for _, tbl := range tbls {
		rows.Rows = nil
		err = rows.Decode(tbl)
		c.Assert(err, IsNil)
		c.Assert(rows.Rows[0][1], Equals, float64(1))
	}

	longTbls := [][]byte{
		[]byte("m\x00\x00\x00\x00\x00\x01\x00\x02\x00\x02\xff\xfc\xd0\n\x00\x00\x00\x01\x00\xcf\n\v\x00\x04\x00\f\x0f\x00text\xbe\x15Lorem ipsum dolor sit amet, consectetuer adipiscing elit. Aenean commodo ligula eget dolor. Aenean massa. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Donec quam felis, ultricies nec, pellentesque eu, pretium quis, sem. Nulla consequat massa quis enim. Donec pede justo, fringilla vel, aliquet nec, vulputate eget, arcu. In enim justo, rhoncus ut, imperdiet a, venenatis vitae, justo. Nullam dictum felis eu pede mollis pretium. Integer tincidunt. Cras dapibus. Vivamus elementum semper nisi. Aenean vulputate eleifend tellus. Aenean leo ligula, porttitor eu, consequat vitae, eleifend ac, enim. Aliquam lorem ante, dapibus in, viverra quis, feugiat a, tellus. Phasellus viverra nulla ut metus varius laoreet. Quisque rutrum. Aenean imperdiet. Etiam ultricies nisi vel augue. Curabitur ullamcorper ultricies nisi. Nam eget dui. Etiam rhoncus. Maecenas tempus, tellus eget condimentum rhoncus, sem quam semper libero, sit amet adipiscing sem neque sed ipsum. Nam quam nunc, blandit vel, luctus pulvinar, hendrerit id, lorem. Maecenas nec odio et ante tincidunt tempus. Donec vitae sapien ut libero venenatis faucibus. Nullam quis ante. Etiam sit amet orci eget eros faucibus tincidunt. Duis leo. Sed fringilla mauris sit amet nibh. Donec sodales sagittis magna. Sed consequat, leo eget bibendum sodales, augue velit cursus nunc, quis gravida magna mi a libero. Fusce vulputate eleifend sapien. Vestibulum purus quam, scelerisque ut, mollis sed, nonummy id, metus. Nullam accumsan lorem in dui. Cras ultricies mi eu turpis hendrerit fringilla. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; In ac dui quis mi consectetuer lacinia. Nam pretium turpis et arcu. Duis arcu tortor, suscipit eget, imperdiet nec, imperdiet iaculis, ipsum. Sed aliquam ultrices mauris. Integer ante arcu, accumsan a, consectetuer eget, posuere ut, mauris. Praesent adipiscing. Phasellus ullamcorper ipsum rutrum nunc. Nunc nonummy metus. Vestibulum volutpat pretium libero. Cras id dui. Aenean ut eros et nisl sagittis vestibulum. Nullam nulla eros, ultricies sit amet, nonummy id, imperdiet feugiat, pede. Sed lectus. Donec mollis hendrerit risus. Phasellus nec sem in justo pellentesque facilisis. Etiam imperdiet imperdiet orci. Nunc nec neque. Phasellus leo dolor, tempus non, auctor et, hendrerit quis, nisi. Curabitur ligula sapien, tincidunt non, euismod vitae, posuere imperdiet, leo. Maecenas malesuada. Praesent congue erat at massa. Sed cursus turpis vitae tortor. Donec posuere vulputate arcu. Phasellus accumsan cursus velit. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Sed aliquam, nisi quis porttitor congue, elit erat euismod orci, ac\x80\x00\x00\x00e"),
	}

	for _, ltbl := range longTbls {
		rows.Rows = nil
		err = rows.Decode(ltbl)
		c.Assert(err, IsNil)
		c.Assert(rows.Rows[0][1], Equals, float64(101))
	}
}
func (_ *testDecodeSuite) TestParseJsonDecimal(c *C) {
	// Table format:
	// mysql> desc t10;
	// +-------+---------------+------+-----+---------+-------+
	// | Field | Type          | Null | Key | Default | Extra |
	// +-------+---------------+------+-----+---------+-------+
	// | c1    | json          | YES  |     | NULL    |       |
	// | c2    | decimal(10,0) | YES  |     | NULL    |       |
	// +-------+---------------+------+-----+---------+-------+

	// CREATE TABLE `t10` (
	//   `c1` json DEFAULT NULL,
	//   `c2` decimal(10,0)
	// ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

	// INSERT INTO `t10` (`c2`) VALUES (1);
	// INSERT INTO `t10` (`c1`, `c2`) VALUES ('{"key1": "value1", "key2": "value2"}', 1);
	// test json deserialization
	// INSERT INTO `t10`(`c1`,`c2`) VALUES ('{"text":"Lorem ipsum dolor sit amet, consectetuer adipiscing elit. Aenean commodo ligula eget dolor. Aenean massa. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Donec quam felis, ultricies nec, pellentesque eu, pretium quis, sem. Nulla consequat massa quis enim. Donec pede justo, fringilla vel, aliquet nec, vulputate eget, arcu. In enim justo, rhoncus ut, imperdiet a, venenatis vitae, justo. Nullam dictum felis eu pede mollis pretium. Integer tincidunt. Cras dapibus. Vivamus elementum semper nisi. Aenean vulputate eleifend tellus. Aenean leo ligula, porttitor eu, consequat vitae, eleifend ac, enim. Aliquam lorem ante, dapibus in, viverra quis, feugiat a, tellus. Phasellus viverra nulla ut metus varius laoreet. Quisque rutrum. Aenean imperdiet. Etiam ultricies nisi vel augue. Curabitur ullamcorper ultricies nisi. Nam eget dui. Etiam rhoncus. Maecenas tempus, tellus eget condimentum rhoncus, sem quam semper libero, sit amet adipiscing sem neque sed ipsum. Nam quam nunc, blandit vel, luctus pulvinar, hendrerit id, lorem. Maecenas nec odio et ante tincidunt tempus. Donec vitae sapien ut libero venenatis faucibus. Nullam quis ante. Etiam sit amet orci eget eros faucibus tincidunt. Duis leo. Sed fringilla mauris sit amet nibh. Donec sodales sagittis magna. Sed consequat, leo eget bibendum sodales, augue velit cursus nunc, quis gravida magna mi a libero. Fusce vulputate eleifend sapien. Vestibulum purus quam, scelerisque ut, mollis sed, nonummy id, metus. Nullam accumsan lorem in dui. Cras ultricies mi eu turpis hendrerit fringilla. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; In ac dui quis mi consectetuer lacinia. Nam pretium turpis et arcu. Duis arcu tortor, suscipit eget, imperdiet nec, imperdiet iaculis, ipsum. Sed aliquam ultrices mauris. Integer ante arcu, accumsan a, consectetuer eget, posuere ut, mauris. Praesent adipiscing. Phasellus ullamcorper ipsum rutrum nunc. Nunc nonummy metus. Vestibulum volutpat pretium libero. Cras id dui. Aenean ut eros et nisl sagittis vestibulum. Nullam nulla eros, ultricies sit amet, nonummy id, imperdiet feugiat, pede. Sed lectus. Donec mollis hendrerit risus. Phasellus nec sem in justo pellentesque facilisis. Etiam imperdiet imperdiet orci. Nunc nec neque. Phasellus leo dolor, tempus non, auctor et, hendrerit quis, nisi. Curabitur ligula sapien, tincidunt non, euismod vitae, posuere imperdiet, leo. Maecenas malesuada. Praesent congue erat at massa. Sed cursus turpis vitae tortor. Donec posuere vulputate arcu. Phasellus accumsan cursus velit. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Sed aliquam, nisi quis porttitor congue, elit erat euismod orci, ac"}',101);
	tableMapEventData := []byte("m\x00\x00\x00\x00\x00\x01\x00\x04test\x00\x03t10\x00\x02\xf5\xf6\x03\x04\n\x00\x03")

	tableMapEvent := new(TableMapEvent)
	tableMapEvent.tableIDSize = 6
	err := tableMapEvent.Decode(tableMapEventData)
	c.Assert(err, IsNil)

	rows := RowsEvent{useDecimal: true}
	rows.tableIDSize = 6
	rows.tables = make(map[uint64]*TableMapEvent)
	rows.tables[tableMapEvent.TableID] = tableMapEvent
	rows.Version = 2

	tbls := [][]byte{
		[]byte("m\x00\x00\x00\x00\x00\x01\x00\x02\x00\x02\xff\xfd\x80\x00\x00\x00\x01"),
		[]byte("m\x00\x00\x00\x00\x00\x01\x00\x02\x00\x02\xff\xfc)\x00\x00\x00\x00\x02\x00(\x00\x12\x00\x04\x00\x16\x00\x04\x00\f\x1a\x00\f!\x00key1key2\x06value1\x06value2\x80\x00\x00\x00\x01"),
	}

	for _, tbl := range tbls {
		rows.Rows = nil
		err = rows.Decode(tbl)
		c.Assert(err, IsNil)
		c.Assert(rows.Rows[0][1], SimpleDecimalEqualsChecker, decimal.NewFromFloat(1))
	}

	longTbls := [][]byte{
		[]byte("m\x00\x00\x00\x00\x00\x01\x00\x02\x00\x02\xff\xfc\xd0\n\x00\x00\x00\x01\x00\xcf\n\v\x00\x04\x00\f\x0f\x00text\xbe\x15Lorem ipsum dolor sit amet, consectetuer adipiscing elit. Aenean commodo ligula eget dolor. Aenean massa. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Donec quam felis, ultricies nec, pellentesque eu, pretium quis, sem. Nulla consequat massa quis enim. Donec pede justo, fringilla vel, aliquet nec, vulputate eget, arcu. In enim justo, rhoncus ut, imperdiet a, venenatis vitae, justo. Nullam dictum felis eu pede mollis pretium. Integer tincidunt. Cras dapibus. Vivamus elementum semper nisi. Aenean vulputate eleifend tellus. Aenean leo ligula, porttitor eu, consequat vitae, eleifend ac, enim. Aliquam lorem ante, dapibus in, viverra quis, feugiat a, tellus. Phasellus viverra nulla ut metus varius laoreet. Quisque rutrum. Aenean imperdiet. Etiam ultricies nisi vel augue. Curabitur ullamcorper ultricies nisi. Nam eget dui. Etiam rhoncus. Maecenas tempus, tellus eget condimentum rhoncus, sem quam semper libero, sit amet adipiscing sem neque sed ipsum. Nam quam nunc, blandit vel, luctus pulvinar, hendrerit id, lorem. Maecenas nec odio et ante tincidunt tempus. Donec vitae sapien ut libero venenatis faucibus. Nullam quis ante. Etiam sit amet orci eget eros faucibus tincidunt. Duis leo. Sed fringilla mauris sit amet nibh. Donec sodales sagittis magna. Sed consequat, leo eget bibendum sodales, augue velit cursus nunc, quis gravida magna mi a libero. Fusce vulputate eleifend sapien. Vestibulum purus quam, scelerisque ut, mollis sed, nonummy id, metus. Nullam accumsan lorem in dui. Cras ultricies mi eu turpis hendrerit fringilla. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; In ac dui quis mi consectetuer lacinia. Nam pretium turpis et arcu. Duis arcu tortor, suscipit eget, imperdiet nec, imperdiet iaculis, ipsum. Sed aliquam ultrices mauris. Integer ante arcu, accumsan a, consectetuer eget, posuere ut, mauris. Praesent adipiscing. Phasellus ullamcorper ipsum rutrum nunc. Nunc nonummy metus. Vestibulum volutpat pretium libero. Cras id dui. Aenean ut eros et nisl sagittis vestibulum. Nullam nulla eros, ultricies sit amet, nonummy id, imperdiet feugiat, pede. Sed lectus. Donec mollis hendrerit risus. Phasellus nec sem in justo pellentesque facilisis. Etiam imperdiet imperdiet orci. Nunc nec neque. Phasellus leo dolor, tempus non, auctor et, hendrerit quis, nisi. Curabitur ligula sapien, tincidunt non, euismod vitae, posuere imperdiet, leo. Maecenas malesuada. Praesent congue erat at massa. Sed cursus turpis vitae tortor. Donec posuere vulputate arcu. Phasellus accumsan cursus velit. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Sed aliquam, nisi quis porttitor congue, elit erat euismod orci, ac\x80\x00\x00\x00e"),
	}

	for _, ltbl := range longTbls {
		rows.Rows = nil
		err = rows.Decode(ltbl)
		c.Assert(err, IsNil)
		c.Assert(rows.Rows[0][1], SimpleDecimalEqualsChecker, decimal.NewFromFloat(101))
	}
}

func (_ *testDecodeSuite) TestEnum(c *C) {
	// mysql> desc aenum;
	// +-------+-------------------------------------------+------+-----+---------+-------+
	// | Field | Type                                      | Null | Key | Default | Extra |
	// +-------+-------------------------------------------+------+-----+---------+-------+
	// | id    | int(11)                                   | YES  |     | NULL    |       |
	// | aset  | enum('0','1','2','3','4','5','6','7','8') | YES  |     | NULL    |       |
	// +-------+-------------------------------------------+------+-----+---------+-------+
	// 2 rows in set (0.00 sec)
	//
	// insert into aenum(id, aset) values(1, '0');
	tableMapEventData := []byte("\x42\x0f\x00\x00\x00\x00\x01\x00\x05\x74\x74\x65\x73\x74\x00\x05")
	tableMapEventData = append(tableMapEventData, []byte("\x61\x65\x6e\x75\x6d\x00\x02\x03\xfe\x02\xf7\x01\x03")...)
	tableMapEvent := new(TableMapEvent)
	tableMapEvent.tableIDSize = 6
	err := tableMapEvent.Decode(tableMapEventData)
	c.Assert(err, IsNil)

	rows := new(RowsEvent)
	rows.tableIDSize = 6
	rows.tables = make(map[uint64]*TableMapEvent)
	rows.tables[tableMapEvent.TableID] = tableMapEvent
	rows.Version = 2

	data := []byte("\x42\x0f\x00\x00\x00\x00\x01\x00\x02\x00\x02\xff\xfc\x01\x00\x00\x00\x01")

	rows.Rows = nil
	err = rows.Decode(data)
	c.Assert(err, IsNil)
	c.Assert(rows.Rows[0][1], Equals, int64(1))
}

func (_ *testDecodeSuite) TestMultiBytesEnum(c *C) {
	// CREATE TABLE numbers (
	// 	id int auto_increment,
	// 	num ENUM( '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41', '42', '43', '44', '45', '46', '47', '48', '49', '50', '51', '52', '53', '54', '55', '56', '57', '58', '59', '60', '61', '62', '63', '64', '65', '66', '67', '68', '69', '70', '71', '72', '73', '74', '75', '76', '77', '78', '79', '80', '81', '82', '83', '84', '85', '86', '87', '88', '89', '90', '91', '92', '93', '94', '95', '96', '97', '98', '99', '100', '101', '102', '103', '104', '105', '106', '107', '108', '109', '110', '111', '112', '113', '114', '115', '116', '117', '118', '119', '120', '121', '122', '123', '124', '125', '126', '127', '128', '129', '130', '131', '132', '133', '134', '135', '136', '137', '138', '139', '140', '141', '142', '143', '144', '145', '146', '147', '148', '149', '150', '151', '152', '153', '154', '155', '156', '157', '158', '159', '160', '161', '162', '163', '164', '165', '166', '167', '168', '169', '170', '171', '172', '173', '174', '175', '176', '177', '178', '179', '180', '181', '182', '183', '184', '185', '186', '187', '188', '189', '190', '191', '192', '193', '194', '195', '196', '197', '198', '199', '200', '201', '202', '203', '204', '205', '206', '207', '208', '209', '210', '211', '212', '213', '214', '215', '216', '217', '218', '219', '220', '221', '222', '223', '224', '225', '226', '227', '228', '229', '230', '231', '232', '233', '234', '235', '236', '237', '238', '239', '240', '241', '242', '243', '244', '245', '246', '247', '248', '249', '250', '251', '252', '253', '254', '255','256','257'

	// ),
	// primary key(id)
	// );

	//
	// insert into numbers(num) values ('0'), ('256');
	tableMapEventData := []byte("\x84\x0f\x00\x00\x00\x00\x01\x00\x05\x74\x74\x65\x73\x74\x00\x07")
	tableMapEventData = append(tableMapEventData, []byte("\x6e\x75\x6d\x62\x65\x72\x73\x00\x02\x03\xfe\x02\xf7\x02\x02")...)
	tableMapEvent := new(TableMapEvent)
	tableMapEvent.tableIDSize = 6
	err := tableMapEvent.Decode(tableMapEventData)
	c.Assert(err, IsNil)

	rows := new(RowsEvent)
	rows.tableIDSize = 6
	rows.tables = make(map[uint64]*TableMapEvent)
	rows.tables[tableMapEvent.TableID] = tableMapEvent
	rows.Version = 2

	data := []byte("\x84\x0f\x00\x00\x00\x00\x01\x00\x02\x00\x02\xff\xfc\x01\x00\x00\x00\x01\x00\xfc\x02\x00\x00\x00\x01\x01")

	rows.Rows = nil
	err = rows.Decode(data)
	c.Assert(err, IsNil)
	c.Assert(rows.Rows[0][1], Equals, int64(1))
	c.Assert(rows.Rows[1][1], Equals, int64(257))
}

func (_ *testDecodeSuite) TestSet(c *C) {
	// mysql> desc aset;
	// +--------+---------------------------------------------------------------------------------------+------+-----+---------+-------+
	// | Field  | Type                                                                                  | Null | Key | Default | Extra |
	// +--------+---------------------------------------------------------------------------------------+------+-----+---------+-------+
	// | id     | int(11)                                                                               | YES  |     | NULL    |       |
	// | region | set('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18') | YES  |     | NULL    |       |
	// +--------+---------------------------------------------------------------------------------------+------+-----+---------+-------+
	// 2 rows in set (0.00 sec)
	//
	// insert into aset(id, region) values(1, '1,3');

	tableMapEventData := []byte("\xe7\x0e\x00\x00\x00\x00\x01\x00\x05\x74\x74\x65\x73\x74\x00\x04")
	tableMapEventData = append(tableMapEventData, []byte("\x61\x73\x65\x74\x00\x02\x03\xfe\x02\xf8\x03\x03")...)
	tableMapEvent := new(TableMapEvent)
	tableMapEvent.tableIDSize = 6
	err := tableMapEvent.Decode(tableMapEventData)
	c.Assert(err, IsNil)

	rows := new(RowsEvent)
	rows.tableIDSize = 6
	rows.tables = make(map[uint64]*TableMapEvent)
	rows.tables[tableMapEvent.TableID] = tableMapEvent
	rows.Version = 2

	data := []byte("\xe7\x0e\x00\x00\x00\x00\x01\x00\x02\x00\x02\xff\xfc\x01\x00\x00\x00\x05\x00\x00")

	rows.Rows = nil
	err = rows.Decode(data)
	c.Assert(err, IsNil)
	c.Assert(rows.Rows[0][1], Equals, int64(5))
}

func (_ *testDecodeSuite) TestJsonNull(c *C) {
	// Table:
	// desc hj_order_preview
	// +------------------+------------+------+-----+-------------------+----------------+
	// | Field            | Type       | Null | Key | Default           | Extra          |
	// +------------------+------------+------+-----+-------------------+----------------+
	// | id               | int(13)    | NO   | PRI | <null>            | auto_increment |
	// | buyer_id         | bigint(13) | NO   |     | <null>            |                |
	// | order_sn         | bigint(13) | NO   |     | <null>            |                |
	// | order_detail     | json       | NO   |     | <null>            |                |
	// | is_del           | tinyint(1) | NO   |     | 0                 |                |
	// | add_time         | int(13)    | NO   |     | <null>            |                |
	// | last_update_time | timestamp  | NO   |     | CURRENT_TIMESTAMP |                |
	// +------------------+------------+------+-----+-------------------+----------------+
	// insert into hj_order_preview
	// (id, buyer_id, order_sn, is_del, add_time, last_update_time)
	// values (1, 95891865464386, 13376222192996417, 0, 1479983995, 1479983995)

	tableMapEventData := []byte("r\x00\x00\x00\x00\x00\x01\x00\x04test\x00\x10hj_order_preview\x00\a\x03\b\b\xf5\x01\x03\x11\x02\x04\x00\x00")

	tableMapEvent := new(TableMapEvent)
	tableMapEvent.tableIDSize = 6
	err := tableMapEvent.Decode(tableMapEventData)
	c.Assert(err, IsNil)

	rows := new(RowsEvent)
	rows.tableIDSize = 6
	rows.tables = make(map[uint64]*TableMapEvent)
	rows.tables[tableMapEvent.TableID] = tableMapEvent
	rows.Version = 2

	data :=
		[]byte("r\x00\x00\x00\x00\x00\x01\x00\x02\x00\a\xff\x80\x01\x00\x00\x00B\ue4d06W\x00\x00A\x10@l\x9a\x85/\x00\x00\x00\x00\x00\x00{\xc36X\x00\x00\x00\x00")

	rows.Rows = nil
	err = rows.Decode(data)
	c.Assert(err, IsNil)
	c.Assert(rows.Rows[0][3], HasLen, 0)
}
