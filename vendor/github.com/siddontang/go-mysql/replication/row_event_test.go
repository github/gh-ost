package replication

import (
	"fmt"

	. "github.com/pingcap/check"
)

type testDecodeSuite struct{}

var _ = Suite(&testDecodeSuite{})

type decodeDecimalChecker struct {
	*CheckerInfo
}

func (_ *decodeDecimalChecker) Check(params []interface{}, names []string) (bool, string) {
	var test int
	val := struct {
		Value  float64
		Pos    int
		Err    error
		EValue float64
		EPos   int
		EErr   error
	}{}

	for i, name := range names {
		switch name {
		case "obtainedValue":
			val.Value, _ = params[i].(float64)
		case "obtainedPos":
			val.Pos, _ = params[i].(int)
		case "obtainedErr":
			val.Err, _ = params[i].(error)
		case "expectedValue":
			val.EValue, _ = params[i].(float64)
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
	if val.Value != val.EValue {
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
		Expected    float64
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
		{[]byte{117, 200, 127, 255}, 4, 2, float64(-10.55), 2, nil},
		{[]byte{127, 255, 244, 127, 245}, 5, 0, float64(-11), 3, nil},
		{[]byte{127, 245, 253, 217, 127, 255}, 7, 3, float64(-10.550), 4, nil},
		{[]byte{127, 255, 255, 245, 200, 127, 255}, 10, 2, float64(-10.55), 5, nil},
		{[]byte{127, 255, 255, 245, 253, 217, 127, 255}, 10, 3, float64(-10.550), 6, nil},
		{[]byte{127, 255, 255, 255, 245, 200, 118, 196}, 13, 2, float64(-10.55), 6, nil},
		{[]byte{118, 196, 101, 54, 0, 254, 121, 96, 127, 255}, 15, 14, float64(-9.99999999999999), 8, nil},
		{[]byte{127, 255, 255, 255, 245, 223, 55, 170, 127, 255, 127, 255}, 20, 10, float64(-10.5500000000), 10, nil},
		{[]byte{127, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 245, 255, 41, 39, 127, 255}, 30, 5, float64(-10.55000), 15, nil},
		{[]byte{127, 255, 255, 255, 245, 223, 55, 170, 127, 255, 255, 255, 255, 255, 127, 255}, 30, 20, float64(-10.55000000000000000000), 14, nil},
		{[]byte{127, 255, 245, 223, 55, 170, 127, 255, 255, 255, 255, 255, 255, 255, 255, 4, 0}, 30, 25, float64(-10.5500000000000000000000000), 15, nil},
		{[]byte{128, 1, 128, 0}, 4, 2, float64(0.01), 2, nil},
		{[]byte{128, 0, 0, 128, 0}, 5, 0, float64(0), 3, nil},
		{[]byte{128, 0, 0, 12, 128, 0}, 7, 3, float64(0.012), 4, nil},
		{[]byte{128, 0, 0, 0, 1, 128, 0}, 10, 2, float64(0.01), 5, nil},
		{[]byte{128, 0, 0, 0, 0, 12, 128, 0}, 10, 3, float64(0.012), 6, nil},
		{[]byte{128, 0, 0, 0, 0, 1, 128, 0}, 13, 2, float64(0.01), 6, nil},
		{[]byte{128, 0, 188, 97, 78, 1, 96, 11, 128, 0}, 15, 14, float64(0.01234567890123), 8, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 188, 97, 78, 9, 128, 0}, 20, 10, float64(0.0123456789), 10, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 211, 128, 0}, 30, 5, float64(0.01235), 15, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 188, 97, 78, 53, 183, 191, 135, 89, 128, 0}, 30, 20, float64(0.01234567890123456789), 14, nil},
		{[]byte{128, 0, 0, 0, 188, 97, 78, 53, 183, 191, 135, 0, 135, 253, 217, 30, 0}, 30, 25, float64(0.0123456789012345678912345), 15, nil},
		{[]byte{227, 99, 128, 48}, 4, 2, float64(99.99), 2, nil},
		{[]byte{128, 48, 57, 167, 15}, 5, 0, float64(12345), 3, nil},
		{[]byte{167, 15, 3, 231, 128, 0}, 7, 3, float64(9999.999), 4, nil},
		{[]byte{128, 0, 48, 57, 0, 128, 0}, 10, 2, float64(12345.00), 5, nil},
		{[]byte{128, 0, 48, 57, 0, 0, 128, 0}, 10, 3, float64(12345.000), 6, nil},
		{[]byte{128, 0, 0, 48, 57, 0, 137, 59}, 13, 2, float64(12345.00), 6, nil},
		{[]byte{137, 59, 154, 201, 255, 1, 134, 159, 128, 0}, 15, 14, float64(9.99999999999999), 8, nil},
		{[]byte{128, 0, 0, 48, 57, 0, 0, 0, 0, 0, 128, 0}, 20, 10, float64(12345.0000000000), 10, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 48, 57, 0, 0, 0, 128, 0}, 30, 5, float64(12345.00000), 15, nil},
		{[]byte{128, 0, 0, 48, 57, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 48}, 30, 20, float64(12345.00000000000000000000), 14, nil},
		{[]byte{128, 48, 57, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 0}, 30, 25, float64(12345.0000000000000000000000000), 15, nil},
		{[]byte{227, 99, 128, 48}, 4, 2, float64(99.99), 2, nil},
		{[]byte{128, 48, 57, 167, 15}, 5, 0, float64(12345), 3, nil},
		{[]byte{167, 15, 3, 231, 128, 0}, 7, 3, float64(9999.999), 4, nil},
		{[]byte{128, 0, 48, 57, 0, 128, 0}, 10, 2, float64(12345.00), 5, nil},
		{[]byte{128, 0, 48, 57, 0, 0, 128, 0}, 10, 3, float64(12345.000), 6, nil},
		{[]byte{128, 0, 0, 48, 57, 0, 137, 59}, 13, 2, float64(12345.00), 6, nil},
		{[]byte{137, 59, 154, 201, 255, 1, 134, 159, 128, 0}, 15, 14, float64(9.99999999999999), 8, nil},
		{[]byte{128, 0, 0, 48, 57, 0, 0, 0, 0, 0, 128, 0}, 20, 10, float64(12345.0000000000), 10, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 48, 57, 0, 0, 0, 128, 0}, 30, 5, float64(12345.00000), 15, nil},
		{[]byte{128, 0, 0, 48, 57, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 48}, 30, 20, float64(12345.00000000000000000000), 14, nil},
		{[]byte{128, 48, 57, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0}, 30, 25, float64(12345.0000000000000000000000000), 15, nil},
		{[]byte{227, 99, 128, 0}, 4, 2, float64(99.99), 2, nil},
		{[]byte{128, 0, 123, 128, 123}, 5, 0, float64(123), 3, nil},
		{[]byte{128, 123, 1, 194, 128, 0}, 7, 3, float64(123.450), 4, nil},
		{[]byte{128, 0, 0, 123, 45, 128, 0}, 10, 2, float64(123.45), 5, nil},
		{[]byte{128, 0, 0, 123, 1, 194, 128, 0}, 10, 3, float64(123.450), 6, nil},
		{[]byte{128, 0, 0, 0, 123, 45, 137, 59}, 13, 2, float64(123.45), 6, nil},
		{[]byte{137, 59, 154, 201, 255, 1, 134, 159, 128, 0}, 15, 14, float64(9.99999999999999), 8, nil},
		{[]byte{128, 0, 0, 0, 123, 26, 210, 116, 128, 0, 128, 0}, 20, 10, float64(123.4500000000), 10, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 123, 0, 175, 200, 128, 0}, 30, 5, float64(123.45000), 15, nil},
		{[]byte{128, 0, 0, 0, 123, 26, 210, 116, 128, 0, 0, 0, 0, 0, 128, 0}, 30, 20, float64(123.45000000000000000000), 14, nil},
		{[]byte{128, 0, 123, 26, 210, 116, 128, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0}, 30, 25, float64(123.4500000000000000000000000), 15, nil},
		{[]byte{28, 156, 127, 255}, 4, 2, float64(-99.99), 2, nil},
		{[]byte{127, 255, 132, 127, 132}, 5, 0, float64(-123), 3, nil},
		{[]byte{127, 132, 254, 61, 127, 255}, 7, 3, float64(-123.450), 4, nil},
		{[]byte{127, 255, 255, 132, 210, 127, 255}, 10, 2, float64(-123.45), 5, nil},
		{[]byte{127, 255, 255, 132, 254, 61, 127, 255}, 10, 3, float64(-123.450), 6, nil},
		{[]byte{127, 255, 255, 255, 132, 210, 118, 196}, 13, 2, float64(-123.45), 6, nil},
		{[]byte{118, 196, 101, 54, 0, 254, 121, 96, 127, 255}, 15, 14, float64(-9.99999999999999), 8, nil},
		{[]byte{127, 255, 255, 255, 132, 229, 45, 139, 127, 255, 127, 255}, 20, 10, float64(-123.4500000000), 10, nil},
		{[]byte{127, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 132, 255, 80, 55, 127, 255}, 30, 5, float64(-123.45000), 15, nil},
		{[]byte{127, 255, 255, 255, 132, 229, 45, 139, 127, 255, 255, 255, 255, 255, 127, 255}, 30, 20, float64(-123.45000000000000000000), 14, nil},
		{[]byte{127, 255, 132, 229, 45, 139, 127, 255, 255, 255, 255, 255, 255, 255, 255, 20, 0}, 30, 25, float64(-123.4500000000000000000000000), 15, nil},
		{[]byte{128, 0, 128, 0}, 4, 2, float64(0.00), 2, nil},
		{[]byte{128, 0, 0, 128, 0}, 5, 0, float64(0), 3, nil},
		{[]byte{128, 0, 0, 0, 128, 0}, 7, 3, float64(0.000), 4, nil},
		{[]byte{128, 0, 0, 0, 0, 128, 0}, 10, 2, float64(0.00), 5, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 128, 0}, 10, 3, float64(0.000), 6, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 128, 0}, 13, 2, float64(0.00), 6, nil},
		{[]byte{128, 0, 1, 226, 58, 0, 0, 99, 128, 0}, 15, 14, float64(0.00012345000099), 8, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 1, 226, 58, 0, 128, 0}, 20, 10, float64(0.0001234500), 10, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12, 128, 0}, 30, 5, float64(0.00012), 15, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 1, 226, 58, 0, 15, 18, 2, 0, 128, 0}, 30, 20, float64(0.00012345000098765000), 14, nil},
		{[]byte{128, 0, 0, 0, 1, 226, 58, 0, 15, 18, 2, 0, 0, 0, 0, 15, 0}, 30, 25, float64(0.0001234500009876500000000), 15, nil},
		{[]byte{128, 0, 128, 0}, 4, 2, float64(0.00), 2, nil},
		{[]byte{128, 0, 0, 128, 0}, 5, 0, float64(0), 3, nil},
		{[]byte{128, 0, 0, 0, 128, 0}, 7, 3, float64(0.000), 4, nil},
		{[]byte{128, 0, 0, 0, 0, 128, 0}, 10, 2, float64(0.00), 5, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 128, 0}, 10, 3, float64(0.000), 6, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 128, 0}, 13, 2, float64(0.00), 6, nil},
		{[]byte{128, 0, 1, 226, 58, 0, 0, 99, 128, 0}, 15, 14, float64(0.00012345000099), 8, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 1, 226, 58, 0, 128, 0}, 20, 10, float64(0.0001234500), 10, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12, 128, 0}, 30, 5, float64(0.00012), 15, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 1, 226, 58, 0, 15, 18, 2, 0, 128, 0}, 30, 20, float64(0.00012345000098765000), 14, nil},
		{[]byte{128, 0, 0, 0, 1, 226, 58, 0, 15, 18, 2, 0, 0, 0, 0, 22, 0}, 30, 25, float64(0.0001234500009876500000000), 15, nil},
		{[]byte{128, 12, 128, 0}, 4, 2, float64(0.12), 2, nil},
		{[]byte{128, 0, 0, 128, 0}, 5, 0, float64(0), 3, nil},
		{[]byte{128, 0, 0, 123, 128, 0}, 7, 3, float64(0.123), 4, nil},
		{[]byte{128, 0, 0, 0, 12, 128, 0}, 10, 2, float64(0.12), 5, nil},
		{[]byte{128, 0, 0, 0, 0, 123, 128, 0}, 10, 3, float64(0.123), 6, nil},
		{[]byte{128, 0, 0, 0, 0, 12, 128, 7}, 13, 2, float64(0.12), 6, nil},
		{[]byte{128, 7, 91, 178, 144, 1, 129, 205, 128, 0}, 15, 14, float64(0.12345000098765), 8, nil},
		{[]byte{128, 0, 0, 0, 0, 7, 91, 178, 145, 0, 128, 0}, 20, 10, float64(0.1234500010), 10, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 48, 57, 128, 0}, 30, 5, float64(0.12345), 15, nil},
		{[]byte{128, 0, 0, 0, 0, 7, 91, 178, 144, 58, 222, 87, 208, 0, 128, 0}, 30, 20, float64(0.12345000098765000000), 14, nil},
		{[]byte{128, 0, 0, 7, 91, 178, 144, 58, 222, 87, 208, 0, 0, 0, 0, 30, 0}, 30, 25, float64(0.1234500009876500000000000), 15, nil},
		{[]byte{128, 0, 128, 0}, 4, 2, float64(0.00), 2, nil},
		{[]byte{128, 0, 0, 128, 0}, 5, 0, float64(0), 3, nil},
		{[]byte{128, 0, 0, 0, 128, 0}, 7, 3, float64(0.000), 4, nil},
		{[]byte{128, 0, 0, 0, 0, 128, 0}, 10, 2, float64(0.00), 5, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 128, 0}, 10, 3, float64(0.000), 6, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 127, 255}, 13, 2, float64(0.00), 6, nil},
		{[]byte{127, 255, 255, 255, 243, 255, 121, 59, 127, 255}, 15, 14, float64(-0.00000001234500), 8, nil},
		{[]byte{127, 255, 255, 255, 255, 255, 255, 255, 243, 252, 128, 0}, 20, 10, float64(-0.0000000123), 10, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 127, 255}, 30, 5, float64(0.00000), 15, nil},
		{[]byte{127, 255, 255, 255, 255, 255, 255, 255, 243, 235, 111, 183, 93, 178, 127, 255}, 30, 20, float64(-0.00000001234500009877), 14, nil},
		{[]byte{127, 255, 255, 255, 255, 255, 243, 235, 111, 183, 93, 255, 139, 69, 47, 30, 0}, 30, 25, float64(-0.0000000123450000987650000), 15, nil},
		{[]byte{227, 99, 129, 134}, 4, 2, float64(99.99), 2, nil},
		{[]byte{129, 134, 159, 167, 15}, 5, 0, float64(99999), 3, nil},
		{[]byte{167, 15, 3, 231, 133, 245}, 7, 3, float64(9999.999), 4, nil},
		{[]byte{133, 245, 224, 255, 99, 128, 152}, 10, 2, float64(99999999.99), 5, nil},
		{[]byte{128, 152, 150, 127, 3, 231, 227, 59}, 10, 3, float64(9999999.999), 6, nil},
		{[]byte{227, 59, 154, 201, 255, 99, 137, 59}, 13, 2, float64(99999999999.99), 6, nil},
		{[]byte{137, 59, 154, 201, 255, 1, 134, 159, 137, 59}, 15, 14, float64(9.99999999999999), 8, nil},
		{[]byte{137, 59, 154, 201, 255, 59, 154, 201, 255, 9, 128, 0}, 20, 10, float64(9999999999.9999999999), 10, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 4, 210, 29, 205, 139, 148, 0, 195, 80, 137, 59}, 30, 5, float64(1234500009876.50000), 15, nil},
		{[]byte{137, 59, 154, 201, 255, 59, 154, 201, 255, 59, 154, 201, 255, 99, 129, 134}, 30, 20, float64(9999999999.99999999999999999999), 14, nil},
		{[]byte{129, 134, 159, 59, 154, 201, 255, 59, 154, 201, 255, 0, 152, 150, 127, 30, 0}, 30, 25, float64(99999.9999999999999999999999999), 15, nil},
		{[]byte{227, 99, 129, 134}, 4, 2, float64(99.99), 2, nil},
		{[]byte{129, 134, 159, 167, 15}, 5, 0, float64(99999), 3, nil},
		{[]byte{167, 15, 3, 231, 133, 245}, 7, 3, float64(9999.999), 4, nil},
		{[]byte{133, 245, 224, 255, 99, 128, 152}, 10, 2, float64(99999999.99), 5, nil},
		{[]byte{128, 152, 150, 127, 3, 231, 128, 6}, 10, 3, float64(9999999.999), 6, nil},
		{[]byte{128, 6, 159, 107, 199, 11, 137, 59}, 13, 2, float64(111111111.11), 6, nil},
		{[]byte{137, 59, 154, 201, 255, 1, 134, 159, 128, 6}, 15, 14, float64(9.99999999999999), 8, nil},
		{[]byte{128, 6, 159, 107, 199, 6, 142, 119, 128, 0, 128, 0}, 20, 10, float64(111111111.1100000000), 10, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 0, 6, 159, 107, 199, 0, 42, 248, 128, 6}, 30, 5, float64(111111111.11000), 15, nil},
		{[]byte{128, 6, 159, 107, 199, 6, 142, 119, 128, 0, 0, 0, 0, 0, 129, 134}, 30, 20, float64(111111111.11000000000000000000), 14, nil},
		{[]byte{129, 134, 159, 59, 154, 201, 255, 59, 154, 201, 255, 0, 152, 150, 127, 10, 0}, 30, 25, float64(99999.9999999999999999999999999), 15, nil},
		{[]byte{128, 1, 128, 0}, 4, 2, float64(0.01), 2, nil},
		{[]byte{128, 0, 0, 128, 0}, 5, 0, float64(0), 3, nil},
		{[]byte{128, 0, 0, 10, 128, 0}, 7, 3, float64(0.010), 4, nil},
		{[]byte{128, 0, 0, 0, 1, 128, 0}, 10, 2, float64(0.01), 5, nil},
		{[]byte{128, 0, 0, 0, 0, 10, 128, 0}, 10, 3, float64(0.010), 6, nil},
		{[]byte{128, 0, 0, 0, 0, 1, 128, 0}, 13, 2, float64(0.01), 6, nil},
		{[]byte{128, 0, 152, 150, 128, 0, 0, 0, 128, 0}, 15, 14, float64(0.01000000000000), 8, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 152, 150, 128, 0, 128, 0}, 20, 10, float64(0.0100000000), 10, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 232, 128, 0}, 30, 5, float64(0.01000), 15, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 152, 150, 128, 0, 0, 0, 0, 0, 128, 0}, 30, 20, float64(0.01000000000000000000), 14, nil},
		{[]byte{128, 0, 0, 0, 152, 150, 128, 0, 0, 0, 0, 0, 0, 0, 0, 7, 0}, 30, 25, float64(0.0100000000000000000000000), 15, nil},
		{[]byte{227, 99, 128, 0}, 4, 2, float64(99.99), 2, nil},
		{[]byte{128, 0, 123, 128, 123}, 5, 0, float64(123), 3, nil},
		{[]byte{128, 123, 1, 144, 128, 0}, 7, 3, float64(123.400), 4, nil},
		{[]byte{128, 0, 0, 123, 40, 128, 0}, 10, 2, float64(123.40), 5, nil},
		{[]byte{128, 0, 0, 123, 1, 144, 128, 0}, 10, 3, float64(123.400), 6, nil},
		{[]byte{128, 0, 0, 0, 123, 40, 137, 59}, 13, 2, float64(123.40), 6, nil},
		{[]byte{137, 59, 154, 201, 255, 1, 134, 159, 128, 0}, 15, 14, float64(9.99999999999999), 8, nil},
		{[]byte{128, 0, 0, 0, 123, 23, 215, 132, 0, 0, 128, 0}, 20, 10, float64(123.4000000000), 10, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 123, 0, 156, 64, 128, 0}, 30, 5, float64(123.40000), 15, nil},
		{[]byte{128, 0, 0, 0, 123, 23, 215, 132, 0, 0, 0, 0, 0, 0, 128, 0}, 30, 20, float64(123.40000000000000000000), 14, nil},
		{[]byte{128, 0, 123, 23, 215, 132, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0}, 30, 25, float64(123.4000000000000000000000000), 15, nil},
		{[]byte{28, 156, 127, 253}, 4, 2, float64(-99.99), 2, nil},
		{[]byte{127, 253, 204, 125, 205}, 5, 0, float64(-563), 3, nil},
		{[]byte{125, 205, 253, 187, 127, 255}, 7, 3, float64(-562.580), 4, nil},
		{[]byte{127, 255, 253, 205, 197, 127, 255}, 10, 2, float64(-562.58), 5, nil},
		{[]byte{127, 255, 253, 205, 253, 187, 127, 255}, 10, 3, float64(-562.580), 6, nil},
		{[]byte{127, 255, 255, 253, 205, 197, 118, 196}, 13, 2, float64(-562.58), 6, nil},
		{[]byte{118, 196, 101, 54, 0, 254, 121, 96, 127, 255}, 15, 14, float64(-9.99999999999999), 8, nil},
		{[]byte{127, 255, 255, 253, 205, 221, 109, 230, 255, 255, 127, 255}, 20, 10, float64(-562.5800000000), 10, nil},
		{[]byte{127, 255, 255, 255, 255, 255, 255, 255, 255, 255, 253, 205, 255, 29, 111, 127, 255}, 30, 5, float64(-562.58000), 15, nil},
		{[]byte{127, 255, 255, 253, 205, 221, 109, 230, 255, 255, 255, 255, 255, 255, 127, 253}, 30, 20, float64(-562.58000000000000000000), 14, nil},
		{[]byte{127, 253, 205, 221, 109, 230, 255, 255, 255, 255, 255, 255, 255, 255, 255, 13, 0}, 30, 25, float64(-562.5800000000000000000000000), 15, nil},
		{[]byte{28, 156, 127, 241}, 4, 2, float64(-99.99), 2, nil},
		{[]byte{127, 241, 140, 113, 140}, 5, 0, float64(-3699), 3, nil},
		{[]byte{113, 140, 255, 245, 127, 255}, 7, 3, float64(-3699.010), 4, nil},
		{[]byte{127, 255, 241, 140, 254, 127, 255}, 10, 2, float64(-3699.01), 5, nil},
		{[]byte{127, 255, 241, 140, 255, 245, 127, 255}, 10, 3, float64(-3699.010), 6, nil},
		{[]byte{127, 255, 255, 241, 140, 254, 118, 196}, 13, 2, float64(-3699.01), 6, nil},
		{[]byte{118, 196, 101, 54, 0, 254, 121, 96, 127, 255}, 15, 14, float64(-9.99999999999999), 8, nil},
		{[]byte{127, 255, 255, 241, 140, 255, 103, 105, 127, 255, 127, 255}, 20, 10, float64(-3699.0100000000), 10, nil},
		{[]byte{127, 255, 255, 255, 255, 255, 255, 255, 255, 255, 241, 140, 255, 252, 23, 127, 255}, 30, 5, float64(-3699.01000), 15, nil},
		{[]byte{127, 255, 255, 241, 140, 255, 103, 105, 127, 255, 255, 255, 255, 255, 127, 241}, 30, 20, float64(-3699.01000000000000000000), 14, nil},
		{[]byte{127, 241, 140, 255, 103, 105, 127, 255, 255, 255, 255, 255, 255, 255, 255, 13, 0}, 30, 25, float64(-3699.0100000000000000000000000), 15, nil},
		{[]byte{28, 156, 127, 248}, 4, 2, float64(-99.99), 2, nil},
		{[]byte{127, 248, 99, 120, 99}, 5, 0, float64(-1948), 3, nil},
		{[]byte{120, 99, 255, 115, 127, 255}, 7, 3, float64(-1948.140), 4, nil},
		{[]byte{127, 255, 248, 99, 241, 127, 255}, 10, 2, float64(-1948.14), 5, nil},
		{[]byte{127, 255, 248, 99, 255, 115, 127, 255}, 10, 3, float64(-1948.140), 6, nil},
		{[]byte{127, 255, 255, 248, 99, 241, 118, 196}, 13, 2, float64(-1948.14), 6, nil},
		{[]byte{118, 196, 101, 54, 0, 254, 121, 96, 127, 255}, 15, 14, float64(-9.99999999999999), 8, nil},
		{[]byte{127, 255, 255, 248, 99, 247, 167, 196, 255, 255, 127, 255}, 20, 10, float64(-1948.1400000000), 10, nil},
		{[]byte{127, 255, 255, 255, 255, 255, 255, 255, 255, 255, 248, 99, 255, 201, 79, 127, 255}, 30, 5, float64(-1948.14000), 15, nil},
		{[]byte{127, 255, 255, 248, 99, 247, 167, 196, 255, 255, 255, 255, 255, 255, 127, 248}, 30, 20, float64(-1948.14000000000000000000), 14, nil},
		{[]byte{127, 248, 99, 247, 167, 196, 255, 255, 255, 255, 255, 255, 255, 255, 255, 13, 0}, 30, 25, float64(-1948.1400000000000000000000000), 15, nil},
	}
	for i, tc := range testcases {
		value, pos, err := decodeDecimal(tc.Data, tc.Precision, tc.Decimals)
		c.Assert(value, DecodeDecimalsEquals, pos, err, tc.Expected, tc.ExpectedPos, tc.ExpectedErr, i)
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
