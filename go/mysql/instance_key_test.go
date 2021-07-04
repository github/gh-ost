/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package mysql

import (
	"testing"

	"github.com/openark/golib/log"
	test "github.com/openark/golib/tests"
)

func init() {
	log.SetLevel(log.ERROR)
}

func TestParseInstanceKey(t *testing.T) {
	{
		key, err := ParseInstanceKey("myhost:1234")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(key.Hostname, "myhost")
		test.S(t).ExpectEquals(key.Port, 1234)
	}
	{
		key, err := ParseInstanceKey("myhost")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(key.Hostname, "myhost")
		test.S(t).ExpectEquals(key.Port, 3306)
	}
	{
		key, err := ParseInstanceKey("10.0.0.3:3307")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(key.Hostname, "10.0.0.3")
		test.S(t).ExpectEquals(key.Port, 3307)
	}
	{
		key, err := ParseInstanceKey("10.0.0.3")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(key.Hostname, "10.0.0.3")
		test.S(t).ExpectEquals(key.Port, 3306)
	}
	{
		key, err := ParseInstanceKey("[2001:db8:1f70::999:de8:7648:6e8]:3308")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(key.Hostname, "2001:db8:1f70::999:de8:7648:6e8")
		test.S(t).ExpectEquals(key.Port, 3308)
	}
	{
		key, err := ParseInstanceKey("::1")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(key.Hostname, "::1")
		test.S(t).ExpectEquals(key.Port, 3306)
	}
	{
		key, err := ParseInstanceKey("0:0:0:0:0:0:0:0")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(key.Hostname, "0:0:0:0:0:0:0:0")
		test.S(t).ExpectEquals(key.Port, 3306)
	}
	{
		_, err := ParseInstanceKey("[2001:xxxx:1f70::999:de8:7648:6e8]:3308")
		test.S(t).ExpectNotNil(err)
	}
	{
		_, err := ParseInstanceKey("10.0.0.4:")
		test.S(t).ExpectNotNil(err)
	}
	{
		_, err := ParseInstanceKey("10.0.0.4:5.6.7")
		test.S(t).ExpectNotNil(err)
	}
}
