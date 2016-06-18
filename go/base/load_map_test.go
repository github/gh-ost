/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package base

import (
	"testing"

	"github.com/outbrain/golib/log"
	test "github.com/outbrain/golib/tests"
)

func init() {
	log.SetLevel(log.ERROR)
}

func TestParseLoadMap(t *testing.T) {
	{
		loadList := ""
		m, err := ParseLoadMap(loadList)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(len(m), 0)
	}
	{
		loadList := "threads_running=20,threads_connected=10"
		m, err := ParseLoadMap(loadList)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(len(m), 2)
		test.S(t).ExpectEquals(m["threads_running"], int64(20))
		test.S(t).ExpectEquals(m["threads_connected"], int64(10))
	}
	{
		loadList := "threads_running=20=30,threads_connected=10"
		_, err := ParseLoadMap(loadList)
		test.S(t).ExpectNotNil(err)
	}
	{
		loadList := "threads_running=20,threads_connected"
		_, err := ParseLoadMap(loadList)
		test.S(t).ExpectNotNil(err)
	}
}

func TestString(t *testing.T) {
	{
		m, _ := ParseLoadMap("")
		s := m.String()
		test.S(t).ExpectEquals(s, "")
	}
	{
		loadList := "threads_running=20,threads_connected=10"
		m, _ := ParseLoadMap(loadList)
		s := m.String()
		test.S(t).ExpectEquals(s, "threads_connected=10,threads_running=20")
	}
}
