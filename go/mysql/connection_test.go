/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package mysql

import (
	"testing"

	"github.com/outbrain/golib/log"
	test "github.com/outbrain/golib/tests"
)

func init() {
	log.SetLevel(log.ERROR)
}

func TestNewConnectionConfig(t *testing.T) {
	c := NewConnectionConfig()
	test.S(t).ExpectEquals(c.Key.Hostname, "")
	test.S(t).ExpectEquals(c.Key.Port, 0)
	test.S(t).ExpectEquals(c.ImpliedKey.Hostname, "")
	test.S(t).ExpectEquals(c.ImpliedKey.Port, 0)
	test.S(t).ExpectEquals(c.User, "")
	test.S(t).ExpectEquals(c.Password, "")
}

func TestDuplicateCredentials(t *testing.T) {
	c := NewConnectionConfig()
	c.Key = InstanceKey{Hostname: "myhost", Port: 3306}
	c.User = "gromit"
	c.Password = "penguin"

	dup := c.DuplicateCredentials(InstanceKey{Hostname: "otherhost", Port: 3310})
	test.S(t).ExpectEquals(dup.Key.Hostname, "otherhost")
	test.S(t).ExpectEquals(dup.Key.Port, 3310)
	test.S(t).ExpectEquals(dup.ImpliedKey.Hostname, "otherhost")
	test.S(t).ExpectEquals(dup.ImpliedKey.Port, 3310)
	test.S(t).ExpectEquals(dup.User, "gromit")
	test.S(t).ExpectEquals(dup.Password, "penguin")
}

func TestDuplicate(t *testing.T) {
	c := NewConnectionConfig()
	c.Key = InstanceKey{Hostname: "myhost", Port: 3306}
	c.User = "gromit"
	c.Password = "penguin"

	dup := c.Duplicate()
	test.S(t).ExpectEquals(dup.Key.Hostname, "myhost")
	test.S(t).ExpectEquals(dup.Key.Port, 3306)
	test.S(t).ExpectEquals(dup.ImpliedKey.Hostname, "myhost")
	test.S(t).ExpectEquals(dup.ImpliedKey.Port, 3306)
	test.S(t).ExpectEquals(dup.User, "gromit")
	test.S(t).ExpectEquals(dup.Password, "penguin")
}
func TestGetDBUri(t *testing.T) {
	c := NewConnectionConfig()
	c.Key = InstanceKey{Hostname: "myhost", Port: 3306}
	c.User = "gromit"
	c.Password = "penguin"

	uri := c.GetDBUri("test", true)
	test.S(t).ExpectEquals(uri, "gromit:penguin@tcp(myhost:3306)/test?interpolateParams=false&autocommit=true&charset=utf8mb4,utf8,latin1,gbk,gb2312,big5,cp932,sjis")
	uri = c.GetDBUri("test", false)
	test.S(t).ExpectEquals(uri, "gromit:penguin@tcp(myhost:3306)/test?interpolateParams=true&autocommit=true&charset=utf8mb4,utf8,latin1")
}
