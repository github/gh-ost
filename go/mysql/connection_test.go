/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package mysql

import (
	"testing"

	test "github.com/outbrain/golib/tests"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
)

func init() {
	log.SetLevel(log.LEVEL_ERROR)
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
