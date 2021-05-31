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

func TestStringContainsAll(t *testing.T) {
	s := `insert,delete,update`

	test.S(t).ExpectFalse(StringContainsAll(s))
	test.S(t).ExpectFalse(StringContainsAll(s, ""))
	test.S(t).ExpectFalse(StringContainsAll(s, "drop"))
	test.S(t).ExpectTrue(StringContainsAll(s, "insert"))
	test.S(t).ExpectFalse(StringContainsAll(s, "insert", "drop"))
	test.S(t).ExpectTrue(StringContainsAll(s, "insert", ""))
	test.S(t).ExpectTrue(StringContainsAll(s, "insert", "update", "delete"))
}
