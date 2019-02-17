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

func TestIsSmallerMajorVersion(t *testing.T) {
	i55 := "5.5"
	i5517 := "5.5.17"
	i56 := "5.6"
	i8013 := "8.0.13"

	test.S(t).ExpectFalse(IsSmallerMajorVersion(i55, i5517))
	test.S(t).ExpectFalse(IsSmallerMajorVersion(i56, i5517))
	test.S(t).ExpectTrue(IsSmallerMajorVersion(i55, i56))
	test.S(t).ExpectTrue(IsSmallerMajorVersion(i56, i8013))
	test.S(t).ExpectFalse(IsSmallerMajorVersion(i8013, i56))
}
