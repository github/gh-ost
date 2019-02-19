/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package mysql

import (
	"reflect"
	"testing"

	"github.com/outbrain/golib/log"
	test "github.com/outbrain/golib/tests"
)

func init() {
	log.SetLevel(log.ERROR)
}

func TestVersionTokens(t *testing.T) {
	test.S(t).ExpectTrue(reflect.DeepEqual(versionTokens("5.7.24-log", 3), []int{5, 7, 24}))
	test.S(t).ExpectTrue(reflect.DeepEqual(versionTokens("8.0.13", 3), []int{8, 0, 13}))
	test.S(t).ExpectTrue(reflect.DeepEqual(versionTokens("5.5", 2), []int{5, 5}))
	test.S(t).ExpectTrue(reflect.DeepEqual(versionTokens("5.5", 3), []int{5, 5, 0}))
	test.S(t).ExpectTrue(reflect.DeepEqual(versionTokens("5.5-log", 3), []int{5, 5, 0}))
}

func TestIsSmallerMajorVersion(t *testing.T) {
	i55 := "5.5"
	i5516 := "5.5.16"
	i5517 := "5.5.17"
	i56 := "5.6"
	i8013 := "8.0.13"

	test.S(t).ExpectFalse(IsSmallerMajorVersion(i55, i5517))
	test.S(t).ExpectFalse(IsSmallerMajorVersion(i5516, i5517))
	test.S(t).ExpectFalse(IsSmallerMajorVersion(i56, i5517))
	test.S(t).ExpectTrue(IsSmallerMajorVersion(i55, i56))
	test.S(t).ExpectTrue(IsSmallerMajorVersion(i56, i8013))
	test.S(t).ExpectFalse(IsSmallerMajorVersion(i8013, i56))
}

func TestIsSmallerMinorVersion(t *testing.T) {
	i55 := "5.5"
	i5516 := "5.5.16"
	i5517 := "5.5.17"
	i56 := "5.6"
	i8013 := "8.0.13"

	test.S(t).ExpectTrue(IsSmallerMinorVersion(i55, i5517))
	test.S(t).ExpectTrue(IsSmallerMinorVersion(i5516, i5517))
	test.S(t).ExpectFalse(IsSmallerMinorVersion(i56, i5517))
	test.S(t).ExpectTrue(IsSmallerMinorVersion(i55, i56))
	test.S(t).ExpectTrue(IsSmallerMinorVersion(i56, i8013))
	test.S(t).ExpectFalse(IsSmallerMinorVersion(i8013, i56))
}
