/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-osc/blob/master/LICENSE
*/

package sql

import (
	"testing"

	"github.com/outbrain/golib/log"
	test "github.com/outbrain/golib/tests"
	"reflect"
)

func init() {
	log.SetLevel(log.ERROR)
}

func TestParseColumnList(t *testing.T) {
	names := "id,category,max_len"

	columnList := ParseColumnList(names)
	test.S(t).ExpectEquals(columnList.Len(), 3)
	test.S(t).ExpectTrue(reflect.DeepEqual(columnList.Names, []string{"id", "category", "max_len"}))
	test.S(t).ExpectEquals(columnList.Ordinals["id"], 0)
	test.S(t).ExpectEquals(columnList.Ordinals["category"], 1)
	test.S(t).ExpectEquals(columnList.Ordinals["max_len"], 2)
}
