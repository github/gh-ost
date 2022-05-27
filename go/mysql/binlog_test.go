/*
   Copyright 2022 GitHub Inc.
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

func TestBinlogCoordinatesToImplementation(t *testing.T) {
	test.S(t).ExpectNil(binlogCoordinatesToImplementation(
		&FileBinlogCoordinates{},
		&FileBinlogCoordinates{},
	))
	test.S(t).ExpectNil(binlogCoordinatesToImplementation(
		&GTIDBinlogCoordinates{},
		&GTIDBinlogCoordinates{},
	))
	test.S(t).ExpectNotNil(binlogCoordinatesToImplementation(
		&FileBinlogCoordinates{},
		&GTIDBinlogCoordinates{},
	))
	test.S(t).ExpectNotNil(binlogCoordinatesToImplementation(
		&FileBinlogCoordinates{},
		map[string]string{},
	))
}
