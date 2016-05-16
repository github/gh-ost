/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package binlog

import (
	"bufio"
	"os"
	"testing"

	"github.com/outbrain/golib/log"
	test "github.com/outbrain/golib/tests"
)

func init() {
	log.SetLevel(log.ERROR)
}

func __TestRBRSample0(t *testing.T) {
	testFile, err := os.Open("testdata/rbr-sample-0.txt")
	test.S(t).ExpectNil(err)
	defer testFile.Close()

	scanner := bufio.NewScanner(testFile)
	entries, err := parseEntries(scanner)
	test.S(t).ExpectNil(err)

	test.S(t).ExpectEquals(len(entries), 17)
	test.S(t).ExpectEquals(entries[0].DatabaseName, "test")
	test.S(t).ExpectEquals(entries[0].TableName, "samplet")
	test.S(t).ExpectEquals(entries[0].StatementType, "INSERT")
	test.S(t).ExpectEquals(entries[1].StatementType, "INSERT")
	test.S(t).ExpectEquals(entries[2].StatementType, "INSERT")
	test.S(t).ExpectEquals(entries[3].StatementType, "INSERT")
	test.S(t).ExpectEquals(entries[4].StatementType, "INSERT")
	test.S(t).ExpectEquals(entries[5].StatementType, "INSERT")
	test.S(t).ExpectEquals(entries[6].StatementType, "UPDATE")
	test.S(t).ExpectEquals(entries[7].StatementType, "DELETE")
	test.S(t).ExpectEquals(entries[8].StatementType, "UPDATE")
	test.S(t).ExpectEquals(entries[9].StatementType, "INSERT")
	test.S(t).ExpectEquals(entries[10].StatementType, "INSERT")
	test.S(t).ExpectEquals(entries[11].StatementType, "DELETE")
	test.S(t).ExpectEquals(entries[12].StatementType, "DELETE")
	test.S(t).ExpectEquals(entries[13].StatementType, "INSERT")
	test.S(t).ExpectEquals(entries[14].StatementType, "UPDATE")
	test.S(t).ExpectEquals(entries[15].StatementType, "DELETE")
	test.S(t).ExpectEquals(entries[16].StatementType, "INSERT")
}
