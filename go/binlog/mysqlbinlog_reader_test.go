/*
   Copyright 2014 Outbrain Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
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

func TestRBRSample0(t *testing.T) {
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
