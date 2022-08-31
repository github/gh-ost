/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"testing"

	"github.com/openark/golib/log"
	test "github.com/openark/golib/tests"
)

func init() {
	log.SetLevel(log.ERROR)
}

func TestGrantMatch(t *testing.T) {
	databaseName := `my_database%name`

	test.S(t).ExpectFalse(grantMatch("GRANT SELECT ON *.* TO 'user'@'%'", databaseName))
	test.S(t).ExpectFalse(grantMatch("GRANT SELECT ON `other\\_database\\%name`.* TO 'user'@'%'", databaseName))
	test.S(t).ExpectTrue(grantMatch("GRANT SELECT ON `my\\_database\\%name`.* TO 'user'@'%'", databaseName))
	test.S(t).ExpectTrue(grantMatch("GRANT SELECT ON `my_database_name`.* TO 'user'@'%'", databaseName))
	test.S(t).ExpectTrue(grantMatch("GRANT SELECT ON `my\\_database\\%%`.* TO 'user'@'%'", databaseName))
	test.S(t).ExpectFalse(grantMatch("GRANT SELECT ON `database\\%%`.* TO 'user'@'%'", databaseName))
	test.S(t).ExpectTrue(grantMatch("GRANT SELECT ON `my\\_database\\%____`.* TO 'user'@'%'", databaseName))
	test.S(t).ExpectFalse(grantMatch("GRANT SELECT ON `my\\_database\\%_`.* TO 'user'@'%'", databaseName))
	test.S(t).ExpectTrue(grantMatch("GRANT SELECT ON `%database%`.* TO 'user'@'%'", databaseName))
}
