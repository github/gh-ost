/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-osc/blob/master/LICENSE
*/

package base

import ()

type MigrationContext struct {
	DatabaseName      string
	OriginalTableName string
	GhostTableName    string
}

var Context = newMigrationContext()

func newMigrationContext() *MigrationContext {
	return &MigrationContext{}
}
