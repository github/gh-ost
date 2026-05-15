/*
   Copyright 2026 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package base

// Hooks is the set of lifecycle callbacks gh-ost invokes during a migration.
type Hooks interface {
	OnStartup() error
	OnValidated() error
	OnRowCountComplete() error
	OnBeforeRowCopy() error
	OnRowCopyComplete() error
	OnBeginPostponed() error
	OnBeforeCutOver() error
	OnInteractiveCommand(command string) error
	OnSuccess(instantDDL bool) error
	OnFailure() error
	OnBatchCopyRetry(errorMessage string) error
	OnStatus(statusMessage string) error
	OnStopReplication() error
	OnStartReplication() error
}
