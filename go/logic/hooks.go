/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/github/gh-ost/go/base"
	"github.com/openark/golib/log"
)

const (
	onStartup            = "gh-ost-on-startup"
	onValidated          = "gh-ost-on-validated"
	onAboutToRowCopy     = "gh-ost-on-about-row-copy"
	onRowCopyComplete    = "gh-ost-on-row-copy-complete"
	onBeginPostponed     = "gh-ost-on-begin-postponed"
	onAboutToCutOver     = "gh-ost-on-about-cut-over"
	onInteractiveCommand = "gh-ost-on-interactive-command"
	onSuccess            = "gh-ost-on-success"
	onFailure            = "gh-ost-on-failure"
	onStatus             = "gh-ost-on-status"
	onStopReplication    = "gh-ost-on-stop-replication"
)

type HooksExecutor struct {
	migrationContext *base.MigrationContext
}

func NewHooksExecutor() *HooksExecutor {
	return &HooksExecutor{
		migrationContext: base.GetMigrationContext(),
	}
}

func (this *HooksExecutor) initHooks() error {
	return nil
}

func (this *HooksExecutor) applyEnvironmentVairables(extraVariables ...string) []string {
	env := os.Environ()
	env = append(env, fmt.Sprintf("GH_OST_DATABASE_NAME=%s", this.migrationContext.DatabaseName))
	env = append(env, fmt.Sprintf("GH_OST_TABLE_NAME=%s", this.migrationContext.OriginalTableName))
	env = append(env, fmt.Sprintf("GH_OST_GHOST_TABLE_NAME=%s", this.migrationContext.GetGhostTableName()))
	env = append(env, fmt.Sprintf("GH_OST_OLD_TABLE_NAME=%s", this.migrationContext.GetOldTableName()))
	env = append(env, fmt.Sprintf("GH_OST_DDL=%s", this.migrationContext.AlterStatement))
	env = append(env, fmt.Sprintf("GH_OST_ELAPSED_SECONDS=%f", this.migrationContext.ElapsedTime().Seconds()))
	env = append(env, fmt.Sprintf("GH_OST_MIGRATED_HOST=%s", this.migrationContext.ApplierConnectionConfig.ImpliedKey.Hostname))
	env = append(env, fmt.Sprintf("GH_OST_INSPECTED_HOST=%s", this.migrationContext.InspectorConnectionConfig.ImpliedKey.Hostname))
	env = append(env, fmt.Sprintf("GH_OST_EXECUTING_HOST=%s", this.migrationContext.Hostname))

	for _, variable := range extraVariables {
		env = append(env, variable)
	}
	return env
}

// executeHook executes a command, and sets relevant environment variables
func (this *HooksExecutor) executeHook(hook string, extraVariables ...string) error {
	cmd := exec.Command(hook)
	cmd.Env = this.applyEnvironmentVairables(extraVariables...)

	if err := cmd.Run(); err != nil {
		return log.Errore(err)
	}
	return nil
}

func (this *HooksExecutor) detectHooks(baseName string) (hooks []string, err error) {
	if this.migrationContext.HooksPath == "" {
		return hooks, err
	}
	pattern := fmt.Sprintf("%s/%s*", this.migrationContext.HooksPath, baseName)
	hooks, err = filepath.Glob(pattern)
	return hooks, err
}

func (this *HooksExecutor) executeHooks(baseName string, extraVariables ...string) error {
	hooks, err := this.detectHooks(baseName)
	if err != nil {
		return err
	}
	for _, hook := range hooks {
		if err := this.executeHook(hook, extraVariables...); err != nil {
			return err
		}
	}
	return nil
}

func (this *HooksExecutor) onStartup() error {
	return this.executeHooks(onStartup)
}

func (this *HooksExecutor) onValidated() error {
	return this.executeHooks(onValidated)
}

func (this *HooksExecutor) onAboutToRowCopy() error {
	return this.executeHooks(onAboutToRowCopy)
}

func (this *HooksExecutor) onRowCopyComplete() error {
	return this.executeHooks(onRowCopyComplete)
}

func (this *HooksExecutor) onBeginPostponed() error {
	return this.executeHooks(onBeginPostponed)
}

func (this *HooksExecutor) onAboutToCutOver() error {
	return this.executeHooks(onAboutToCutOver)
}

func (this *HooksExecutor) onInteractiveCommand(command string) error {
	v := fmt.Sprintf("GH_OST_COMMAND='%s'", command)
	return this.executeHooks(onInteractiveCommand, v)
}

func (this *HooksExecutor) onSuccess() error {
	return this.executeHooks(onSuccess)
}

func (this *HooksExecutor) onFailure() error {
	return this.executeHooks(onFailure)
}

func (this *HooksExecutor) onStatus(statusMessage string) error {
	v := fmt.Sprintf("GH_OST_STATUS='%s'", statusMessage)
	return this.executeHooks(onStatus, v)
}

func (this *HooksExecutor) onStopReplication() error {
	return this.executeHooks(onStopReplication)
}
