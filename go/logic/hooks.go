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

func (this *HooksExecutor) applyEnvironmentVairables() []string {
	env := os.Environ()
	env = append(env, fmt.Sprintf("GH_OST_DATABASE_NAME=%s", this.migrationContext.DatabaseName))
	env = append(env, fmt.Sprintf("GH_OST_TABLE_NAME=%s", this.migrationContext.OriginalTableName))
	env = append(env, fmt.Sprintf("GH_OST_GHOST_TABLE_NAME=%s", this.migrationContext.GetGhostTableName()))
	env = append(env, fmt.Sprintf("GH_OST_OLD_TABLE_NAME=%s", this.migrationContext.GetOldTableName()))
	env = append(env, fmt.Sprintf("GH_OST_DDL=%s", this.migrationContext.AlterStatement))
	env = append(env, fmt.Sprintf("GH_OST_ELAPSED_SECONDS=%f", this.migrationContext.ElapsedTime().Seconds()))
	return env
}

// executeHook executes a command with arguments, and set relevant environment variables
func (this *HooksExecutor) executeHook(hook string, arguments ...string) error {
	cmd := exec.Command(hook, arguments...)
	cmd.Env = this.applyEnvironmentVairables()

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

func (this *HooksExecutor) executeHooks(baseName string) error {
	hooks, err := this.detectHooks(baseName)
	if err != nil {
		return err
	}
	for _, hook := range hooks {
		if err := this.executeHook(hook); err != nil {
			return err
		}
	}
	return nil
}

func (this *HooksExecutor) onStartup() error {
	return nil
}

func (this *HooksExecutor) onValidated() error {
	return nil
}

func (this *HooksExecutor) onAboutToRowCopy() error {
	return nil
}

func (this *HooksExecutor) onRowCopyComplete() error {
	return nil
}

func (this *HooksExecutor) onBeginPostponed() error {
	return nil
}

func (this *HooksExecutor) onAboutToCutOver() error {
	return nil
}

func (this *HooksExecutor) onInteractiveCommand(command string) error {
	return nil
}

func (this *HooksExecutor) onSuccess() error {
	return nil
}

func (this *HooksExecutor) onFailure() error {
	return nil
}
