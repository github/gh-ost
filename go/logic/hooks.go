/*
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
	"sync/atomic"

	"github.com/github/gh-ost/go/base"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
)

const (
	onStartup            = "gh-ost-on-startup"
	onValidated          = "gh-ost-on-validated"
	onRowCountComplete   = "gh-ost-on-rowcount-complete"
	onBeforeRowCopy      = "gh-ost-on-before-row-copy"
	onRowCopyComplete    = "gh-ost-on-row-copy-complete"
	onBeginPostponed     = "gh-ost-on-begin-postponed"
	onBeforeCutOver      = "gh-ost-on-before-cut-over"
	onInteractiveCommand = "gh-ost-on-interactive-command"
	onSuccess            = "gh-ost-on-success"
	onFailure            = "gh-ost-on-failure"
	onStatus             = "gh-ost-on-status"
	onStopReplication    = "gh-ost-on-stop-replication"
	onStartReplication   = "gh-ost-on-start-replication"
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

// 构建新的环境变量
// GH_OST的很多信息也放环境变量中
//
func (this *HooksExecutor) applyEnvironmentVairables(extraVariables ...string) []string {
	env := os.Environ()
	env = append(env, fmt.Sprintf("GH_OST_DATABASE_NAME=%s", this.migrationContext.DatabaseName))
	env = append(env, fmt.Sprintf("GH_OST_TABLE_NAME=%s", this.migrationContext.OriginalTableName))
	env = append(env, fmt.Sprintf("GH_OST_GHOST_TABLE_NAME=%s", this.migrationContext.GetGhostTableName()))
	env = append(env, fmt.Sprintf("GH_OST_OLD_TABLE_NAME=%s", this.migrationContext.GetOldTableName()))
	env = append(env, fmt.Sprintf("GH_OST_DDL=%s", this.migrationContext.AlterStatement))
	env = append(env, fmt.Sprintf("GH_OST_ELAPSED_SECONDS=%f", this.migrationContext.ElapsedTime().Seconds()))
	env = append(env, fmt.Sprintf("GH_OST_ELAPSED_COPY_SECONDS=%f", this.migrationContext.ElapsedRowCopyTime().Seconds()))
	estimatedRows := atomic.LoadInt64(&this.migrationContext.RowsEstimate) + atomic.LoadInt64(&this.migrationContext.RowsDeltaEstimate)
	env = append(env, fmt.Sprintf("GH_OST_ESTIMATED_ROWS=%d", estimatedRows))
	totalRowsCopied := this.migrationContext.GetTotalRowsCopied()
	env = append(env, fmt.Sprintf("GH_OST_COPIED_ROWS=%d", totalRowsCopied))
	env = append(env, fmt.Sprintf("GH_OST_MIGRATED_HOST=%s", this.migrationContext.GetApplierHostname()))
	env = append(env, fmt.Sprintf("GH_OST_INSPECTED_HOST=%s", this.migrationContext.GetInspectorHostname()))
	env = append(env, fmt.Sprintf("GH_OST_EXECUTING_HOST=%s", this.migrationContext.Hostname))
	env = append(env, fmt.Sprintf("GH_OST_HOOKS_HINT=%s", this.migrationContext.HooksHintMessage))

	for _, variable := range extraVariables {
		env = append(env, variable)
	}
	return env
}

// executeHook executes a command, and sets relevant environment variables
// combined output & error are printed to gh-ost's standard error.
func (this *HooksExecutor) executeHook(hook string, extraVariables ...string) error {
	// gt-ost 如何给Hook传递信息？
	// 通过环境变量来传递
	cmd := exec.Command(hook)
	cmd.Env = this.applyEnvironmentVairables(extraVariables...)

	combinedOutput, err := cmd.CombinedOutput()
	fmt.Fprintln(os.Stderr, string(combinedOutput))
	log.ErrorErrorf(err, "executeHook failed")
	return err
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
	// 检查所有的hooks
	hooks, err := this.detectHooks(baseName)
	if err != nil {
		return err
	}

	// 遍历执行每一个Hook
	for _, hook := range hooks {
		log.Infof("executing %+v hook: %+v", baseName, hook)
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

func (this *HooksExecutor) onRowCountComplete() error {
	return this.executeHooks(onRowCountComplete)
}
func (this *HooksExecutor) onBeforeRowCopy() error {
	return this.executeHooks(onBeforeRowCopy)
}

func (this *HooksExecutor) onRowCopyComplete() error {
	return this.executeHooks(onRowCopyComplete)
}

func (this *HooksExecutor) onBeginPostponed() error {
	return this.executeHooks(onBeginPostponed)
}

func (this *HooksExecutor) onBeforeCutOver() error {
	return this.executeHooks(onBeforeCutOver)
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

func (this *HooksExecutor) onStartReplication() error {
	return this.executeHooks(onStartReplication)
}
