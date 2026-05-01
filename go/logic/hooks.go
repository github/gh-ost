/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"

	"github.com/github/gh-ost/go/base"
	"github.com/openark/golib/log"
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
	onBatchCopyRetry     = "gh-ost-on-batch-copy-retry"
	onStatus             = "gh-ost-on-status"
	onStopReplication    = "gh-ost-on-stop-replication"
	onStartReplication   = "gh-ost-on-start-replication"
)

type HooksExecutor struct {
	migrationContext *base.MigrationContext
	writer           io.Writer
}

func NewHooksExecutor(migrationContext *base.MigrationContext) *HooksExecutor {
	return &HooksExecutor{
		migrationContext: migrationContext,
		writer:           os.Stderr,
	}
}

func (he *HooksExecutor) applyEnvironmentVariables(extraVariables ...string) []string {
	env := os.Environ()
	env = append(env, fmt.Sprintf("GH_OST_DATABASE_NAME=%s", he.migrationContext.DatabaseName))
	env = append(env, fmt.Sprintf("GH_OST_TABLE_NAME=%s", he.migrationContext.OriginalTableName))
	env = append(env, fmt.Sprintf("GH_OST_GHOST_TABLE_NAME=%s", he.migrationContext.GetGhostTableName()))
	env = append(env, fmt.Sprintf("GH_OST_OLD_TABLE_NAME=%s", he.migrationContext.GetOldTableName()))
	env = append(env, fmt.Sprintf("GH_OST_DDL=%s", he.migrationContext.AlterStatement))
	env = append(env, fmt.Sprintf("GH_OST_ELAPSED_SECONDS=%f", he.migrationContext.ElapsedTime().Seconds()))
	env = append(env, fmt.Sprintf("GH_OST_ELAPSED_COPY_SECONDS=%f", he.migrationContext.ElapsedRowCopyTime().Seconds()))
	estimatedRows := atomic.LoadInt64(&he.migrationContext.RowsEstimate) + atomic.LoadInt64(&he.migrationContext.RowsDeltaEstimate)
	env = append(env, fmt.Sprintf("GH_OST_ESTIMATED_ROWS=%d", estimatedRows))
	totalRowsCopied := he.migrationContext.GetTotalRowsCopied()
	env = append(env, fmt.Sprintf("GH_OST_COPIED_ROWS=%d", totalRowsCopied))
	env = append(env, fmt.Sprintf("GH_OST_MIGRATED_HOST=%s", he.migrationContext.GetApplierHostname()))
	env = append(env, fmt.Sprintf("GH_OST_INSPECTED_HOST=%s", he.migrationContext.GetInspectorHostname()))
	env = append(env, fmt.Sprintf("GH_OST_EXECUTING_HOST=%s", he.migrationContext.Hostname))
	env = append(env, fmt.Sprintf("GH_OST_INSPECTED_LAG=%f", he.migrationContext.GetCurrentLagDuration().Seconds()))
	env = append(env, fmt.Sprintf("GH_OST_HEARTBEAT_LAG=%f", he.migrationContext.TimeSinceLastHeartbeatOnChangelog().Seconds()))
	env = append(env, fmt.Sprintf("GH_OST_PROGRESS=%f", he.migrationContext.GetProgressPct()))
	env = append(env, fmt.Sprintf("GH_OST_ETA_SECONDS=%d", he.migrationContext.GetETASeconds()))
	env = append(env, fmt.Sprintf("GH_OST_HOOKS_HINT=%s", he.migrationContext.HooksHintMessage))
	env = append(env, fmt.Sprintf("GH_OST_HOOKS_HINT_OWNER=%s", he.migrationContext.HooksHintOwner))
	env = append(env, fmt.Sprintf("GH_OST_HOOKS_HINT_TOKEN=%s", he.migrationContext.HooksHintToken))
	env = append(env, fmt.Sprintf("GH_OST_DRY_RUN=%t", he.migrationContext.Noop))
	env = append(env, fmt.Sprintf("GH_OST_REVERT=%t", he.migrationContext.Revert))

	env = append(env, extraVariables...)
	return env
}

// executeHook executes a command, and sets relevant environment variables
// combined output & error are printed to the configured writer.
func (he *HooksExecutor) executeHook(hook string, extraVariables ...string) error {
	he.migrationContext.Log.Infof("executing hook: %+v", hook)
	cmd := exec.Command(hook)
	cmd.Env = he.applyEnvironmentVariables(extraVariables...)

	combinedOutput, err := cmd.CombinedOutput()
	fmt.Fprintln(he.writer, string(combinedOutput))
	return log.Errore(err)
}

func (he *HooksExecutor) detectHooks(baseName string) (hooks []string, err error) {
	if he.migrationContext.HooksPath == "" {
		return hooks, err
	}
	pattern := fmt.Sprintf("%s/%s*", he.migrationContext.HooksPath, baseName)
	hooks, err = filepath.Glob(pattern)
	return hooks, err
}

func (he *HooksExecutor) executeHooks(baseName string, extraVariables ...string) error {
	hooks, err := he.detectHooks(baseName)
	if err != nil {
		return err
	}
	for _, hook := range hooks {
		log.Infof("executing %+v hook: %+v", baseName, hook)
		if err := he.executeHook(hook, extraVariables...); err != nil {
			return err
		}
	}
	return nil
}

func (he *HooksExecutor) onStartup() error {
	return he.executeHooks(onStartup)
}

func (he *HooksExecutor) onValidated() error {
	return he.executeHooks(onValidated)
}

func (he *HooksExecutor) onRowCountComplete() error {
	return he.executeHooks(onRowCountComplete)
}
func (he *HooksExecutor) onBeforeRowCopy() error {
	return he.executeHooks(onBeforeRowCopy)
}

func (he *HooksExecutor) onBatchCopyRetry(errorMessage string) error {
	v := fmt.Sprintf("GH_OST_LAST_BATCH_COPY_ERROR=%s", errorMessage)
	return he.executeHooks(onBatchCopyRetry, v)
}

func (he *HooksExecutor) onRowCopyComplete() error {
	return he.executeHooks(onRowCopyComplete)
}

func (he *HooksExecutor) onBeginPostponed() error {
	return he.executeHooks(onBeginPostponed)
}

func (he *HooksExecutor) onBeforeCutOver() error {
	return he.executeHooks(onBeforeCutOver)
}

func (he *HooksExecutor) onInteractiveCommand(command string) error {
	v := fmt.Sprintf("GH_OST_COMMAND='%s'", command)
	return he.executeHooks(onInteractiveCommand, v)
}

func (he *HooksExecutor) onSuccess(instantDDL bool) error {
	v := fmt.Sprintf("GH_OST_INSTANT_DDL=%t", instantDDL)
	return he.executeHooks(onSuccess, v)
}

func (he *HooksExecutor) onFailure() error {
	return he.executeHooks(onFailure)
}

func (he *HooksExecutor) onStatus(statusMessage string) error {
	v := fmt.Sprintf("GH_OST_STATUS='%s'", statusMessage)
	return he.executeHooks(onStatus, v)
}

func (he *HooksExecutor) onStopReplication() error {
	return he.executeHooks(onStopReplication)
}

func (he *HooksExecutor) onStartReplication() error {
	return he.executeHooks(onStartReplication)
}
