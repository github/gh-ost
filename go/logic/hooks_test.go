/*
   Copyright 2022 GitHub Inc.
         See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/github/gh-ost/go/base"
)

type recordingHooks struct {
	name   string
	calls  *[]string
	errOn  string
	errVal error
}

func (r *recordingHooks) record(method string) error {
	*r.calls = append(*r.calls, r.name+":"+method)
	if r.errOn == method {
		return r.errVal
	}
	return nil
}

func (r *recordingHooks) OnStartup() error          { return r.record("OnStartup") }
func (r *recordingHooks) OnValidated() error        { return r.record("OnValidated") }
func (r *recordingHooks) OnRowCountComplete() error { return r.record("OnRowCountComplete") }
func (r *recordingHooks) OnBeforeRowCopy() error    { return r.record("OnBeforeRowCopy") }
func (r *recordingHooks) OnRowCopyComplete() error  { return r.record("OnRowCopyComplete") }
func (r *recordingHooks) OnBeginPostponed() error   { return r.record("OnBeginPostponed") }
func (r *recordingHooks) OnBeforeCutOver() error    { return r.record("OnBeforeCutOver") }
func (r *recordingHooks) OnInteractiveCommand(string) error {
	return r.record("OnInteractiveCommand")
}
func (r *recordingHooks) OnSuccess(bool) error          { return r.record("OnSuccess") }
func (r *recordingHooks) OnFailure() error              { return r.record("OnFailure") }
func (r *recordingHooks) OnBatchCopyRetry(string) error { return r.record("OnBatchCopyRetry") }
func (r *recordingHooks) OnStatus(string) error         { return r.record("OnStatus") }
func (r *recordingHooks) OnStopReplication() error      { return r.record("OnStopReplication") }
func (r *recordingHooks) OnStartReplication() error     { return r.record("OnStartReplication") }

func TestCompositeHooks_FanOut(t *testing.T) {
	var calls []string
	composite := CompositeHooks{
		&recordingHooks{name: "a", calls: &calls},
		&recordingHooks{name: "b", calls: &calls},
		&recordingHooks{name: "c", calls: &calls},
	}

	require.NoError(t, composite.OnStartup())
	require.NoError(t, composite.OnBeforeCutOver())
	require.NoError(t, composite.OnSuccess(true))

	require.Equal(t, []string{
		"a:OnStartup", "b:OnStartup", "c:OnStartup",
		"a:OnBeforeCutOver", "b:OnBeforeCutOver", "c:OnBeforeCutOver",
		"a:OnSuccess", "b:OnSuccess", "c:OnSuccess",
	}, calls)
}

func TestCompositeHooks_SkipsNil(t *testing.T) {
	var calls []string
	composite := CompositeHooks{
		nil,
		&recordingHooks{name: "a", calls: &calls},
		nil,
		&recordingHooks{name: "b", calls: &calls},
	}

	require.NoError(t, composite.OnStartup())
	require.NoError(t, composite.OnSuccess(false))
	require.Equal(t, []string{"a:OnStartup", "b:OnStartup", "a:OnSuccess", "b:OnSuccess"}, calls)
}

func TestCompositeHooks_FirstErrorWins(t *testing.T) {
	var calls []string
	boom := errors.New("boom")
	composite := CompositeHooks{
		&recordingHooks{name: "a", calls: &calls},
		&recordingHooks{name: "b", calls: &calls, errOn: "OnBeforeCutOver", errVal: boom},
		&recordingHooks{name: "c", calls: &calls}, // must not be called
	}

	err := composite.OnBeforeCutOver()
	require.ErrorIs(t, err, boom)
	require.Equal(t, []string{"a:OnBeforeCutOver", "b:OnBeforeCutOver"}, calls)
}

var (
	_ base.Hooks = (*HooksExecutor)(nil)
	_ base.Hooks = (CompositeHooks)(nil)
)

func TestCompositeHooks_WithShellExecutor(t *testing.T) {
	ctx := base.NewMigrationContext()
	ctx.DatabaseName = "test"
	ctx.OriginalTableName = "tablename"

	hooksDir, err := os.MkdirTemp("", "TestCompositeHooks_WithShellExecutor")
	require.NoError(t, err)
	defer os.RemoveAll(hooksDir)
	ctx.HooksPath = hooksDir

	sideEffect := filepath.Join(hooksDir, "ran.marker")
	script := fmt.Sprintf("#!/bin/sh\ntouch %q\n", sideEffect)
	require.NoError(t, os.WriteFile(filepath.Join(hooksDir, "gh-ost-on-startup"), []byte(script), 0o777))

	shellExec := NewHooksExecutor(ctx)
	shellExec.writer = new(bytes.Buffer) // suppress stderr noise

	var calls []string
	goFake := &recordingHooks{name: "go", calls: &calls}

	composite := CompositeHooks{shellExec, goFake}
	require.NoError(t, composite.OnStartup())

	_, err = os.Stat(sideEffect)
	require.NoError(t, err, "shell hook should have created marker file")
	require.Equal(t, []string{"go:OnStartup"}, calls, "Go fake should have been invoked once")
}

func TestNewMigrator_HooksFromContext(t *testing.T) {
	t.Run("default-is-script-executor", func(t *testing.T) {
		ctx := base.NewMigrationContext()
		m := NewMigrator(ctx, "test")
		_, ok := m.hooksExecutor.(*HooksExecutor)
		require.True(t, ok)
	})

	t.Run("context-hooks-take-precedence", func(t *testing.T) {
		var calls []string
		fake := &recordingHooks{name: "fake", calls: &calls}

		ctx := base.NewMigrationContext()
		ctx.Hooks = fake
		m := NewMigrator(ctx, "test")

		require.Same(t, fake, m.hooksExecutor)
		require.NoError(t, m.hooksExecutor.OnStartup())
		require.Equal(t, []string{"fake:OnStartup"}, calls)
	})
}

func TestHooksExecutorExecuteHooks(t *testing.T) {
	migrationContext := base.NewMigrationContext()
	migrationContext.AlterStatement = "ENGINE=InnoDB"
	migrationContext.DatabaseName = "test"
	migrationContext.Hostname = "test.example.com"
	migrationContext.OriginalTableName = "tablename"
	migrationContext.RowsDeltaEstimate = 1
	migrationContext.RowsEstimate = 122
	migrationContext.TotalRowsCopied = 123456
	migrationContext.SetETADuration(time.Minute)
	migrationContext.SetProgressPct(50)
	hooksExecutor := NewHooksExecutor(migrationContext)

	writeTmpHookFunc := func(testName, hookName, script string) (path string, err error) {
		if path, err = os.MkdirTemp("", testName); err != nil {
			return path, err
		}
		err = os.WriteFile(filepath.Join(path, hookName), []byte(script), 0777)
		return path, err
	}

	t.Run("does-not-exist", func(t *testing.T) {
		migrationContext.HooksPath = "/does/not/exist"
		require.Nil(t, hooksExecutor.executeHooks("test-hook"))
	})

	t.Run("failed", func(t *testing.T) {
		var err error
		if migrationContext.HooksPath, err = writeTmpHookFunc(
			"TestHooksExecutorExecuteHooks-failed",
			"failed-hook",
			"#!/bin/sh\nexit 1",
		); err != nil {
			panic(err)
		}
		defer os.RemoveAll(migrationContext.HooksPath)
		require.NotNil(t, hooksExecutor.executeHooks("failed-hook"))
	})

	t.Run("success", func(t *testing.T) {
		var err error
		if migrationContext.HooksPath, err = writeTmpHookFunc(
			"TestHooksExecutorExecuteHooks-success",
			"success-hook",
			"#!/bin/sh\nenv",
		); err != nil {
			panic(err)
		}
		defer os.RemoveAll(migrationContext.HooksPath)

		var buf bytes.Buffer
		hooksExecutor.writer = &buf
		require.Nil(t, hooksExecutor.executeHooks("success-hook", "TEST="+t.Name()))

		scanner := bufio.NewScanner(&buf)
		for scanner.Scan() {
			split := strings.SplitN(scanner.Text(), "=", 2)
			switch split[0] {
			case "GH_OST_COPIED_ROWS":
				copiedRows, _ := strconv.ParseInt(split[1], 10, 64)
				require.Equal(t, migrationContext.TotalRowsCopied, copiedRows)
			case "GH_OST_DATABASE_NAME":
				require.Equal(t, migrationContext.DatabaseName, split[1])
			case "GH_OST_DDL":
				require.Equal(t, migrationContext.AlterStatement, split[1])
			case "GH_OST_DRY_RUN":
				require.Equal(t, "false", split[1])
			case "GH_OST_ESTIMATED_ROWS":
				estimatedRows, _ := strconv.ParseInt(split[1], 10, 64)
				require.Equal(t, int64(123), estimatedRows)
			case "GH_OST_ETA_SECONDS":
				etaSeconds, _ := strconv.ParseInt(split[1], 10, 64)
				require.Equal(t, int64(60), etaSeconds)
			case "GH_OST_EXECUTING_HOST":
				require.Equal(t, migrationContext.Hostname, split[1])
			case "GH_OST_GHOST_TABLE_NAME":
				require.Equal(t, fmt.Sprintf("_%s_gho", migrationContext.OriginalTableName), split[1])
			case "GH_OST_OLD_TABLE_NAME":
				require.Equal(t, fmt.Sprintf("_%s_del", migrationContext.OriginalTableName), split[1])
			case "GH_OST_PROGRESS":
				progress, _ := strconv.ParseFloat(split[1], 64)
				require.Equal(t, 50.0, progress)
			case "GH_OST_TABLE_NAME":
				require.Equal(t, migrationContext.OriginalTableName, split[1])
			case "GH_OST_INSTANT_DDL":
				require.Equal(t, "false", split[1])
			case "TEST":
				require.Equal(t, t.Name(), split[1])
			}
		}
	})
}
