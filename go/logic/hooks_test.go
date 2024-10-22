/*
   Copyright 2022 GitHub Inc.
         See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"bufio"
	"bytes"
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
			case "TEST":
				require.Equal(t, t.Name(), split[1])
			}
		}
	})
}
