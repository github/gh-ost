package logic

import (
	"context"
	gosql "database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/mysql"
	driver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	testmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
)

type cleanupWarningLogger struct {
	base.Logger
	mu       sync.Mutex
	warnings []string
}

func (l *cleanupWarningLogger) Warningf(format string, args ...interface{}) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warnings = append(l.warnings, fmt.Sprintf(format, args...))
	return nil
}

func (l *cleanupWarningLogger) contains(text string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Contains(strings.Join(l.warnings, "\n"), text)
}

type cleanupFailureHooks struct {
	CompositeHooks
	onBeforeCutOver func() error
	onSuccess       func() error
}

func (h *cleanupFailureHooks) OnSuccess(bool) error {
	if h.onSuccess != nil {
		return h.onSuccess()
	}
	return nil
}

func (h *cleanupFailureHooks) OnBeforeCutOver() error {
	if h.onBeforeCutOver != nil {
		return h.onBeforeCutOver()
	}
	return nil
}

func createBlockingReference(t *testing.T, ctx context.Context, db *gosql.DB, database, table, parent string) {
	t.Helper()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()

	_, err = conn.ExecContext(ctx, "SET SESSION foreign_key_checks = 0")
	require.NoError(t, err)
	_, createErr := conn.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE `%s`.`ref_%s` (id INT, FOREIGN KEY (id) REFERENCES `%s`.`%s` (id))",
		database, table, database, parent))
	_, resetErr := conn.ExecContext(ctx, "SET SESSION foreign_key_checks = 1")
	require.NoError(t, resetErr)
	require.NoError(t, createErr)
}

func TestMoveTablesCleanupErrors(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	container, err := testmysql.Run(ctx, testMysqlContainerImage,
		testmysql.WithDatabase(testMysqlDatabase),
		testmysql.WithUsername(testMysqlUser),
		testmysql.WithPassword(testMysqlPass),
		testmysql.WithConfigFile("my.cnf.test"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, testcontainers.TerminateContainer(container)) })

	dsn, err := container.ConnectionString(ctx)
	require.NoError(t, err)
	db, err := gosql.Open("mysql", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.ExecContext(ctx, "CREATE DATABASE "+testMysqlDatabaseOther)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "CREATE TABLE test.cleanup_progress (id INT)")
	require.NoError(t, err)
	config, err := getTestConnectionConfig(ctx, container)
	require.NoError(t, err)

	for _, tc := range []struct {
		name   string
		noop   bool
		resume bool
	}{
		{name: "fresh_real"},
		{name: "fresh_noop", noop: true},
		{name: "resumed_post_rename", resume: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table := "cleanup_" + tc.name
			_, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE `%s`.`%s` (id INT NOT NULL PRIMARY KEY)", testMysqlDatabase, table))
			require.NoError(t, err)
			_, err = db.ExecContext(ctx, fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES (1)", testMysqlDatabase, table))
			require.NoError(t, err)

			mc := newTestMigrationContext()
			mc.Checkpoint, mc.Resume, mc.Noop, mc.UseGTIDs = true, tc.resume, tc.noop, true
			mc.OkToDropTable = true
			mc.InspectorConnectionConfig = config
			mc.AssumeMasterHostname = fmt.Sprintf("%s:%d", config.Key.Hostname, config.Key.Port)
			mc.MoveTables.TableNames = []string{table}
			mc.MoveTables.TargetDatabase = testMysqlDatabaseOther
			mc.MoveTables.ConnectionConfig = config
			mc.MoveTables.AllowOnSourcePrimary = true
			mc.CutOverLockTimeoutSeconds = 15
			mc.CheckpointIntervalSeconds = 300
			mc.SetDefaultNumRetries(1)
			mc.ServeSocketFile = filepath.Join(t.TempDir(), "sock")
			logger := &cleanupWarningLogger{Logger: mc.Log}
			mc.Log = logger
			require.NoError(t, mc.SetConnectionConfig("innodb"))

			if tc.resume {
				_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE `%s`.`%s` (id INT NOT NULL PRIMARY KEY)", testMysqlDatabaseOther, table))
				require.NoError(t, err)
				var start string
				require.NoError(t, db.QueryRowContext(ctx, "SELECT @@global.gtid_executed").Scan(&start))
				startCoords, err := mysql.NewGTIDBinlogCoordinates(mysql.MySQLFlavor, start)
				require.NoError(t, err)
				_, err = db.ExecContext(ctx, fmt.Sprintf("RENAME TABLE `%s`.`%s` TO `%s`.`_%s_del`", testMysqlDatabase, table, testMysqlDatabase, table))
				require.NoError(t, err)
				var drain string
				require.NoError(t, db.QueryRowContext(ctx, "SELECT @@global.gtid_executed").Scan(&drain))
				drainCoords, err := mysql.NewGTIDBinlogCoordinates(mysql.MySQLFlavor, drain)
				require.NoError(t, err)
				checkpoint := NewApplier(mc)
				checkpoint.moveTablesTargetDB = db
				require.NoError(t, checkpoint.CreateCheckpointTable())
				require.NoError(t, checkpoint.WriteMoveTableCheckpoints([]*Checkpoint{{
					TableName: table, LastTrxCoords: startCoords,
					MoveTablesCutOverStarted: true, MoveTablesCutOverDrainGTID: drainCoords,
				}}))
			}

			if tc.noop {
				createBlockingReference(t, ctx, db, testMysqlDatabaseOther, table, table)
			} else {
				createBlockingReference(t, ctx, db, testMysqlDatabase, table, "_"+table+"_del")
			}
			hooks := &cleanupFailureHooks{}
			var successCalled bool
			hooks.onSuccess = func() error {
				successCalled = true
				return nil
			}
			var progress chan error
			if !tc.noop && !tc.resume {
				hooks.onBeforeCutOver = func() error {
					progress = make(chan error, 1)
					// A later transaction advances the streamer past the rename GTID.
					go func() {
						progressCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
						defer cancel()
						ticker := time.NewTicker(50 * time.Millisecond)
						defer ticker.Stop()
						for {
							var count int
							err := db.QueryRowContext(progressCtx, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?", testMysqlDatabase, "_"+table+"_del").Scan(&count)
							if err != nil {
								progress <- err
								return
							}
							if count > 0 {
								_, err = db.ExecContext(progressCtx, "INSERT INTO test.cleanup_progress VALUES (1)")
								progress <- err
								return
							}
							select {
							case <-ticker.C:
							case <-progressCtx.Done():
								progress <- fmt.Errorf("source rename was not observed: %w", progressCtx.Err())
								return
							}
						}
					}()
					return nil
				}
			}
			mc.Hooks = hooks

			//nolint:contextcheck // Lock teardown uses an independent timeout after migration cancellation.
			err = NewMigrator(mc, "test").MoveTables()
			if progress != nil {
				select {
				case progressErr := <-progress:
					require.NoError(t, progressErr, "MoveTables error: %v", err)
				case <-time.After(12 * time.Second):
					t.Fatalf("timed out waiting for progress; MoveTables error: %v", err)
				}
			}
			var mysqlErr *driver.MySQLError
			require.ErrorAs(t, err, &mysqlErr)
			require.Equal(t, uint16(3730), mysqlErr.Number)
			if tc.noop {
				require.ErrorContains(t, err, "move-tables cleanup failed")
				require.False(t, successCalled)
				require.Zero(t, atomic.LoadInt64(&mc.CutOverCompleteFlag))
				require.False(t, logger.contains("cutover may have enabled target writes"))
			} else {
				require.ErrorContains(t, err, "move-tables cutover completed but cleanup failed")
				require.True(t, successCalled)
				require.Equal(t, int64(1), atomic.LoadInt64(&mc.CutOverCompleteFlag))
				require.True(t, logger.contains("cutover may have enabled target writes"))
			}
		})
	}

	t.Run("cli_fresh_noop", func(t *testing.T) {
		dir := t.TempDir()
		binary := filepath.Join(dir, "gh-ost")
		build := exec.Command("go", "build", "-o", binary, "../cmd/gh-ost")
		output, err := build.CombinedOutput()
		require.NoError(t, err, "%s", output)

		_, err = db.ExecContext(ctx, "CREATE TABLE test.cleanup_cli (id INT NOT NULL PRIMARY KEY)")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "INSERT INTO test.cleanup_cli VALUES (1)")
		require.NoError(t, err)
		createBlockingReference(t, ctx, db, testMysqlDatabaseOther, "cleanup_cli", "cleanup_cli")
		failure := filepath.Join(dir, "failure")
		failureHook := fmt.Sprintf("#!/bin/sh\n: > %q\n", failure)
		require.NoError(t, os.WriteFile(filepath.Join(dir, "gh-ost-on-failure"), []byte(failureHook), 0o755))
		socket := filepath.Join(dir, "sock")

		runCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()
		args := []string{
			"--host=" + config.Key.Hostname, fmt.Sprintf("--port=%d", config.Key.Port),
			"--user=" + testMysqlUser, "--password=" + testMysqlPass,
			"--database=" + testMysqlDatabase, "--move-tables=cleanup_cli",
			"--target-host=" + config.Key.Hostname, fmt.Sprintf("--target-port=%d", config.Key.Port),
			"--target-database=" + testMysqlDatabaseOther,
			"--assume-master-host=" + fmt.Sprintf("%s:%d", config.Key.Hostname, config.Key.Port),
			"--allow-on-master", "--allow-on-source-primary", "--skip-port-validation",
			"--default-retries=1", "--serve-socket-file=" + socket,
			"--postpone-cut-over-flag-file=" + filepath.Join(dir, "postpone"), "--hooks-path=" + dir}
		cmd := exec.CommandContext(runCtx, binary, args...)
		output, err = cmd.CombinedOutput()
		require.Error(t, err, "%s", output)
		require.Contains(t, string(output), "move-tables cleanup failed")
		require.NotContains(t, string(output), "cutover may have enabled target writes")
		require.FileExists(t, failure)

		startup := filepath.Join(dir, "startup")
		startupHook := fmt.Sprintf("#!/bin/sh\n: > %q\n", startup)
		require.NoError(t, os.WriteFile(filepath.Join(dir, "gh-ost-on-startup"), []byte(startupHook), 0o755))
		require.NoError(t, os.Remove(failure))
		for i, arg := range args {
			if arg == "--move-tables=cleanup_cli" {
				args[i] = "--move-tables=cleanup_preflight"
			}
		}
		args = append(args, "--resume")
		preflight := exec.CommandContext(runCtx, binary, args...)
		output, err = preflight.CombinedOutput()
		require.Error(t, err, "%s", output)
		require.Contains(t, string(output), "failed to inspect move-tables cutover checkpoint")
		require.NoFileExists(t, startup)
		require.FileExists(t, failure)
	})
}
