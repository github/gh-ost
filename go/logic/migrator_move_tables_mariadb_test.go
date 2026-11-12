package logic

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/github/gh-ost/go/mysql"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	testmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/wait"
)

func newMoveTablesTestDB(t *testing.T, image string, options ...testcontainers.ContainerCustomizer) (*mysql.ConnectionConfig, *gosql.DB) {
	t.Helper()
	ctx := context.Background()
	options = append([]testcontainers.ContainerCustomizer{
		testmysql.WithDatabase(testMysqlDatabase),
		testmysql.WithUsername(testMysqlUser),
		testmysql.WithPassword(testMysqlPass),
	}, options...)
	container, err := testmysql.Run(ctx, image, options...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, testcontainers.TerminateContainer(container)) })
	dsn, err := container.ConnectionString(ctx)
	require.NoError(t, err)
	db, err := gosql.Open("mysql", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	config, err := getTestConnectionConfig(ctx, container)
	require.NoError(t, err)
	return config, db
}

func TestMoveTablesMariaDBBaseTables(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	config, db := newMoveTablesTestDB(t, "mariadb:10.11.18",
		testcontainers.WithCmd("--log-bin=mysql-bin", "--binlog-format=ROW", "--server-id=1"),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("3306/tcp")))

	_, err := db.ExecContext(ctx, "CREATE TABLE regular_table (id INT PRIMARY KEY)")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "CREATE TABLE versioned_table (id INT PRIMARY KEY) WITH SYSTEM VERSIONING")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "CREATE VIEW table_view AS SELECT id FROM regular_table")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "CREATE DATABASE "+testMysqlDatabaseOther)
	require.NoError(t, err)

	mc := newTestMigrationContext()
	m := NewMigrator(mc, "test")
	for _, tc := range []struct {
		table string
		want  string
	}{
		{table: "regular_table"},
		{table: "versioned_table", want: `type "SYSTEM VERSIONED"; only BASE TABLE is supported`},
		{table: "table_view", want: `type "VIEW"; only BASE TABLE is supported`},
		{table: "regularXtable", want: "does not exist"},
	} {
		t.Run(tc.table, func(t *testing.T) {
			mc.MoveTables.TableNames = []string{tc.table}
			err := m.validateMoveTablesBaseTables(db, "source read host")
			if tc.want == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.want)
			}
		})
	}
	mc.MoveTables.TableNames = []string{"versioned_table"}
	m.sourcePrimaryDB = db
	require.ErrorContains(t, m.validateMoveTablesResumeSourceTables(false), "recover manually")
	_, err = db.ExecContext(ctx, "RENAME TABLE versioned_table TO _versioned_table_del")
	require.NoError(t, err)
	require.ErrorContains(t, m.validateMoveTablesResumeSourceTables(true), "recover manually")
	_, err = db.ExecContext(ctx, "RENAME TABLE _versioned_table_del TO versioned_table")
	require.NoError(t, err)

	mc = newTestMigrationContext()
	mc.Checkpoint, mc.UseGTIDs = true, true
	mc.InspectorConnectionConfig = config
	mc.AssumeMasterHostname = fmt.Sprintf("%s:%d", config.Key.Hostname, config.Key.Port)
	mc.MoveTables.TableNames = []string{"regular_table", "versioned_table"}
	mc.MoveTables.TargetDatabase = testMysqlDatabaseOther
	mc.MoveTables.ConnectionConfig = config
	mc.MoveTables.AllowOnSourcePrimary = true
	mc.Hooks = CompositeHooks{}
	require.NoError(t, mc.SetConnectionConfig("innodb"))

	err = NewMigrator(mc, "test").MoveTables()
	require.ErrorContains(t, err, `type "SYSTEM VERSIONED"; only BASE TABLE is supported`)
	var count int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ?", testMysqlDatabaseOther).Scan(&count)
	require.NoError(t, err)
	require.Zero(t, count, "validation must run before creating a checkpoint or target tables")

	readConfig, readDB := newMoveTablesTestDB(t, "mariadb:10.11.18",
		testcontainers.WithCmd("--log-bin=mysql-bin", "--binlog-format=ROW", "--server-id=2", "--log-slave-updates=ON"),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("3306/tcp")))
	_, err = readDB.ExecContext(ctx, "CREATE TABLE regular_table (id INT PRIMARY KEY) WITH SYSTEM VERSIONING")
	require.NoError(t, err)

	mc = newTestMigrationContext()
	mc.Checkpoint, mc.UseGTIDs = true, true
	mc.InspectorConnectionConfig = readConfig
	mc.AssumeMasterHostname = fmt.Sprintf("%s:%d", config.Key.Hostname, config.Key.Port)
	mc.MoveTables.TableNames = []string{"regular_table"}
	mc.MoveTables.TargetDatabase = testMysqlDatabaseOther
	mc.MoveTables.ConnectionConfig = config
	mc.Hooks = CompositeHooks{}
	require.NoError(t, mc.SetConnectionConfig("innodb"))

	err = NewMigrator(mc, "test").MoveTables()
	require.ErrorContains(t, err, `on source read host has type "SYSTEM VERSIONED"`)
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ?", testMysqlDatabaseOther).Scan(&count)
	require.NoError(t, err)
	require.Zero(t, count, "read-host validation must run before creating a checkpoint or target tables")
}

func TestMoveTablesRejectsMixedFlavors(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	mysqlConfig, mysqlDB := newMoveTablesTestDB(t, testMysqlContainerImage,
		testmysql.WithConfigFile("my.cnf.test"))
	mariaConfig, mariaDB := newMoveTablesTestDB(t, "mariadb:10.11.18",
		testcontainers.WithCmd("--log-bin=mysql-bin", "--binlog-format=ROW", "--server-id=1", "--log-slave-updates=ON"),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("3306/tcp")))

	configs := map[string]*mysql.ConnectionConfig{"mysql": mysqlConfig, "mariadb": mariaConfig}
	dbs := map[string]*gosql.DB{"mysql": mysqlDB, "mariadb": mariaDB}
	for _, db := range dbs {
		_, err := db.ExecContext(ctx, "CREATE DATABASE "+testMysqlDatabaseOther)
		require.NoError(t, err)
	}

	for _, tc := range []struct {
		name, read, primary, target, want string
	}{
		{"mysql_read_mariadb_primary", "mysql", "mariadb", "mysql", "source primary flavor mariadb does not match source read host flavor mysql"},
		{"mariadb_read_mysql_primary", "mariadb", "mysql", "mariadb", "source primary flavor mysql does not match source read host flavor mariadb"},
		{"mysql_source_mariadb_target", "mysql", "mysql", "mariadb", "target flavor mariadb does not match source read host flavor mysql"},
		{"mariadb_source_mysql_target", "mariadb", "mariadb", "mysql", "target flavor mysql does not match source read host flavor mariadb"},
		{"mysql_matching", "mysql", "mysql", "mysql", ""},
		{"mariadb_matching", "mariadb", "mariadb", "mariadb", ""},
	} {
		for _, resume := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/resume=%t", tc.name, resume), func(t *testing.T) {
				mc := newTestMigrationContext()
				mc.Checkpoint, mc.UseGTIDs, mc.Resume = true, true, resume
				mc.InspectorConnectionConfig = configs[tc.read]
				primary := configs[tc.primary]
				mc.AssumeMasterHostname = fmt.Sprintf("%s:%d", primary.Key.Hostname, primary.Key.Port)
				mc.MoveTables.TableNames = []string{"flavor_test"}
				mc.MoveTables.TargetDatabase = testMysqlDatabaseOther
				mc.MoveTables.ConnectionConfig = configs[tc.target]
				mc.MoveTables.AllowOnSourcePrimary = true
				mc.Hooks = CompositeHooks{}
				require.NoError(t, mc.SetConnectionConfig("innodb"))

				//nolint:contextcheck // Lock teardown uses an independent timeout after migration cancellation.
				err := NewMigrator(mc, "test").MoveTables()
				if tc.want != "" {
					require.ErrorContains(t, err, tc.want)
				} else if resume {
					require.ErrorContains(t, err, "failed to inspect move-tables cutover checkpoint")
				} else {
					require.ErrorContains(t, err, "does not exist")
				}
				var count int
				err = dbs[tc.target].QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ?", testMysqlDatabaseOther).Scan(&count)
				require.NoError(t, err)
				require.Zero(t, count, "validation must run before creating a checkpoint or target tables")
			})
		}
	}
}
