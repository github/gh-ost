/*
   Copyright 2025 GitHub Inc.
         See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"context"
	gosql "database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/testcontainers/testcontainers-go"
	testmysql "github.com/testcontainers/testcontainers-go/modules/mysql"

	"fmt"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/binlog"
	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"
)

func TestApplierGenerateSqlModeQuery(t *testing.T) {
	migrationContext := base.NewMigrationContext()
	applier := NewApplier(migrationContext)

	{
		require.Equal(t,
			`sql_mode = CONCAT(@@session.sql_mode, ',NO_AUTO_VALUE_ON_ZERO,STRICT_ALL_TABLES')`,
			applier.generateSqlModeQuery(),
		)
	}
	{
		migrationContext.SkipStrictMode = true
		migrationContext.AllowZeroInDate = false
		require.Equal(t,
			`sql_mode = CONCAT(@@session.sql_mode, ',NO_AUTO_VALUE_ON_ZERO')`,
			applier.generateSqlModeQuery(),
		)
	}
	{
		migrationContext.SkipStrictMode = false
		migrationContext.AllowZeroInDate = true
		require.Equal(t,
			`sql_mode = REPLACE(REPLACE(CONCAT(@@session.sql_mode, ',NO_AUTO_VALUE_ON_ZERO,STRICT_ALL_TABLES'), 'NO_ZERO_IN_DATE', ''), 'NO_ZERO_DATE', '')`,
			applier.generateSqlModeQuery(),
		)
	}
	{
		migrationContext.SkipStrictMode = true
		migrationContext.AllowZeroInDate = true
		require.Equal(t,
			`sql_mode = REPLACE(REPLACE(CONCAT(@@session.sql_mode, ',NO_AUTO_VALUE_ON_ZERO'), 'NO_ZERO_IN_DATE', ''), 'NO_ZERO_DATE', '')`,
			applier.generateSqlModeQuery(),
		)
	}
}

func TestApplierUpdateModifiesUniqueKeyColumns(t *testing.T) {
	columns := sql.NewColumnList([]string{"id", "item_id", "item_text"})
	columnValues := sql.ToColumnValues([]interface{}{123456, 42, []uint8{116, 101, 115, 116}})

	migrationContext := base.NewMigrationContext()
	migrationContext.OriginalTableColumns = columns
	migrationContext.UniqueKey = &sql.UniqueKey{
		Name:    t.Name(),
		Columns: *columns,
	}

	applier := NewApplier(migrationContext)

	t.Run("unmodified", func(t *testing.T) {
		modifiedColumn, isModified := applier.updateModifiesUniqueKeyColumns(&binlog.BinlogDMLEvent{
			DatabaseName:      "test",
			DML:               binlog.UpdateDML,
			NewColumnValues:   columnValues,
			WhereColumnValues: columnValues,
		})
		require.Equal(t, "", modifiedColumn)
		require.False(t, isModified)
	})

	t.Run("modified", func(t *testing.T) {
		modifiedColumn, isModified := applier.updateModifiesUniqueKeyColumns(&binlog.BinlogDMLEvent{
			DatabaseName:      "test",
			DML:               binlog.UpdateDML,
			NewColumnValues:   sql.ToColumnValues([]interface{}{123456, 24}),
			WhereColumnValues: columnValues,
		})
		require.Equal(t, "item_id", modifiedColumn)
		require.True(t, isModified)
	})
}

func TestApplierBuildDMLEventQuery(t *testing.T) {
	columns := sql.NewColumnList([]string{"id", "item_id"})
	columnValues := sql.ToColumnValues([]interface{}{123456, 42})

	migrationContext := base.NewMigrationContext()
	migrationContext.DatabaseName = "test"
	migrationContext.OriginalTableName = "test"
	migrationContext.OriginalTableColumns = columns
	migrationContext.SharedColumns = columns
	migrationContext.MappedSharedColumns = columns
	migrationContext.UniqueKey = &sql.UniqueKey{
		Name:    t.Name(),
		Columns: *columns,
	}

	applier := NewApplier(migrationContext)
	applier.prepareQueries()

	t.Run("delete", func(t *testing.T) {
		binlogEvent := &binlog.BinlogDMLEvent{
			DatabaseName:      "test",
			DML:               binlog.DeleteDML,
			WhereColumnValues: columnValues,
		}

		res := applier.buildDMLEventQuery(binlogEvent)
		require.Len(t, res, 1)
		require.NoError(t, res[0].err)
		require.Equal(t, `delete /* gh-ost `+"`test`.`_test_gho`"+` */
		from
			`+"`test`.`_test_gho`"+`
		where
			((`+"`id`"+` = ?) and (`+"`item_id`"+` = ?))`,
			strings.TrimSpace(res[0].query))
		require.Len(t, res[0].args, 2)
		require.Equal(t, 123456, res[0].args[0])
		require.Equal(t, 42, res[0].args[1])
	})

	t.Run("insert", func(t *testing.T) {
		binlogEvent := &binlog.BinlogDMLEvent{
			DatabaseName:    "test",
			DML:             binlog.InsertDML,
			NewColumnValues: columnValues,
		}
		res := applier.buildDMLEventQuery(binlogEvent)
		require.Len(t, res, 1)
		require.NoError(t, res[0].err)
		require.Equal(t,
			`insert /* gh-ost `+"`test`.`_test_gho`"+` */ ignore
		into
			`+"`test`.`_test_gho`"+`
			`+"(`id`, `item_id`)"+`
		values
			(?, ?)`,
			strings.TrimSpace(res[0].query))
		require.Len(t, res[0].args, 2)
		require.Equal(t, 123456, res[0].args[0])
		require.Equal(t, 42, res[0].args[1])
	})

	t.Run("update", func(t *testing.T) {
		binlogEvent := &binlog.BinlogDMLEvent{
			DatabaseName:      "test",
			DML:               binlog.UpdateDML,
			NewColumnValues:   columnValues,
			WhereColumnValues: columnValues,
		}
		res := applier.buildDMLEventQuery(binlogEvent)
		require.Len(t, res, 1)
		require.NoError(t, res[0].err)
		require.Equal(t,
			`update /* gh-ost `+"`test`.`_test_gho`"+` */
			`+"`test`.`_test_gho`"+`
		set
			`+"`id`"+`=?, `+"`item_id`"+`=?
		where
			((`+"`id`"+` = ?) and (`+"`item_id`"+` = ?))`,
			strings.TrimSpace(res[0].query))
		require.Len(t, res[0].args, 4)
		require.Equal(t, 123456, res[0].args[0])
		require.Equal(t, 42, res[0].args[1])
		require.Equal(t, 123456, res[0].args[2])
		require.Equal(t, 42, res[0].args[3])
	})
}

func TestApplierInstantDDL(t *testing.T) {
	migrationContext := base.NewMigrationContext()
	migrationContext.DatabaseName = "test"
	migrationContext.SkipPortValidation = true
	migrationContext.OriginalTableName = "mytable"
	migrationContext.AlterStatementOptions = "ADD INDEX (foo)"
	applier := NewApplier(migrationContext)

	t.Run("instantDDLstmt", func(t *testing.T) {
		stmt := applier.generateInstantDDLQuery()
		require.Equal(t, "ALTER /* gh-ost */ TABLE `test`.`mytable` ADD INDEX (foo), ALGORITHM=INSTANT", stmt)
	})
}

func TestRetryOnLockWaitTimeout(t *testing.T) {
	oldRetrySleepFn := RetrySleepFn
	defer func() { RetrySleepFn = oldRetrySleepFn }()
	RetrySleepFn = func(d time.Duration) {} // no-op for tests

	logger := base.NewMigrationContext().Log

	lockWaitTimeoutErr := &drivermysql.MySQLError{Number: 1205, Message: "Lock wait timeout exceeded"}
	nonRetryableErr := &drivermysql.MySQLError{Number: 1845, Message: "ALGORITHM=INSTANT is not supported"}

	t.Run("success on first attempt", func(t *testing.T) {
		calls := 0
		err := retryOnLockWaitTimeout(func() error {
			calls++
			return nil
		}, int64(5), logger)
		require.NoError(t, err)
		require.Equal(t, 1, calls)
	})

	t.Run("retry on lock wait timeout then succeed", func(t *testing.T) {
		calls := 0
		err := retryOnLockWaitTimeout(func() error {
			calls++
			if calls < 3 {
				return lockWaitTimeoutErr
			}
			return nil
		}, int64(5), logger)
		require.NoError(t, err)
		require.Equal(t, 3, calls)
	})

	t.Run("non-retryable error returns immediately", func(t *testing.T) {
		calls := 0
		err := retryOnLockWaitTimeout(func() error {
			calls++
			return nonRetryableErr
		}, int64(5), logger)
		require.ErrorIs(t, err, nonRetryableErr)
		require.Equal(t, 1, calls)
	})

	t.Run("non-mysql error returns immediately", func(t *testing.T) {
		calls := 0
		genericErr := errors.New("connection refused")
		err := retryOnLockWaitTimeout(func() error {
			calls++
			return genericErr
		}, int64(5), logger)
		require.ErrorIs(t, err, genericErr)
		require.Equal(t, 1, calls)
	})

	t.Run("exhausts all retries", func(t *testing.T) {
		calls := 0
		err := retryOnLockWaitTimeout(func() error {
			calls++
			return lockWaitTimeoutErr
		}, int64(5), logger)
		require.ErrorIs(t, err, lockWaitTimeoutErr)
		require.Equal(t, 5, calls)
	})
}

type ApplierTestSuite struct {
	suite.Suite

	mysqlContainer testcontainers.Container
	db             *gosql.DB
}

func (suite *ApplierTestSuite) SetupSuite() {
	ctx := context.Background()
	mysqlContainer, err := testmysql.Run(ctx,
		testMysqlContainerImage,
		testmysql.WithDatabase(testMysqlDatabase),
		testmysql.WithUsername(testMysqlUser),
		testmysql.WithPassword(testMysqlPass),
		testmysql.WithConfigFile("my.cnf.test"),
	)
	suite.Require().NoError(err)

	suite.mysqlContainer = mysqlContainer

	dsn, err := mysqlContainer.ConnectionString(ctx)
	suite.Require().NoError(err)

	db, err := gosql.Open("mysql", dsn)
	suite.Require().NoError(err)

	suite.db = db
}

func (suite *ApplierTestSuite) TeardownSuite() {
	suite.Assert().NoError(suite.db.Close())
	suite.Assert().NoError(testcontainers.TerminateContainer(suite.mysqlContainer))
}

func (suite *ApplierTestSuite) SetupTest() {
	ctx := context.Background()
	_, err := suite.db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", testMysqlDatabase))
	suite.Require().NoError(err)
}

func (suite *ApplierTestSuite) TearDownTest() {
	ctx := context.Background()

	_, err := suite.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+getTestTableName())
	suite.Require().NoError(err)
	_, err = suite.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+getTestGhostTableName())
	suite.Require().NoError(err)
}

func (suite *ApplierTestSuite) TestInitDBConnections() {
	ctx := context.Background()

	var err error

	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, item_id INT);", getTestTableName()))
	suite.Require().NoError(err)

	connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := newTestMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.SetConnectionConfig("innodb")

	applier := NewApplier(migrationContext)
	defer applier.Teardown()

	err = applier.InitDBConnections()
	suite.Require().NoError(err)

	mysqlVersion, _ := strings.CutPrefix(testMysqlContainerImage, "mysql:")
	suite.Require().Equal(mysqlVersion, migrationContext.ApplierMySQLVersion)
	suite.Require().Equal(int64(28800), migrationContext.ApplierWaitTimeout)
	suite.Require().Equal("+00:00", migrationContext.ApplierTimeZone)

	suite.Require().Equal(sql.NewColumnList([]string{"id", "item_id"}), migrationContext.OriginalTableColumnsOnApplier)
}

func (suite *ApplierTestSuite) TestApplyDMLEventQueries() {
	ctx := context.Background()

	var err error

	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, item_id INT);", getTestTableName()))
	suite.Require().NoError(err)

	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, item_id INT);", getTestGhostTableName()))
	suite.Require().NoError(err)

	connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := newTestMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.SetConnectionConfig("innodb")

	migrationContext.OriginalTableColumns = sql.NewColumnList([]string{"id", "item_id"})
	migrationContext.SharedColumns = sql.NewColumnList([]string{"id", "item_id"})
	migrationContext.MappedSharedColumns = sql.NewColumnList([]string{"id", "item_id"})
	migrationContext.UniqueKey = &sql.UniqueKey{
		Name:    "primary_key",
		Columns: *sql.NewColumnList([]string{"id"}),
	}

	applier := NewApplier(migrationContext)
	suite.Require().NoError(applier.prepareQueries())
	defer applier.Teardown()

	err = applier.InitDBConnections()
	suite.Require().NoError(err)

	dmlEvents := []*binlog.BinlogDMLEvent{
		{
			DatabaseName:    testMysqlDatabase,
			TableName:       testMysqlTableName,
			DML:             binlog.InsertDML,
			NewColumnValues: sql.ToColumnValues([]interface{}{123456, 42}),
		},
	}
	err = applier.ApplyDMLEventQueries(dmlEvents)
	suite.Require().NoError(err)

	// Check that the row was inserted
	rows, err := suite.db.Query("SELECT * FROM " + getTestGhostTableName())
	suite.Require().NoError(err)
	defer rows.Close()

	var count, id, item_id int
	for rows.Next() {
		err = rows.Scan(&id, &item_id)
		suite.Require().NoError(err)
		count += 1
	}
	suite.Require().NoError(rows.Err())

	suite.Require().Equal(1, count)
	suite.Require().Equal(123456, id)
	suite.Require().Equal(42, item_id)

	suite.Require().Equal(int64(1), migrationContext.TotalDMLEventsApplied)
	suite.Require().Equal(int64(0), migrationContext.RowsDeltaEstimate)
}

func (suite *ApplierTestSuite) TestValidateOrDropExistingTables() {
	ctx := context.Background()

	var err error

	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, item_id INT);", getTestTableName()))
	suite.Require().NoError(err)

	connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := newTestMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.SetConnectionConfig("innodb")

	migrationContext.OriginalTableColumns = sql.NewColumnList([]string{"id", "item_id"})
	migrationContext.SharedColumns = sql.NewColumnList([]string{"id", "item_id"})
	migrationContext.MappedSharedColumns = sql.NewColumnList([]string{"id", "item_id"})

	applier := NewApplier(migrationContext)
	defer applier.Teardown()

	err = applier.InitDBConnections()
	suite.Require().NoError(err)

	err = applier.ValidateOrDropExistingTables()
	suite.Require().NoError(err)
}

func (suite *ApplierTestSuite) TestValidateOrDropExistingTablesWithGhostTableExisting() {
	ctx := context.Background()

	var err error

	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, item_id INT);", getTestTableName()))
	suite.Require().NoError(err)

	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, item_id INT);", getTestGhostTableName()))
	suite.Require().NoError(err)

	connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := newTestMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.SetConnectionConfig("innodb")

	migrationContext.OriginalTableColumns = sql.NewColumnList([]string{"id", "item_id"})
	migrationContext.SharedColumns = sql.NewColumnList([]string{"id", "item_id"})
	migrationContext.MappedSharedColumns = sql.NewColumnList([]string{"id", "item_id"})

	applier := NewApplier(migrationContext)
	defer applier.Teardown()

	err = applier.InitDBConnections()
	suite.Require().NoError(err)

	err = applier.ValidateOrDropExistingTables()
	suite.Require().Error(err)
	suite.Require().EqualError(err, "Table `_testing_gho` already exists. Panicking. Use --initially-drop-ghost-table to force dropping it, though I really prefer that you drop it or rename it away")
}

func (suite *ApplierTestSuite) TestValidateOrDropExistingTablesWithGhostTableExistingAndInitiallyDropGhostTableSet() {
	ctx := context.Background()

	var err error

	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, item_id INT);", getTestTableName()))
	suite.Require().NoError(err)

	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, item_id INT);", getTestGhostTableName()))
	suite.Require().NoError(err)

	connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := newTestMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.SetConnectionConfig("innodb")

	migrationContext.InitiallyDropGhostTable = true

	applier := NewApplier(migrationContext)
	defer applier.Teardown()

	err = applier.InitDBConnections()
	suite.Require().NoError(err)

	err = applier.ValidateOrDropExistingTables()
	suite.Require().NoError(err)

	// Check that the ghost table was dropped
	var tableName string
	//nolint:execinquery
	err = suite.db.QueryRow(fmt.Sprintf("SHOW TABLES IN test LIKE '_%s_gho'", testMysqlTableName)).Scan(&tableName)
	suite.Require().Error(err)
	suite.Require().Equal(gosql.ErrNoRows, err)
}

func (suite *ApplierTestSuite) TestCreateGhostTable() {
	ctx := context.Background()

	var err error

	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, item_id INT);", getTestTableName()))
	suite.Require().NoError(err)

	connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := newTestMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.SetConnectionConfig("innodb")

	migrationContext.OriginalTableColumns = sql.NewColumnList([]string{"id", "item_id"})
	migrationContext.SharedColumns = sql.NewColumnList([]string{"id", "item_id"})
	migrationContext.MappedSharedColumns = sql.NewColumnList([]string{"id", "item_id"})

	migrationContext.InitiallyDropGhostTable = true

	applier := NewApplier(migrationContext)
	defer applier.Teardown()

	err = applier.InitDBConnections()
	suite.Require().NoError(err)

	err = applier.CreateGhostTable()
	suite.Require().NoError(err)

	// Check that the ghost table was created
	var tableName string
	//nolint:execinquery
	err = suite.db.QueryRow("SHOW TABLES IN test LIKE '_testing_gho'").Scan(&tableName)
	suite.Require().NoError(err)
	suite.Require().Equal("_testing_gho", tableName)

	// Check that the ghost table has the same columns as the original table
	var createDDL string
	//nolint:execinquery
	err = suite.db.QueryRow(fmt.Sprintf("SHOW CREATE TABLE %s", getTestGhostTableName())).Scan(&tableName, &createDDL)
	suite.Require().NoError(err)
	suite.Require().Equal("CREATE TABLE `_testing_gho` (\n  `id` int DEFAULT NULL,\n  `item_id` int DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", createDDL)
}

func (suite *ApplierTestSuite) TestPanicOnWarningsInApplyIterationInsertQuerySucceedsWithUniqueKeyWarningInsertedByDMLEvent() {
	ctx := context.Background()

	var err error

	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, item_id INT, UNIQUE KEY (item_id));", getTestTableName()))
	suite.Require().NoError(err)

	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, item_id INT, UNIQUE KEY (item_id));", getTestGhostTableName()))
	suite.Require().NoError(err)

	connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := newTestMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.SetConnectionConfig("innodb")

	migrationContext.PanicOnWarnings = true

	migrationContext.OriginalTableColumns = sql.NewColumnList([]string{"id", "item_id"})
	migrationContext.SharedColumns = sql.NewColumnList([]string{"id", "item_id"})
	migrationContext.MappedSharedColumns = sql.NewColumnList([]string{"id", "item_id"})
	migrationContext.UniqueKey = &sql.UniqueKey{
		Name:             "item_id",
		NameInGhostTable: "item_id",
		Columns:          *sql.NewColumnList([]string{"item_id"}),
	}

	applier := NewApplier(migrationContext)
	suite.Require().NoError(applier.prepareQueries())
	defer applier.Teardown()

	err = applier.InitDBConnections()
	suite.Require().NoError(err)

	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, item_id) VALUES (123456, 42);", getTestTableName()))
	suite.Require().NoError(err)

	dmlEvents := []*binlog.BinlogDMLEvent{
		{
			DatabaseName:    testMysqlDatabase,
			TableName:       testMysqlTableName,
			DML:             binlog.InsertDML,
			NewColumnValues: sql.ToColumnValues([]interface{}{123456, 42}),
		},
	}
	err = applier.ApplyDMLEventQueries(dmlEvents)
	suite.Require().NoError(err)

	err = applier.CreateChangelogTable()
	suite.Require().NoError(err)
	err = applier.ReadMigrationRangeValues()
	suite.Require().NoError(err)

	migrationContext.SetNextIterationRangeMinValues()
	hasFurtherRange, err := applier.CalculateNextIterationRangeEndValues()

	suite.Require().NoError(err)
	suite.Require().True(hasFurtherRange)

	_, rowsAffected, _, err := applier.ApplyIterationInsertQuery()
	suite.Require().NoError(err)
	suite.Require().Equal(int64(0), rowsAffected)

	// Ensure Duplicate entry '42' for key '_testing_gho.item_id' is ignored correctly
	suite.Require().Empty(applier.migrationContext.MigrationLastInsertSQLWarnings)

	// Check that the row was inserted
	rows, err := suite.db.Query("SELECT * FROM " + getTestGhostTableName())
	suite.Require().NoError(err)
	defer rows.Close()

	var count, id, item_id int
	for rows.Next() {
		err = rows.Scan(&id, &item_id)
		suite.Require().NoError(err)
		count += 1
	}
	suite.Require().NoError(rows.Err())

	suite.Require().Equal(1, count)
	suite.Require().Equal(123456, id)
	suite.Require().Equal(42, item_id)

	suite.Require().
		Equal(int64(1), migrationContext.TotalDMLEventsApplied)
	suite.Require().
		Equal(int64(0), migrationContext.RowsDeltaEstimate)
}

func (suite *ApplierTestSuite) TestPanicOnWarningsInApplyIterationInsertQueryFailsWithTruncationWarning() {
	ctx := context.Background()

	var err error

	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id int not null, name varchar(20), primary key(id))", getTestTableName()))
	suite.Require().NoError(err)

	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, name varchar(20), primary key(id));", getTestGhostTableName()))
	suite.Require().NoError(err)

	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'this string is long')", getTestTableName()))
	suite.Require().NoError(err)

	connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := newTestMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.SetConnectionConfig("innodb")

	migrationContext.AlterStatementOptions = "modify column name varchar(10)"
	migrationContext.OriginalTableColumns = sql.NewColumnList([]string{"id", "name"})
	migrationContext.SharedColumns = sql.NewColumnList([]string{"id", "name"})
	migrationContext.MappedSharedColumns = sql.NewColumnList([]string{"id", "name"})
	migrationContext.UniqueKey = &sql.UniqueKey{
		Name:             "PRIMARY",
		NameInGhostTable: "PRIMARY",
		Columns:          *sql.NewColumnList([]string{"id"}),
	}
	applier := NewApplier(migrationContext)

	err = applier.InitDBConnections()
	suite.Require().NoError(err)

	err = applier.CreateChangelogTable()
	suite.Require().NoError(err)

	err = applier.ReadMigrationRangeValues()
	suite.Require().NoError(err)

	err = applier.AlterGhost()
	suite.Require().NoError(err)

	migrationContext.SetNextIterationRangeMinValues()
	hasFurtherRange, err := applier.CalculateNextIterationRangeEndValues()
	suite.Require().NoError(err)
	suite.Require().True(hasFurtherRange)

	_, rowsAffected, _, err := applier.ApplyIterationInsertQuery()
	suite.Equal(int64(1), rowsAffected)
	suite.Require().NoError(err)

	// Verify the warning was recorded and will cause the migrator to panic
	suite.Require().NotEmpty(applier.migrationContext.MigrationLastInsertSQLWarnings)
	suite.Require().Contains(applier.migrationContext.MigrationLastInsertSQLWarnings[0], "Warning: Data truncated for column 'name' at row 1")
}

func (suite *ApplierTestSuite) TestWriteCheckpoint() {
	ctx := context.Background()

	var err error

	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id int not null, id2 char(4) CHARACTER SET utf8mb4, primary key(id, id2))", getTestTableName()))
	suite.Require().NoError(err)

	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, id2 char(4) CHARACTER SET utf8mb4, name varchar(20), primary key(id, id2));", getTestGhostTableName()))
	suite.Require().NoError(err)

	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, id2) VALUES (?,?), (?,?), (?,?)", getTestTableName()), 411, "君子懷德", 411, "小人懷土", 212, "君子不器")
	suite.Require().NoError(err)

	connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := newTestMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.InspectorConnectionConfig = connectionConfig
	migrationContext.SetConnectionConfig("innodb")

	migrationContext.AlterStatementOptions = "add column name varchar(20)"
	migrationContext.OriginalTableColumns = sql.NewColumnList([]string{"id", "id2"})
	migrationContext.SharedColumns = sql.NewColumnList([]string{"id", "id2"})
	migrationContext.MappedSharedColumns = sql.NewColumnList([]string{"id", "id2"})
	migrationContext.Checkpoint = true
	migrationContext.UniqueKey = &sql.UniqueKey{
		Name:             "PRIMARY",
		NameInGhostTable: "PRIMARY",
		Columns:          *sql.NewColumnList([]string{"id", "id2"}),
	}

	inspector := NewInspector(migrationContext)
	suite.Require().NoError(inspector.InitDBConnections())

	err = inspector.applyColumnTypes(testMysqlDatabase, testMysqlTableName, &migrationContext.UniqueKey.Columns)
	suite.Require().NoError(err)

	applier := NewApplier(migrationContext)

	err = applier.InitDBConnections()
	suite.Require().NoError(err)

	err = applier.CreateChangelogTable()
	suite.Require().NoError(err)

	err = applier.CreateCheckpointTable()
	suite.Require().NoError(err)

	err = applier.prepareQueries()
	suite.Require().NoError(err)

	err = applier.ReadMigrationRangeValues()
	suite.Require().NoError(err)

	// checkpoint table is empty
	_, err = applier.ReadLastCheckpoint()
	suite.Require().ErrorIs(err, ErrNoCheckpointFound)

	// write a checkpoint and read it back
	coords := mysql.NewFileBinlogCoordinates("mysql-bin.000003", int64(219202907))

	chk := &Checkpoint{
		LastTrxCoords:     coords,
		IterationRangeMin: applier.migrationContext.MigrationRangeMinValues,
		IterationRangeMax: applier.migrationContext.MigrationRangeMaxValues,
		Iteration:         2,
		RowsCopied:        100000,
		DMLApplied:        200000,
		IsCutover:         true,
	}
	id, err := applier.WriteCheckpoint(chk)
	suite.Require().NoError(err)
	suite.Require().Equal(int64(1), id)

	gotChk, err := applier.ReadLastCheckpoint()
	suite.Require().NoError(err)

	suite.Require().Equal(chk.Iteration, gotChk.Iteration)
	suite.Require().Equal(chk.LastTrxCoords.String(), gotChk.LastTrxCoords.String())
	suite.Require().Equal(chk.IterationRangeMin.String(), gotChk.IterationRangeMin.String())
	suite.Require().Equal(chk.IterationRangeMax.String(), gotChk.IterationRangeMax.String())
	suite.Require().Equal(chk.RowsCopied, gotChk.RowsCopied)
	suite.Require().Equal(chk.DMLApplied, gotChk.DMLApplied)
	suite.Require().Equal(chk.IsCutover, gotChk.IsCutover)
}

func (suite *ApplierTestSuite) TestPanicOnWarningsWithDuplicateKeyOnNonMigrationIndex() {
	ctx := context.Background()

	var err error

	// Create table with id and email columns, where id is the primary key
	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, email VARCHAR(100));", getTestTableName()))
	suite.Require().NoError(err)

	// Create ghost table with same schema plus a new unique index on email
	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, email VARCHAR(100), UNIQUE KEY email_unique (email));", getTestGhostTableName()))
	suite.Require().NoError(err)

	connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := newTestMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.SetConnectionConfig("innodb")

	migrationContext.PanicOnWarnings = true

	migrationContext.OriginalTableColumns = sql.NewColumnList([]string{"id", "email"})
	migrationContext.SharedColumns = sql.NewColumnList([]string{"id", "email"})
	migrationContext.MappedSharedColumns = sql.NewColumnList([]string{"id", "email"})
	migrationContext.UniqueKey = &sql.UniqueKey{
		Name:             "PRIMARY",
		NameInGhostTable: "PRIMARY",
		Columns:          *sql.NewColumnList([]string{"id"}),
	}

	applier := NewApplier(migrationContext)
	suite.Require().NoError(applier.prepareQueries())
	defer applier.Teardown()

	err = applier.InitDBConnections()
	suite.Require().NoError(err)

	// Insert initial rows into ghost table (simulating bulk copy phase)
	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, email) VALUES (1, 'user1@example.com'), (2, 'user2@example.com'), (3, 'user3@example.com');", getTestGhostTableName()))
	suite.Require().NoError(err)

	// Simulate binlog event: try to insert a row with duplicate email
	// This should fail with a warning because the ghost table has a unique index on email
	dmlEvents := []*binlog.BinlogDMLEvent{
		{
			DatabaseName:    testMysqlDatabase,
			TableName:       testMysqlTableName,
			DML:             binlog.InsertDML,
			NewColumnValues: sql.ToColumnValues([]interface{}{4, "user2@example.com"}), // duplicate email
		},
	}

	// This should return an error when PanicOnWarnings is enabled
	err = applier.ApplyDMLEventQueries(dmlEvents)
	suite.Require().Error(err)
	suite.Require().Contains(err.Error(), "Duplicate entry")

	// Verify that the ghost table still has only the original 3 rows with correct data (no data loss)
	rows, err := suite.db.Query("SELECT id, email FROM " + getTestGhostTableName() + " ORDER BY id")
	suite.Require().NoError(err)
	defer rows.Close()

	var results []struct {
		id    int
		email string
	}
	for rows.Next() {
		var id int
		var email string
		err = rows.Scan(&id, &email)
		suite.Require().NoError(err)
		results = append(results, struct {
			id    int
			email string
		}{id, email})
	}
	suite.Require().NoError(rows.Err())

	// All 3 original rows should still be present with correct data
	suite.Require().Len(results, 3)
	suite.Require().Equal(1, results[0].id)
	suite.Require().Equal("user1@example.com", results[0].email)
	suite.Require().Equal(2, results[1].id)
	suite.Require().Equal("user2@example.com", results[1].email)
	suite.Require().Equal(3, results[2].id)
	suite.Require().Equal("user3@example.com", results[2].email)
}

func (suite *ApplierTestSuite) TestPanicOnWarningsWithDuplicateCompositeUniqueKey() {
	ctx := context.Background()

	var err error

	// Create table with id, email, and username columns
	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, email VARCHAR(100), username VARCHAR(100));", getTestTableName()))
	suite.Require().NoError(err)

	// Create ghost table with same schema plus a composite unique index on (email, username)
	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, email VARCHAR(100), username VARCHAR(100), UNIQUE KEY email_username_unique (email, username));", getTestGhostTableName()))
	suite.Require().NoError(err)

	connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := newTestMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.SetConnectionConfig("innodb")

	migrationContext.PanicOnWarnings = true

	migrationContext.OriginalTableColumns = sql.NewColumnList([]string{"id", "email", "username"})
	migrationContext.SharedColumns = sql.NewColumnList([]string{"id", "email", "username"})
	migrationContext.MappedSharedColumns = sql.NewColumnList([]string{"id", "email", "username"})
	migrationContext.UniqueKey = &sql.UniqueKey{
		Name:             "PRIMARY",
		NameInGhostTable: "PRIMARY",
		Columns:          *sql.NewColumnList([]string{"id"}),
	}

	applier := NewApplier(migrationContext)
	suite.Require().NoError(applier.prepareQueries())
	defer applier.Teardown()

	err = applier.InitDBConnections()
	suite.Require().NoError(err)

	// Insert initial rows into ghost table (simulating bulk copy phase)
	// alice@example.com + bob is ok due to composite unique index
	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, email, username) VALUES (1, 'alice@example.com', 'alice'), (2, 'alice@example.com', 'bob'), (3, 'charlie@example.com', 'charlie');", getTestGhostTableName()))
	suite.Require().NoError(err)

	// Simulate binlog event: try to insert a row with duplicate composite key (email + username)
	// This should fail with a warning because the ghost table has a composite unique index
	dmlEvents := []*binlog.BinlogDMLEvent{
		{
			DatabaseName:    testMysqlDatabase,
			TableName:       testMysqlTableName,
			DML:             binlog.InsertDML,
			NewColumnValues: sql.ToColumnValues([]interface{}{4, "alice@example.com", "alice"}), // duplicate (email, username)
		},
	}

	// This should return an error when PanicOnWarnings is enabled
	err = applier.ApplyDMLEventQueries(dmlEvents)
	suite.Require().Error(err)
	suite.Require().Contains(err.Error(), "Duplicate entry")

	// Verify that the ghost table still has only the original 3 rows with correct data (no data loss)
	rows, err := suite.db.Query("SELECT id, email, username FROM " + getTestGhostTableName() + " ORDER BY id")
	suite.Require().NoError(err)
	defer rows.Close()

	var results []struct {
		id       int
		email    string
		username string
	}
	for rows.Next() {
		var id int
		var email string
		var username string
		err = rows.Scan(&id, &email, &username)
		suite.Require().NoError(err)
		results = append(results, struct {
			id       int
			email    string
			username string
		}{id, email, username})
	}
	suite.Require().NoError(rows.Err())

	// All 3 original rows should still be present with correct data
	suite.Require().Len(results, 3)
	suite.Require().Equal(1, results[0].id)
	suite.Require().Equal("alice@example.com", results[0].email)
	suite.Require().Equal("alice", results[0].username)
	suite.Require().Equal(2, results[1].id)
	suite.Require().Equal("alice@example.com", results[1].email)
	suite.Require().Equal("bob", results[1].username)
	suite.Require().Equal(3, results[2].id)
	suite.Require().Equal("charlie@example.com", results[2].email)
	suite.Require().Equal("charlie", results[2].username)
}

// TestUpdateModifyingUniqueKeyWithDuplicateOnOtherIndex tests the scenario where:
// 1. An UPDATE modifies the unique key (converted to DELETE+INSERT)
// 2. The INSERT would create a duplicate on a NON-migration unique index
// 3. Without warning detection: DELETE succeeds, INSERT IGNORE skips = DATA LOSS
// 4. With PanicOnWarnings: Warning detected, transaction rolled back, no data loss
// This test verifies that PanicOnWarnings correctly prevents the data loss scenario.
func (suite *ApplierTestSuite) TestUpdateModifyingUniqueKeyWithDuplicateOnOtherIndex() {
	ctx := context.Background()

	var err error

	// Create table with id (PRIMARY) and email (NO unique constraint yet)
	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, email VARCHAR(100));", getTestTableName()))
	suite.Require().NoError(err)

	// Create ghost table with id (PRIMARY) AND email unique index (being added)
	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, email VARCHAR(100), UNIQUE KEY email_unique (email));", getTestGhostTableName()))
	suite.Require().NoError(err)

	connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := newTestMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.SetConnectionConfig("innodb")

	migrationContext.PanicOnWarnings = true

	migrationContext.OriginalTableColumns = sql.NewColumnList([]string{"id", "email"})
	migrationContext.SharedColumns = sql.NewColumnList([]string{"id", "email"})
	migrationContext.MappedSharedColumns = sql.NewColumnList([]string{"id", "email"})
	migrationContext.UniqueKey = &sql.UniqueKey{
		Name:             "PRIMARY",
		NameInGhostTable: "PRIMARY",
		Columns:          *sql.NewColumnList([]string{"id"}),
	}

	applier := NewApplier(migrationContext)
	suite.Require().NoError(applier.prepareQueries())
	defer applier.Teardown()

	err = applier.InitDBConnections()
	suite.Require().NoError(err)

	// Setup: Insert initial rows into ghost table
	// Row 1: id=1, email='bob@example.com'
	// Row 2: id=2, email='charlie@example.com'
	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, email) VALUES (1, 'bob@example.com'), (2, 'charlie@example.com');", getTestGhostTableName()))
	suite.Require().NoError(err)

	// Simulate binlog event: UPDATE that changes BOTH PRIMARY KEY and email
	// From: id=2, email='charlie@example.com'
	// To:   id=3, email='bob@example.com'  (duplicate email with id=1)
	// This will be converted to DELETE (id=2) + INSERT (id=3, 'bob@example.com')
	// With INSERT IGNORE, the INSERT will skip because email='bob@example.com' already exists in id=1
	// Result: id=2 deleted, id=3 never inserted = DATA LOSS
	dmlEvents := []*binlog.BinlogDMLEvent{
		{
			DatabaseName:      testMysqlDatabase,
			TableName:         testMysqlTableName,
			DML:               binlog.UpdateDML,
			NewColumnValues:   sql.ToColumnValues([]interface{}{3, "bob@example.com"}),     // new: id=3, email='bob@example.com'
			WhereColumnValues: sql.ToColumnValues([]interface{}{2, "charlie@example.com"}), // old: id=2, email='charlie@example.com'
		},
	}

	// First verify this would be converted to DELETE+INSERT
	buildResults := applier.buildDMLEventQuery(dmlEvents[0])
	suite.Require().Len(buildResults, 2, "UPDATE modifying unique key should be converted to DELETE+INSERT")

	// Apply the event - this should FAIL because INSERT will have duplicate email warning
	err = applier.ApplyDMLEventQueries(dmlEvents)
	suite.Require().Error(err, "Should fail when DELETE+INSERT causes duplicate on non-migration unique key")
	suite.Require().Contains(err.Error(), "Duplicate entry", "Error should mention duplicate entry")

	// Verify that BOTH rows still exist (transaction rolled back)
	rows, err := suite.db.Query("SELECT id, email FROM " + getTestGhostTableName() + " ORDER BY id")
	suite.Require().NoError(err)
	defer rows.Close()

	var count int
	var ids []int
	var emails []string
	for rows.Next() {
		var id int
		var email string
		err = rows.Scan(&id, &email)
		suite.Require().NoError(err)
		ids = append(ids, id)
		emails = append(emails, email)
		count++
	}
	suite.Require().NoError(rows.Err())

	// Transaction should have rolled back, so original 2 rows should still be there
	suite.Require().Equal(2, count, "Should still have 2 rows after failed transaction")
	suite.Require().Equal([]int{1, 2}, ids, "Should have original ids")
	suite.Require().Equal([]string{"bob@example.com", "charlie@example.com"}, emails)
}

// TestNormalUpdateWithPanicOnWarnings tests that normal UPDATEs (not modifying unique key) work correctly
func (suite *ApplierTestSuite) TestNormalUpdateWithPanicOnWarnings() {
	ctx := context.Background()

	var err error

	// Create table with id (PRIMARY) and email
	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, email VARCHAR(100));", getTestTableName()))
	suite.Require().NoError(err)

	// Create ghost table with same schema plus unique index on email
	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, email VARCHAR(100), UNIQUE KEY email_unique (email));", getTestGhostTableName()))
	suite.Require().NoError(err)

	connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := newTestMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.SetConnectionConfig("innodb")

	migrationContext.PanicOnWarnings = true

	migrationContext.OriginalTableColumns = sql.NewColumnList([]string{"id", "email"})
	migrationContext.SharedColumns = sql.NewColumnList([]string{"id", "email"})
	migrationContext.MappedSharedColumns = sql.NewColumnList([]string{"id", "email"})
	migrationContext.UniqueKey = &sql.UniqueKey{
		Name:             "PRIMARY",
		NameInGhostTable: "PRIMARY",
		Columns:          *sql.NewColumnList([]string{"id"}),
	}

	applier := NewApplier(migrationContext)
	suite.Require().NoError(applier.prepareQueries())
	defer applier.Teardown()

	err = applier.InitDBConnections()
	suite.Require().NoError(err)

	// Setup: Insert initial rows into ghost table
	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, email) VALUES (1, 'alice@example.com'), (2, 'bob@example.com');", getTestGhostTableName()))
	suite.Require().NoError(err)

	// Simulate binlog event: Normal UPDATE that only changes email (not PRIMARY KEY)
	// This should use UPDATE query, not DELETE+INSERT
	dmlEvents := []*binlog.BinlogDMLEvent{
		{
			DatabaseName:      testMysqlDatabase,
			TableName:         testMysqlTableName,
			DML:               binlog.UpdateDML,
			NewColumnValues:   sql.ToColumnValues([]interface{}{2, "robert@example.com"}), // update email only
			WhereColumnValues: sql.ToColumnValues([]interface{}{2, "bob@example.com"}),
		},
	}

	// Verify this generates a single UPDATE query (not DELETE+INSERT)
	buildResults := applier.buildDMLEventQuery(dmlEvents[0])
	suite.Require().Len(buildResults, 1, "Normal UPDATE should generate single UPDATE query")

	// Apply the event - should succeed
	err = applier.ApplyDMLEventQueries(dmlEvents)
	suite.Require().NoError(err)

	// Verify the update was applied correctly
	rows, err := suite.db.Query("SELECT id, email FROM " + getTestGhostTableName() + " WHERE id = 2")
	suite.Require().NoError(err)
	defer rows.Close()

	var id int
	var email string
	suite.Require().True(rows.Next(), "Should find updated row")
	err = rows.Scan(&id, &email)
	suite.Require().NoError(err)
	suite.Require().Equal(2, id)
	suite.Require().Equal("robert@example.com", email)
	suite.Require().False(rows.Next(), "Should only have one row")
	suite.Require().NoError(rows.Err())
}

// TestDuplicateOnMigrationKeyAllowedInBinlogReplay tests the positive case where
// a duplicate on the migration unique key during binlog replay is expected and should be allowed
func (suite *ApplierTestSuite) TestDuplicateOnMigrationKeyAllowedInBinlogReplay() {
	ctx := context.Background()

	var err error

	// Create table with id and email columns, where id is the primary key
	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, email VARCHAR(100));", getTestTableName()))
	suite.Require().NoError(err)

	// Create ghost table with same schema plus a new unique index on email
	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, email VARCHAR(100), UNIQUE KEY email_unique (email));", getTestGhostTableName()))
	suite.Require().NoError(err)

	connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := newTestMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.SetConnectionConfig("innodb")

	migrationContext.PanicOnWarnings = true

	migrationContext.OriginalTableColumns = sql.NewColumnList([]string{"id", "email"})
	migrationContext.SharedColumns = sql.NewColumnList([]string{"id", "email"})
	migrationContext.MappedSharedColumns = sql.NewColumnList([]string{"id", "email"})
	migrationContext.UniqueKey = &sql.UniqueKey{
		Name:             "PRIMARY",
		NameInGhostTable: "PRIMARY",
		Columns:          *sql.NewColumnList([]string{"id"}),
	}

	applier := NewApplier(migrationContext)
	suite.Require().NoError(applier.prepareQueries())
	defer applier.Teardown()

	err = applier.InitDBConnections()
	suite.Require().NoError(err)

	// Insert initial rows into ghost table (simulating bulk copy phase)
	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, email) VALUES (1, 'alice@example.com'), (2, 'bob@example.com');", getTestGhostTableName()))
	suite.Require().NoError(err)

	// Simulate binlog event: try to insert the same row again (duplicate on PRIMARY KEY - the migration key)
	// This is expected during binlog replay when a row was already copied during bulk copy
	dmlEvents := []*binlog.BinlogDMLEvent{
		{
			DatabaseName:    testMysqlDatabase,
			TableName:       testMysqlTableName,
			DML:             binlog.InsertDML,
			NewColumnValues: sql.ToColumnValues([]interface{}{1, "alice@example.com"}), // duplicate PRIMARY KEY
		},
	}

	// This should succeed - duplicate on migration unique key is expected and should be filtered out
	err = applier.ApplyDMLEventQueries(dmlEvents)
	suite.Require().NoError(err)

	// Verify that the ghost table still has only the original 2 rows with correct data
	rows, err := suite.db.Query("SELECT id, email FROM " + getTestGhostTableName() + " ORDER BY id")
	suite.Require().NoError(err)
	defer rows.Close()

	var results []struct {
		id    int
		email string
	}
	for rows.Next() {
		var id int
		var email string
		err = rows.Scan(&id, &email)
		suite.Require().NoError(err)
		results = append(results, struct {
			id    int
			email string
		}{id, email})
	}
	suite.Require().NoError(rows.Err())

	// Should still have exactly 2 rows with correct data
	suite.Require().Len(results, 2)
	suite.Require().Equal(1, results[0].id)
	suite.Require().Equal("alice@example.com", results[0].email)
	suite.Require().Equal(2, results[1].id)
	suite.Require().Equal("bob@example.com", results[1].email)
}

// TestRegexMetacharactersInIndexName tests that index names with regex metacharacters
// are properly escaped. We test with a plus sign in the index name, which without
// QuoteMeta would be treated as a regex quantifier (one or more of 'x' in this case).
// This test verifies the pattern matches ONLY the exact index name, not a regex pattern.
func (suite *ApplierTestSuite) TestRegexMetacharactersInIndexName() {
	ctx := context.Background()

	var err error

	// Create tables with an index name containing a plus sign
	// Without QuoteMeta, "idx+email" would be treated as a regex pattern where + is a quantifier
	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, email VARCHAR(100), UNIQUE KEY `idx+email` (email));", getTestTableName()))
	suite.Require().NoError(err)

	// MySQL allows + in index names when quoted
	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, email VARCHAR(100), UNIQUE KEY `idx+email` (email));", getTestGhostTableName()))
	suite.Require().NoError(err)

	connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := newTestMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.SetConnectionConfig("innodb")

	migrationContext.PanicOnWarnings = true

	migrationContext.OriginalTableColumns = sql.NewColumnList([]string{"id", "email"})
	migrationContext.SharedColumns = sql.NewColumnList([]string{"id", "email"})
	migrationContext.MappedSharedColumns = sql.NewColumnList([]string{"id", "email"})
	migrationContext.UniqueKey = &sql.UniqueKey{
		Name:             "idx+email",
		NameInGhostTable: "idx+email",
		Columns:          *sql.NewColumnList([]string{"email"}),
	}

	applier := NewApplier(migrationContext)
	suite.Require().NoError(applier.prepareQueries())
	defer applier.Teardown()

	err = applier.InitDBConnections()
	suite.Require().NoError(err)

	// Insert initial rows
	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, email) VALUES (1, 'alice@example.com'), (2, 'bob@example.com');", getTestGhostTableName()))
	suite.Require().NoError(err)

	// Test: duplicate on idx+email (the migration key) should be allowed
	// This verifies our regex correctly identifies "idx+email" as the migration key
	// Without regexp.QuoteMeta, the + would be treated as a regex quantifier and might not match correctly
	dmlEvents := []*binlog.BinlogDMLEvent{
		{
			DatabaseName:    testMysqlDatabase,
			TableName:       testMysqlTableName,
			DML:             binlog.InsertDML,
			NewColumnValues: sql.ToColumnValues([]interface{}{3, "alice@example.com"}),
		},
	}

	err = applier.ApplyDMLEventQueries(dmlEvents)
	suite.Require().NoError(err, "Duplicate on idx+email (migration key) should be allowed with PanicOnWarnings enabled")

	// Test: duplicate on PRIMARY (not the migration key) should fail
	dmlEvents = []*binlog.BinlogDMLEvent{
		{
			DatabaseName:    testMysqlDatabase,
			TableName:       testMysqlTableName,
			DML:             binlog.InsertDML,
			NewColumnValues: sql.ToColumnValues([]interface{}{1, "charlie@example.com"}),
		},
	}

	err = applier.ApplyDMLEventQueries(dmlEvents)
	suite.Require().Error(err, "Duplicate on PRIMARY (not migration key) should fail with PanicOnWarnings enabled")
	suite.Require().Contains(err.Error(), "Duplicate entry")

	// Verify final state - should still have only the original 2 rows
	rows, err := suite.db.Query("SELECT id, email FROM " + getTestGhostTableName() + " ORDER BY id")
	suite.Require().NoError(err)
	defer rows.Close()

	var results []struct {
		id    int
		email string
	}
	for rows.Next() {
		var id int
		var email string
		err = rows.Scan(&id, &email)
		suite.Require().NoError(err)
		results = append(results, struct {
			id    int
			email string
		}{id, email})
	}
	suite.Require().NoError(rows.Err())

	suite.Require().Len(results, 2)
	suite.Require().Equal(1, results[0].id)
	suite.Require().Equal("alice@example.com", results[0].email)
	suite.Require().Equal(2, results[1].id)
	suite.Require().Equal("bob@example.com", results[1].email)
}

// TestPanicOnWarningsDisabled tests that when PanicOnWarnings is false,
// warnings are not checked and duplicates are silently ignored
func (suite *ApplierTestSuite) TestPanicOnWarningsDisabled() {
	ctx := context.Background()

	var err error

	// Create table with id and email columns
	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, email VARCHAR(100));", getTestTableName()))
	suite.Require().NoError(err)

	// Create ghost table with unique index on email
	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, email VARCHAR(100), UNIQUE KEY email_unique (email));", getTestGhostTableName()))
	suite.Require().NoError(err)

	connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := newTestMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.SetConnectionConfig("innodb")

	// PanicOnWarnings is false (default)
	migrationContext.PanicOnWarnings = false

	migrationContext.OriginalTableColumns = sql.NewColumnList([]string{"id", "email"})
	migrationContext.SharedColumns = sql.NewColumnList([]string{"id", "email"})
	migrationContext.MappedSharedColumns = sql.NewColumnList([]string{"id", "email"})
	migrationContext.UniqueKey = &sql.UniqueKey{
		Name:             "PRIMARY",
		NameInGhostTable: "PRIMARY",
		Columns:          *sql.NewColumnList([]string{"id"}),
	}

	applier := NewApplier(migrationContext)
	suite.Require().NoError(applier.prepareQueries())
	defer applier.Teardown()

	err = applier.InitDBConnections()
	suite.Require().NoError(err)

	// Insert initial rows into ghost table
	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, email) VALUES (1, 'alice@example.com'), (2, 'bob@example.com');", getTestGhostTableName()))
	suite.Require().NoError(err)

	// Simulate binlog event: insert duplicate email on non-migration index
	// With PanicOnWarnings disabled, this should succeed (INSERT IGNORE skips it)
	dmlEvents := []*binlog.BinlogDMLEvent{
		{
			DatabaseName:    testMysqlDatabase,
			TableName:       testMysqlTableName,
			DML:             binlog.InsertDML,
			NewColumnValues: sql.ToColumnValues([]interface{}{3, "alice@example.com"}), // duplicate email
		},
	}

	// Should succeed because PanicOnWarnings is disabled
	err = applier.ApplyDMLEventQueries(dmlEvents)
	suite.Require().NoError(err)

	// Verify that only 2 original rows exist with correct data (the duplicate was silently ignored)
	rows, err := suite.db.Query("SELECT id, email FROM " + getTestGhostTableName() + " ORDER BY id")
	suite.Require().NoError(err)
	defer rows.Close()

	var results []struct {
		id    int
		email string
	}
	for rows.Next() {
		var id int
		var email string
		err = rows.Scan(&id, &email)
		suite.Require().NoError(err)
		results = append(results, struct {
			id    int
			email string
		}{id, email})
	}
	suite.Require().NoError(rows.Err())

	// Should still have exactly 2 original rows (id=3 was silently ignored)
	suite.Require().Len(results, 2)
	suite.Require().Equal(1, results[0].id)
	suite.Require().Equal("alice@example.com", results[0].email)
	suite.Require().Equal(2, results[1].id)
	suite.Require().Equal("bob@example.com", results[1].email)
}

// TestMultipleDMLEventsInBatch tests that multiple DML events are processed in a single transaction
// and that if one fails due to a warning, the entire batch is rolled back - including events that
// come AFTER the failure. This proves true transaction atomicity.
func (suite *ApplierTestSuite) TestMultipleDMLEventsInBatch() {
	ctx := context.Background()

	var err error

	// Create table with id and email columns
	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, email VARCHAR(100));", getTestTableName()))
	suite.Require().NoError(err)

	// Create ghost table with unique index on email
	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, email VARCHAR(100), UNIQUE KEY email_unique (email));", getTestGhostTableName()))
	suite.Require().NoError(err)

	connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := newTestMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.SetConnectionConfig("innodb")

	migrationContext.PanicOnWarnings = true

	migrationContext.OriginalTableColumns = sql.NewColumnList([]string{"id", "email"})
	migrationContext.SharedColumns = sql.NewColumnList([]string{"id", "email"})
	migrationContext.MappedSharedColumns = sql.NewColumnList([]string{"id", "email"})
	migrationContext.UniqueKey = &sql.UniqueKey{
		Name:             "PRIMARY",
		NameInGhostTable: "PRIMARY",
		Columns:          *sql.NewColumnList([]string{"id"}),
	}

	applier := NewApplier(migrationContext)
	suite.Require().NoError(applier.prepareQueries())
	defer applier.Teardown()

	err = applier.InitDBConnections()
	suite.Require().NoError(err)

	// Insert initial rows into ghost table
	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, email) VALUES (1, 'alice@example.com'), (3, 'charlie@example.com');", getTestGhostTableName()))
	suite.Require().NoError(err)

	// Simulate multiple binlog events in a batch:
	// 1. Duplicate on PRIMARY KEY (allowed - expected during binlog replay)
	// 2. Duplicate on email index (should fail) ← FAILURE IN MIDDLE
	// 3. Valid insert (would succeed) ← SUCCESS AFTER FAILURE
	//
	// The critical test: Even though event #3 would succeed on its own, it must be rolled back
	// because event #2 failed. This proves the entire batch is truly atomic.
	dmlEvents := []*binlog.BinlogDMLEvent{
		{
			DatabaseName:    testMysqlDatabase,
			TableName:       testMysqlTableName,
			DML:             binlog.InsertDML,
			NewColumnValues: sql.ToColumnValues([]interface{}{1, "alice@example.com"}), // duplicate PRIMARY (normally allowed)
		},
		{
			DatabaseName:    testMysqlDatabase,
			TableName:       testMysqlTableName,
			DML:             binlog.InsertDML,
			NewColumnValues: sql.ToColumnValues([]interface{}{4, "alice@example.com"}), // duplicate email (FAILS)
		},
		{
			DatabaseName:    testMysqlDatabase,
			TableName:       testMysqlTableName,
			DML:             binlog.InsertDML,
			NewColumnValues: sql.ToColumnValues([]interface{}{2, "bob@example.com"}), // valid insert (would succeed)
		},
	}

	// Should fail due to the second event
	err = applier.ApplyDMLEventQueries(dmlEvents)
	suite.Require().Error(err)
	suite.Require().Contains(err.Error(), "Duplicate entry")

	// Verify that the entire batch was rolled back - still only the original 2 rows
	// Critically: id=2 (bob@example.com) from event #3 should NOT be present
	rows, err := suite.db.Query("SELECT id, email FROM " + getTestGhostTableName() + " ORDER BY id")
	suite.Require().NoError(err)
	defer rows.Close()

	var results []struct {
		id    int
		email string
	}
	for rows.Next() {
		var id int
		var email string
		err = rows.Scan(&id, &email)
		suite.Require().NoError(err)
		results = append(results, struct {
			id    int
			email string
		}{id, email})
	}
	suite.Require().NoError(rows.Err())

	// Should still have exactly 2 original rows (entire batch was rolled back)
	// This proves that even event #3 (which would have succeeded) was rolled back
	suite.Require().Len(results, 2)
	suite.Require().Equal(1, results[0].id)
	suite.Require().Equal("alice@example.com", results[0].email)
	suite.Require().Equal(3, results[1].id)
	suite.Require().Equal("charlie@example.com", results[1].email)
	// Critically: id=2 (bob@example.com) is NOT present, proving event #3 was rolled back
}

func TestApplier(t *testing.T) {
	suite.Run(t, new(ApplierTestSuite))
}
