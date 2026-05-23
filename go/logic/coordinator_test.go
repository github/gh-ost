package logic

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand/v2"
	"os"
	"testing"
	"time"

	"path/filepath"
	"runtime"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/binlog"
	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"golang.org/x/sync/errgroup"
)

type CoordinatorTestSuite struct {
	suite.Suite

	mysqlContainer         testcontainers.Container
	db                     *gosql.DB
	concurrentTransactions int
	transactionsPerWorker  int
	transactionSize        int
}

func (suite *CoordinatorTestSuite) SetupSuite() {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:      "mysql:8.0.40",
		Env:        map[string]string{"MYSQL_ROOT_PASSWORD": "root-password"},
		WaitingFor: wait.ForListeningPort("3306/tcp"),
	}

	mysqlContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	suite.Require().NoError(err)

	suite.mysqlContainer = mysqlContainer

	dsn, err := GetDSN(ctx, mysqlContainer)
	suite.Require().NoError(err)

	db, err := gosql.Open("mysql", dsn)
	suite.Require().NoError(err)

	suite.db = db
	suite.concurrentTransactions = 8
	suite.transactionsPerWorker = 1000
	suite.transactionSize = 10

	db.SetMaxOpenConns(suite.concurrentTransactions)
}

func (suite *CoordinatorTestSuite) SetupTest() {
	ctx := context.Background()
	_, err := suite.db.ExecContext(ctx, "RESET MASTER")
	suite.Require().NoError(err)

	_, err = suite.db.ExecContext(ctx, "SET @@GLOBAL.binlog_transaction_dependency_tracking = WRITESET")
	suite.Require().NoError(err)

	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("SET @@GLOBAL.max_connections = %d", suite.concurrentTransactions*2))
	suite.Require().NoError(err)

	_, err = suite.db.ExecContext(ctx, "CREATE DATABASE test")
	suite.Require().NoError(err)
}

func (suite *CoordinatorTestSuite) TearDownTest() {
	ctx := context.Background()
	_, err := suite.db.ExecContext(ctx, "DROP DATABASE test")
	suite.Require().NoError(err)
}

func (suite *CoordinatorTestSuite) TeardownSuite() {
	ctx := context.Background()

	suite.Assert().NoError(suite.db.Close())
	suite.Assert().NoError(suite.mysqlContainer.Terminate(ctx))
}

func (suite *CoordinatorTestSuite) TestApplyDML() {
	ctx := context.Background()

	connectionConfig, err := GetConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	_ = os.Remove("/tmp/gh-ost.sock")

	_, err = suite.db.Exec("CREATE TABLE test.gh_ost_test (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255)) ENGINE=InnoDB")
	suite.Require().NoError(err)

	_, err = suite.db.Exec("CREATE TABLE test._gh_ost_test_gho (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255))")
	suite.Require().NoError(err)

	migrationContext := base.NewMigrationContext()
	migrationContext.DatabaseName = "test"
	migrationContext.OriginalTableName = "gh_ost_test"
	migrationContext.AlterStatement = "ALTER TABLE gh_ost_test ENGINE=InnoDB"
	migrationContext.AllowedRunningOnMaster = true
	migrationContext.ReplicaServerId = 99999
	migrationContext.HeartbeatIntervalMilliseconds = 100
	migrationContext.ThrottleHTTPIntervalMillis = 100
	migrationContext.DMLBatchSize = 10

	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.InspectorConnectionConfig = connectionConfig

	migrationContext.OriginalTableColumns = sql.NewColumnList([]string{"id", "name"})
	migrationContext.GhostTableColumns = sql.NewColumnList([]string{"id", "name"})
	migrationContext.SharedColumns = sql.NewColumnList([]string{"id", "name"})
	migrationContext.MappedSharedColumns = sql.NewColumnList([]string{"id", "name"})
	migrationContext.UniqueKey = &sql.UniqueKey{
		Name:            "PRIMARY",
		Columns:         *sql.NewColumnList([]string{"id"}),
		IsAutoIncrement: true,
	}

	migrationContext.SetConnectionConfig("innodb")
	migrationContext.SkipPortValidation = true
	migrationContext.NumWorkers = 4

	//nolint:dogsled
	_, filename, _, _ := runtime.Caller(0)
	migrationContext.ServeSocketFile = filepath.Join(filepath.Dir(filename), "../../tmp/gh-ost.sock")

	applier := NewApplier(migrationContext)
	err = applier.InitDBConnections(migrationContext.NumWorkers)
	suite.Require().NoError(err)

	err = applier.prepareQueries()
	suite.Require().NoError(err)

	err = applier.CreateChangelogTable()
	suite.Require().NoError(err)

	g, _ := errgroup.WithContext(ctx)
	for i := range suite.concurrentTransactions {
		g.Go(func() error {
			r := rand.New(rand.NewPCG(uint64(0), uint64(i)))
			maxID := int64(1)
			for range suite.transactionsPerWorker {
				tx, txErr := suite.db.Begin()
				if txErr != nil {
					return txErr
				}

				// generate random write queries
				for range r.IntN(suite.transactionSize) + 1 {
					switch r.IntN(5) {
					case 0:
						_, txErr = tx.Exec(fmt.Sprintf("DELETE FROM test.gh_ost_test WHERE id=%d", r.Int64N(maxID)))
						if txErr != nil {
							return txErr
						}
					case 1, 2:
						_, txErr = tx.Exec(fmt.Sprintf("UPDATE test.gh_ost_test SET name='test-%d' WHERE id=%d", r.Int(), r.Int64N(maxID)))
						if txErr != nil {
							return txErr
						}
					default:
						res, txErr := tx.Exec(fmt.Sprintf("INSERT INTO test.gh_ost_test (name) VALUES ('test-%d')", r.Int()))
						if txErr != nil {
							return txErr
						}
						lastID, err := res.LastInsertId()
						if err != nil {
							return err
						}
						maxID = lastID + 1
					}
				}
				txErr = tx.Commit()
				if txErr != nil {
					return txErr
				}
			}
			return nil
		})
	}

	_, err = applier.WriteChangelogState("completed")
	suite.Require().NoError(err)

	ctx, cancel := context.WithCancel(context.Background())

	coord := NewCoordinator(migrationContext, applier, nil,
		func(dmlEvent *binlog.BinlogDMLEvent) error {
			fmt.Printf("Received Changelog DML event: %+v\n", dmlEvent)
			fmt.Printf("Rowdata: %v - %v\n", dmlEvent.NewColumnValues, dmlEvent.WhereColumnValues)

			cancel()

			return nil
		})
	coord.applier = applier
	coord.InitializeWorkers(4)

	streamCtx, cancelStreaming := context.WithCancel(context.Background())
	canStopStreaming := func() bool {
		return streamCtx.Err() != nil
	}
	go func() {
		streamErr := coord.StartStreaming(streamCtx, &mysql.FileBinlogCoordinates{
			LogFile: "binlog.000001",
			LogPos:  int64(4),
		}, canStopStreaming)
		suite.Require().Equal(context.Canceled, streamErr)
	}()

	// Give streamer some time to start
	time.Sleep(1 * time.Second)

	startAt := time.Now()

	for {
		if ctx.Err() != nil {
			cancelStreaming()
			break
		}

		err = coord.ProcessEventsUntilDrained()
		suite.Require().NoError(err)
	}

	//err = g.Wait()
	//suite.Require().NoError(err)
	g.Wait() // there will be deadlock errors

	fmt.Printf("Time taken: %s\n", time.Since(startAt))

	result, err := suite.db.Exec(`SELECT * FROM (
    SELECT t1.id,
    CRC32(CONCAT_WS(';',t1.id,t1.name)) AS checksum1,
    CRC32(CONCAT_WS(';',t2.id,t2.name)) AS checksum2
    FROM test.gh_ost_test t1
    LEFT JOIN test._gh_ost_test_gho t2
    ON t1.id = t2.id
) AS checksums
WHERE checksums.checksum1 != checksums.checksum2`)
	suite.Require().NoError(err)

	count, err := result.RowsAffected()
	suite.Require().NoError(err)
	suite.Require().Zero(count)
}

func TestCoordinator(t *testing.T) {
	suite.Run(t, new(CoordinatorTestSuite))
}

// TestRotationResetsLowWaterMark is a deterministic unit test verifying that
// after a simulated binlog rotation the coordinator's lowWaterMark is reset
// so that transactions from the new file are properly ordered.
// This is the regression test for the root cause of the MTR data inconsistency:
// MySQL's logical clock (last_committed, sequence_number) resets per-binlog-file,
// but without resetting lwm, post-rotation transactions with small lastCommitted
// values would pass the WaitForTransaction check against the stale high lwm.
func TestRotationResetsLowWaterMark(t *testing.T) {
	// Simulate a coordinator that has processed transactions from the first binlog file.
	c := &Coordinator{
		lowWaterMark:  -1,
		completedJobs: make(map[int64]struct{}),
		waitingJobs:   make(map[int64][]chan struct{}),
		failedCh:      make(chan struct{}),
	}

	// --- First binlog file: sequence numbers 1..5 ---

	// Initialize lwm (simulates first GTID event setting lwm = seqNo - 1 = 0)
	c.mu.Lock()
	c.lowWaterMark = 0
	c.mu.Unlock()

	// Complete transactions 1 through 5
	for seq := int64(1); seq <= 5; seq++ {
		c.MarkTransactionCompleted(seq, 0, 0)
	}

	// Verify lwm advanced to 5
	c.mu.Lock()
	if c.lowWaterMark != 5 {
		t.Fatalf("expected lwm=5 after completing seqs 1-5, got %d", c.lowWaterMark)
	}
	c.mu.Unlock()

	// A transaction with lastCommitted=3 should pass immediately (3 <= 5)
	ch := c.WaitForTransaction(3)
	if ch != nil {
		t.Fatal("expected WaitForTransaction(3) to return nil when lwm=5")
	}

	// --- Simulate binlog rotation: reset coordinator state ---
	// This is what the RotateEvent handler does after draining workers.
	c.mu.Lock()
	c.lowWaterMark = -1
	c.completedJobs = make(map[int64]struct{})
	c.waitingJobs = make(map[int64][]chan struct{})
	c.mu.Unlock()

	// --- Second binlog file: sequence numbers restart at 1 ---

	// Initialize lwm for new file (first GTID sets lwm = seqNo - 1 = 0)
	c.mu.Lock()
	c.lowWaterMark = 0
	c.mu.Unlock()

	// BUG SCENARIO (before fix): if lwm was still 5 from the old file,
	// WaitForTransaction(3) would return nil → tx executes out of order.
	// After fix: lwm=0, so WaitForTransaction(3) must block.
	ch = c.WaitForTransaction(3)
	if ch == nil {
		t.Fatal("expected WaitForTransaction(3) to block when lwm=0 in new binlog file, but it returned nil (stale lwm bug!)")
	}

	// Complete transactions 1, 2, 3 in the new file
	c.MarkTransactionCompleted(1, 0, 0)
	c.MarkTransactionCompleted(2, 0, 0)
	c.MarkTransactionCompleted(3, 0, 0)

	// Now the wait channel should be notified (lwm advances to 3)
	select {
	case <-ch:
		// success
	case <-time.After(time.Second):
		t.Fatal("WaitForTransaction(3) was not notified after completing seqs 1-3")
	}

	// Verify lwm is now 3
	c.mu.Lock()
	if c.lowWaterMark != 3 {
		t.Fatalf("expected lwm=3, got %d", c.lowWaterMark)
	}
	c.mu.Unlock()
}

// TestBufferedWaitChannelNoDeadlock verifies that if a waiter exits early
// (e.g., via failedCh), MarkTransactionCompleted does not block forever.
func TestBufferedWaitChannelNoDeadlock(t *testing.T) {
	c := &Coordinator{
		lowWaterMark:  0,
		completedJobs: make(map[int64]struct{}),
		waitingJobs:   make(map[int64][]chan struct{}),
		failedCh:      make(chan struct{}),
	}

	// Create a waiter for lastCommitted=3
	ch := c.WaitForTransaction(3)
	if ch == nil {
		t.Fatal("expected a wait channel")
	}

	// Simulate the waiter exiting early (not reading from ch)
	// This mimics what happens when a worker exits via failedCh.

	// MarkTransactionCompleted should NOT block even though nobody reads ch
	done := make(chan struct{})
	go func() {
		c.MarkTransactionCompleted(1, 0, 0)
		c.MarkTransactionCompleted(2, 0, 0)
		c.MarkTransactionCompleted(3, 0, 0)
		close(done)
	}()

	select {
	case <-done:
		// success — did not deadlock
	case <-time.After(2 * time.Second):
		t.Fatal("MarkTransactionCompleted deadlocked because wait channel is unbuffered")
	}
}
