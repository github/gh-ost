package logic

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/binlog"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"

	"golang.org/x/sync/errgroup"
)

type EventsStreamerTestSuite struct {
	suite.Suite

	mysqlContainer testcontainers.Container
	db             *gosql.DB
}

func (suite *EventsStreamerTestSuite) SetupSuite() {
	ctx := context.Background()
	mysqlContainer, err := mysql.Run(ctx,
		testMysqlContainerImage,
		mysql.WithDatabase(testMysqlDatabase),
		mysql.WithUsername(testMysqlUser),
		mysql.WithPassword(testMysqlPass),
	)
	suite.Require().NoError(err)

	suite.mysqlContainer = mysqlContainer
	dsn, err := mysqlContainer.ConnectionString(ctx)
	suite.Require().NoError(err)

	db, err := gosql.Open("mysql", dsn)
	suite.Require().NoError(err)

	suite.db = db
}

func (suite *EventsStreamerTestSuite) TeardownSuite() {
	suite.Assert().NoError(suite.db.Close())
	suite.Assert().NoError(testcontainers.TerminateContainer(suite.mysqlContainer))
}

func (suite *EventsStreamerTestSuite) SetupTest() {
	ctx := context.Background()

	_, err := suite.db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+testMysqlDatabase)
	suite.Require().NoError(err)
}

func (suite *EventsStreamerTestSuite) TearDownTest() {
	ctx := context.Background()

	_, err := suite.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+getTestTableName())
	suite.Require().NoError(err)
	_, err = suite.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+getTestGhostTableName())
	suite.Require().NoError(err)
}

func (suite *EventsStreamerTestSuite) TestStreamEvents() {
	ctx := context.Background()

	_, err := suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, name VARCHAR(255))", getTestTableName()))
	suite.Require().NoError(err)

	connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := newTestMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.InspectorConnectionConfig = connectionConfig
	migrationContext.SetConnectionConfig("innodb")

	streamer := NewEventsStreamer(migrationContext)

	err = streamer.InitDBConnections()
	suite.Require().NoError(err)
	defer streamer.Close()
	defer streamer.Teardown()

	streamCtx, cancel := context.WithCancel(context.Background())

	dmlEvents := make([]*binlog.BinlogDMLEvent, 0)
	err = streamer.AddListener(false, testMysqlDatabase, testMysqlTableName, func(event *binlog.BinlogEntry) error {
		dmlEvents = append(dmlEvents, event.DmlEvent)

		// Stop once we've collected three events
		if len(dmlEvents) == 3 {
			cancel()
		}

		return nil
	})
	suite.Require().NoError(err)

	group := errgroup.Group{}
	group.Go(func() error {
		//nolint:contextcheck
		return streamer.StreamEvents(func() bool {
			return streamCtx.Err() != nil
		})
	})

	group.Go(func() error {
		var err error

		_, err = suite.db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'foo')", getTestTableName()))
		if err != nil {
			return err
		}

		_, err = suite.db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (2, 'bar')", getTestTableName()))
		if err != nil {
			return err
		}

		_, err = suite.db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (3, 'baz')", getTestTableName()))
		if err != nil {
			return err
		}

		// Bug: Need to write fourth event to hit the canStopStreaming function again
		_, err = suite.db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (4, 'qux')", getTestTableName()))
		if err != nil {
			return err
		}

		return nil
	})

	err = group.Wait()
	suite.Require().NoError(err)

	suite.Require().Len(dmlEvents, 3)
}

func (suite *EventsStreamerTestSuite) TestStreamEventsAutomaticallyReconnects() {
	ctx := context.Background()
	_, err := suite.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, name VARCHAR(255))", getTestTableName()))
	suite.Require().NoError(err)

	connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)

	migrationContext := newTestMigrationContext()
	migrationContext.ApplierConnectionConfig = connectionConfig
	migrationContext.InspectorConnectionConfig = connectionConfig
	migrationContext.SetConnectionConfig("innodb")

	streamer := NewEventsStreamer(migrationContext)

	err = streamer.InitDBConnections()
	suite.Require().NoError(err)
	defer streamer.Close()
	defer streamer.Teardown()

	streamCtx, cancel := context.WithCancel(context.Background())

	dmlEvents := make([]*binlog.BinlogDMLEvent, 0)
	err = streamer.AddListener(false, testMysqlDatabase, testMysqlTableName, func(event *binlog.BinlogEntry) error {
		dmlEvents = append(dmlEvents, event.DmlEvent)

		// Stop once we've collected three events
		if len(dmlEvents) == 3 {
			cancel()
		}

		return nil
	})
	suite.Require().NoError(err)

	group := errgroup.Group{}
	group.Go(func() error {
		//nolint:contextcheck
		return streamer.StreamEvents(func() bool {
			return streamCtx.Err() != nil
		})
	})

	group.Go(func() error {
		var err error

		_, err = suite.db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'foo')", getTestTableName()))
		if err != nil {
			return err
		}

		_, err = suite.db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (2, 'bar')", getTestTableName()))
		if err != nil {
			return err
		}

		var currentConnectionId int
		err = suite.db.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&currentConnectionId)
		if err != nil {
			return err
		}

		rows, err := suite.db.Query("SHOW FULL PROCESSLIST")
		if err != nil {
			return err
		}
		defer rows.Close()

		connectionIdsToKill := make([]int, 0)

		var id, stateTime int
		var user, host, dbName, command, state, info gosql.NullString
		for rows.Next() {
			err = rows.Scan(&id, &user, &host, &dbName, &command, &stateTime, &state, &info)
			if err != nil {
				return err
			}

			fmt.Printf("id: %d, user: %s, host: %s, dbName: %s, command: %s, time: %d, state: %s, info: %s\n", id, user.String, host.String, dbName.String, command.String, stateTime, state.String, info.String)

			if id != currentConnectionId && user.String == testMysqlUser {
				connectionIdsToKill = append(connectionIdsToKill, id)
			}
		}

		if err := rows.Err(); err != nil {
			return err
		}

		for _, connectionIdToKill := range connectionIdsToKill {
			_, err = suite.db.ExecContext(ctx, "KILL ?", connectionIdToKill)
			if err != nil {
				return err
			}
		}

		// Bug: We need to wait here for the streamer to reconnect
		time.Sleep(time.Second * 2)

		_, err = suite.db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (3, 'baz')", getTestTableName()))
		if err != nil {
			return err
		}

		// Bug: Need to write fourth event to hit the canStopStreaming function again
		_, err = suite.db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (4, 'qux')", getTestTableName()))
		if err != nil {
			return err
		}

		return nil
	})

	err = group.Wait()
	suite.Require().NoError(err)

	suite.Require().Len(dmlEvents, 3)
}

func TestEventsStreamerShouldDecodeRowsEvent(t *testing.T) {
	streamer := NewEventsStreamer(newTestMigrationContext())

	if streamer.shouldDecodeRowsEvent(testMysqlDatabase, testMysqlTableName) {
		t.Fatalf("expected no table match before any listeners are registered")
	}

	err := streamer.AddListener(false, testMysqlDatabase, testMysqlTableName, func(event *binlog.BinlogEntry) error {
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected AddListener error: %+v", err)
	}

	if !streamer.shouldDecodeRowsEvent(testMysqlDatabase, testMysqlTableName) {
		t.Fatalf("expected registered table to be decoded")
	}
	if !streamer.shouldDecodeRowsEvent("TEST", "TESTING") {
		t.Fatalf("expected table matching to be case-insensitive")
	}
	if streamer.shouldDecodeRowsEvent(testMysqlDatabase, "other_table") {
		t.Fatalf("expected unregistered table to be skipped")
	}
	if streamer.shouldDecodeRowsEvent("other_database", testMysqlTableName) {
		t.Fatalf("expected unregistered database to be skipped")
	}
}

func TestEventsStreamerInstantDDLDeadlockIsResolvedByDraining(t *testing.T) {
	// Regression test for the instant-DDL deadlock. It reproduces the exact
	// mechanism and proves that draining the GhostTableMigrated signal (what
	// Migrator.drainGhostTableMigrated does on the instant-DDL success path)
	// resolves it.
	//
	// notifyListeners invokes the changelog listener synchronously while holding
	// listenersMutex. The listener publishes on an unbuffered channel (mirroring
	// onChangelogStateEvent -> SendWithContext) and blocks until the signal is
	// received. Meanwhile the binlog reader's shouldDecodeRowsEvent needs the same
	// mutex and blocks as well. Without a receiver both stay blocked forever.
	migrationContext := newTestMigrationContext()
	streamer := NewEventsStreamer(migrationContext)

	ghostTableMigrated := make(chan bool) // unbuffered, mirrors Migrator.ghostTableMigrated

	err := streamer.AddListener(false, testMysqlDatabase, testMysqlTableName, func(event *binlog.BinlogEntry) error {
		return base.SendWithContext(migrationContext.GetContext(), ghostTableMigrated, true)
	})
	require.NoError(t, err)

	entry := &binlog.BinlogEntry{
		DmlEvent: binlog.NewBinlogDMLEvent(testMysqlDatabase, testMysqlTableName, binlog.InsertDML),
	}

	notifyReturned := make(chan struct{})
	go func() {
		streamer.notifyListeners(entry) // holds listenersMutex, blocks on the listener's send
		close(notifyReturned)
	}()

	decodeReturned := make(chan bool, 1)
	go func() {
		decodeReturned <- streamer.shouldDecodeRowsEvent(testMysqlDatabase, testMysqlTableName)
	}()

	// Both goroutines are blocked and cannot progress until the signal is drained:
	// notifyListeners on the send, shouldDecodeRowsEvent on the mutex.
	select {
	case <-notifyReturned:
		t.Fatal("notifyListeners returned before draining; the test no longer reproduces the deadlock")
	case <-time.After(200 * time.Millisecond):
	}

	// The fix: the instant-DDL path drains the signal before finalCleanup.
	select {
	case <-ghostTableMigrated:
	case <-time.After(2 * time.Second):
		t.Fatal("GhostTableMigrated signal was never published")
	}

	// Draining releases the listener, so notifyListeners returns and frees the mutex,
	// which unblocks the decode path.
	select {
	case <-notifyReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("notifyListeners still blocked after drain: deadlock not resolved")
	}
	select {
	case decoded := <-decodeReturned:
		require.True(t, decoded, "registered table should be decoded")
	case <-time.After(2 * time.Second):
		t.Fatal("shouldDecodeRowsEvent still blocked after drain: mutex was not released")
	}
}

func TestEventsStreamer(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping events streamer test suite in short mode")
	}
	suite.Run(t, new(EventsStreamerTestSuite))
}
