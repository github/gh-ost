package logic

import (
	"context"
	gosql "database/sql"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/binlog"
	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	testmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
)

// queueCheckpointTestEvents uses the production reader, dispatcher and listeners,
// but leaves consumption of the apply queue under the test's control.
//
//nolint:contextcheck // Streamer APIs use MigrationContext; the test deadline closes the reader.
func queueCheckpointTestEvents(t *testing.T, ctx context.Context, migrationContext *base.MigrationContext, migrator *Migrator, start, end mysql.BinlogCoordinates) {
	t.Helper()
	reader := binlog.NewGoMySQLReader(migrationContext, func(database, table string) bool {
		return database == testMysqlDatabase && table == testMysqlTableName
	})
	defer reader.Close()
	require.NoError(t, reader.ConnectBinlogStreamer(start))
	streamer := NewEventsStreamer(migrationContext)
	streamer.binlogReader = reader
	migrator.eventsStreamer = streamer
	require.NoError(t, migrator.addDMLEventsListener())

	delivered := make(chan struct{}, 1)
	streamer.AddTransactionCompleteListener(func(entry *binlog.BinlogEntry) error {
		if entry.Coordinates.Equals(end) {
			delivered <- struct{}{}
		}
		return nil
	})
	done := make(chan error, 1)
	go func() {
		done <- streamer.StreamEvents(func() bool {
			return reader.LastTrxCoords != nil && reader.LastTrxCoords.Equals(end)
		})
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-ctx.Done():
		reader.Close()
		<-done
		t.Fatal("timed out reading transaction")
	}
	// The reader has stopped sending. Let the dispatcher drain and exit.
	close(streamer.eventsChannel)
	select {
	case <-delivered:
	case <-ctx.Done():
		t.Fatal("timed out dispatching transaction completion")
	}
}

//nolint:contextcheck // Migrator APIs use MigrationContext's internal context; SQL/checkpoint calls use the test deadline.
func TestBinlogCheckpointWaitsForTransactionIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("requires MySQL 8.0")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	container, err := testmysql.Run(ctx, testMysqlContainerImage,
		testmysql.WithDatabase(testMysqlDatabase),
		testmysql.WithUsername(testMysqlUser),
		testmysql.WithPassword(testMysqlPass),
		testcontainers.WithCmdArgs("--server-id=1", "--gtid-mode=ON", "--enforce-gtid-consistency=ON"),
	)
	require.NoError(t, err)
	defer testcontainers.TerminateContainer(container)
	dsn, err := container.ConnectionString(ctx)
	require.NoError(t, err)
	db, err := gosql.Open("mysql", dsn)
	require.NoError(t, err)
	defer db.Close()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	for _, query := range []string{
		"SET SESSION time_zone='+00:00'",
		"CREATE TABLE testing (id INT PRIMARY KEY, amount DECIMAL(5,2), created_at TIMESTAMP, padding TEXT)",
	} {
		_, err := conn.ExecContext(ctx, query)
		require.NoError(t, err)
	}
	status := func() (*mysql.FileBinlogCoordinates, *mysql.GTIDBinlogCoordinates) {
		var file, doDB, ignoreDB, gtid string
		var pos int64
		require.NoError(t, conn.QueryRowContext(ctx, "SHOW MASTER STATUS").Scan(&file, &pos, &doDB, &ignoreDB, &gtid))
		coords, err := mysql.NewGTIDBinlogCoordinates(mysql.MySQLFlavor, gtid)
		require.NoError(t, err)
		return mysql.NewFileBinlogCoordinates(file, pos), coords
	}
	type transaction struct {
		name               string
		startFile, endFile *mysql.FileBinlogCoordinates
		startGTID, endGTID *mysql.GTIDBinlogCoordinates
	}
	var transactions []transaction
	for _, compression := range []string{"OFF"} {
		_, err := conn.ExecContext(ctx, "SET SESSION binlog_transaction_compression="+compression)
		require.NoError(t, err)
		startFile, startGTID := status()
		tx, err := conn.BeginTx(ctx, nil)
		require.NoError(t, err)
		for _, query := range []string{
			"INSERT INTO testing VALUES (1, 123.45, '2023-11-14 22:13:20', REPEAT('x', 10000))",
			"UPDATE testing SET amount=456.78 WHERE id=1",
			"DELETE FROM testing WHERE id=1",
		} {
			_, err := tx.ExecContext(ctx, query)
			require.NoError(t, err)
		}
		require.NoError(t, tx.Commit())
		endFile, endGTID := status()
		transactions = append(transactions, transaction{compression, startFile, endFile, startGTID, endGTID})
	}
	config, err := getTestConnectionConfig(ctx, container)
	require.NoError(t, err)

	for _, transaction := range transactions {
		for _, mode := range []struct {
			useGTIDs bool
			cancelAt string
		}{
			{false, "none"}, {true, "none"},
			{false, "apply"}, {true, "apply"},
			{false, "insert"}, {true, "insert"},
		} {
			useGTIDs := mode.useGTIDs
			t.Run(fmt.Sprintf("compression=%s/GTID=%t/cancel=%s", transaction.name, useGTIDs, mode.cancelAt), func(t *testing.T) {
				migrationContext := newTestMigrationContext()
				defer migrationContext.CancelContext()
				migrationContext.Checkpoint = true
				migrationContext.UseGTIDs = useGTIDs
				// Replaying an already-applied INSERT uses INSERT IGNORE. Duplicate
				// warnings are expected on resume; use the default production policy.
				migrationContext.PanicOnWarnings = false
				migrationContext.ApplierConnectionConfig = config
				migrationContext.InspectorConnectionConfig = config
				require.NoError(t, migrationContext.SetConnectionConfig("innodb"))
				migrationContext.SetDMLBatchSize(1)
				columns := sql.NewColumnList([]string{"id", "amount", "created_at", "padding"})
				migrationContext.OriginalTableColumns = columns
				migrationContext.SharedColumns = columns
				migrationContext.MappedSharedColumns = columns
				uniqueKeyColumns := sql.NewColumnList([]string{"id"})
				uniqueKeyColumns.GetColumn("id").MySQLType = "int"
				migrationContext.UniqueKey = &sql.UniqueKey{Name: "PRIMARY", NameInGhostTable: "PRIMARY", Columns: *uniqueKeyColumns}
				_, err := conn.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s LIKE %s", getTestGhostTableName(), getTestTableName()))
				require.NoError(t, err)
				defer func() {
					_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", getTestGhostTableName()))
					require.NoError(t, err)
				}()

				migrator := NewMigrator(migrationContext, "test")
				migrator.applier = NewApplier(migrationContext)
				require.NoError(t, migrator.applier.InitDBConnections())
				defer migrator.applier.Teardown()
				migrator.applier.LastIterationRangeMinValues = sql.ToColumnValues([]interface{}{int64(1)})
				migrator.applier.LastIterationRangeMaxValues = sql.ToColumnValues([]interface{}{int64(1)})
				require.NoError(t, migrator.applier.prepareQueries())
				require.NoError(t, migrator.applier.CreateCheckpointTable())
				defer func() { require.NoError(t, migrator.applier.DropCheckpointTable()) }()
				start, end := transaction.startFile.Clone(), transaction.endFile.Clone()
				if useGTIDs {
					start, end = transaction.startGTID.Clone(), transaction.endGTID.Clone()
				}
				seed := &Checkpoint{
					LastTrxCoords:     start.Clone(),
					IterationRangeMin: migrator.applier.LastIterationRangeMinValues.Clone(),
					IterationRangeMax: migrator.applier.LastIterationRangeMaxValues.Clone(),
				}
				seedID, err := migrator.applier.WriteCheckpoint(ctx, seed)
				require.NoError(t, err)
				migrator.applier.AppliedTransactionCoordinates = start.Clone()
				queueCheckpointTestEvents(t, ctx, migrationContext, migrator, start, end)
				require.Len(t, migrator.applyEventsQueue, 4)
				first := <-migrator.applyEventsQueue
				require.NoError(t, migrator.onApplyEventStruct(first))
				require.Len(t, migrator.applyEventsQueue, 3, "two DMLs and the completion marker remain pending")

				assertCheckpointBlocked := func() {
					checkpointCtx, cancelCheckpoint := context.WithTimeout(ctx, 100*time.Millisecond)
					defer cancelCheckpoint()
					checkpoint, err := migrator.Checkpoint(checkpointCtx)
					require.ErrorIs(t, err, context.DeadlineExceeded, "premature checkpoint: %+v", checkpoint)
					stored, err := migrator.applier.ReadLastCheckpoint()
					require.NoError(t, err)
					require.Equal(t, seedID, stored.Id, "failed checkpoint must not replace the previous durable checkpoint")
					require.True(t, stored.LastTrxCoords.Equals(start))
				}
				assertCheckpointBlocked()
				switch mode.cancelAt {
				case "apply":
					assertCanceledTransactionCheckpoint(t, ctx, migrationContext, migrator, seedID)
					return
				case "insert":
					for len(migrator.applyEventsQueue) > 0 {
						require.NoError(t, migrator.onApplyEventStruct(<-migrator.applyEventsQueue))
					}
					require.True(t, migrator.applier.AppliedTransactionCoordinates.Equals(end))
					assertCanceledCheckpointInsert(t, ctx, migrationContext, migrator, seedID)
					return
				}

				// Simulate a crash after the first applied row: discard volatile
				// queue contents, then restart from the last persisted checkpoint.
				for len(migrator.applyEventsQueue) > 0 {
					<-migrator.applyEventsQueue
				}
				stored, err := migrator.applier.ReadLastCheckpoint()
				require.NoError(t, err)
				queueCheckpointTestEvents(t, ctx, migrationContext, migrator, stored.LastTrxCoords, end)
				require.Len(t, migrator.applyEventsQueue, 4)
				var transactionRows []*applyEventStruct
				for len(migrator.applyEventsQueue) > 1 {
					row := <-migrator.applyEventsQueue
					transactionRows = append(transactionRows, row)
					require.NoError(t, migrator.onApplyEventStruct(row))
				}
				// Even the final row cannot independently acknowledge its commit.
				require.True(t, migrator.applier.AppliedTransactionCoordinates.Equals(start))
				if !useGTIDs && transaction.name == "OFF" {
					// Sample a real row-event position P between the previous commit A
					// and this transaction's XID B. A must block; B must be persisted
					// rather than P once its marker is acknowledged.
					sampled := first.coords.Clone()
					require.True(t, start.SmallerThan(sampled))
					require.True(t, sampled.SmallerThan(end))
					reader := binlog.NewGoMySQLReader(migrationContext, nil)
					defer reader.Close()
					require.NoError(t, reader.ConnectBinlogStreamer(sampled))
					migrator.eventsStreamer.binlogReader = reader
				}
				assertCheckpointBlocked()
				marker := <-migrator.applyEventsQueue
				require.True(t, marker.transactionComplete)
				require.NoError(t, migrator.onApplyEventStruct(marker))
				checkpoint, err := migrator.Checkpoint(ctx)
				require.NoError(t, err)
				require.True(t, checkpoint.LastTrxCoords.Equals(end))
				stored, err = migrator.applier.ReadLastCheckpoint()
				require.NoError(t, err)
				require.True(t, stored.LastTrxCoords.Equals(end))
				var remaining int
				require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", getTestGhostTableName())).Scan(&remaining))
				require.Zero(t, remaining, "resuming after the partial apply must not skip the DELETE")

				// Resume again from the completed checkpoint and apply later changes.
				_, err = conn.ExecContext(ctx, "INSERT INTO testing VALUES (100, 12.34, '2023-11-14 22:13:20', 'after checkpoint') ON DUPLICATE KEY UPDATE amount=amount+1")
				require.NoError(t, err)
				resumeFile, resumeGTID := status()
				resumeEnd := mysql.BinlogCoordinates(resumeFile)
				if useGTIDs {
					resumeEnd = resumeGTID
				}
				queueCheckpointTestEvents(t, ctx, migrationContext, migrator, stored.LastTrxCoords, resumeEnd)
				for len(migrator.applyEventsQueue) > 0 {
					require.NoError(t, migrator.onApplyEventStruct(<-migrator.applyEventsQueue))
				}
				require.True(t, migrator.applier.AppliedTransactionCoordinates.Equals(resumeEnd))
				var sourceRows, ghostRows string
				query := "SELECT GROUP_CONCAT(CONCAT(id, ':', amount) ORDER BY id) FROM %s"
				require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf(query, getTestTableName())).Scan(&sourceRows))
				require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf(query, getTestGhostTableName())).Scan(&ghostRows))
				require.Equal(t, sourceRows, ghostRows)

				// Batch across two source-transaction markers, ending inside the
				// second transaction. Only the first may be acknowledged yet.
				migrationContext.SetDMLBatchSize(4)
				migrator.applier.AppliedTransactionCoordinates = start.Clone()
				nextMarker := &applyEventStruct{coords: resumeEnd.Clone(), transactionComplete: true}
				for _, event := range []*applyEventStruct{transactionRows[1], transactionRows[2], marker, transactionRows[0], transactionRows[1], transactionRows[2], nextMarker} {
					migrator.applyEventsQueue <- event
				}
				appliedBefore := atomic.LoadInt64(&migrationContext.TotalDMLEventsApplied)
				require.NoError(t, migrator.onApplyEventStruct(transactionRows[0]))
				require.Equal(t, appliedBefore+4, atomic.LoadInt64(&migrationContext.TotalDMLEventsApplied))
				require.Len(t, migrator.applyEventsQueue, 3)
				require.True(t, migrator.applier.AppliedTransactionCoordinates.Equals(end))
				require.NoError(t, migrator.onApplyEventStruct(<-migrator.applyEventsQueue))
				require.Empty(t, migrator.applyEventsQueue)
				require.True(t, migrator.applier.AppliedTransactionCoordinates.Equals(resumeEnd))

				// A marker collected while assembling a batch cannot acknowledge
				// anything if the destination SQL transaction fails.
				_, err = conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE %s", getTestGhostTableName()))
				require.NoError(t, err)
				migrationContext.SetDefaultNumRetries(1)
				migrationContext.PanicAbort = make(chan error, 1)
				migrator.applier.AppliedTransactionCoordinates = end.Clone()
				migrator.applyEventsQueue <- nextMarker
				migrator.applyEventsQueue <- transactionRows[1]
				require.Error(t, migrator.onApplyEventStruct(first))
				require.True(t, migrator.applier.AppliedTransactionCoordinates.Equals(end))
			})
		}
	}
}
