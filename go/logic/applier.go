/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-osc/blob/master/LICENSE
*/

package logic

import (
	gosql "database/sql"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/github/gh-osc/go/base"
	"github.com/github/gh-osc/go/binlog"
	"github.com/github/gh-osc/go/mysql"
	"github.com/github/gh-osc/go/sql"

	"github.com/outbrain/golib/log"
	"github.com/outbrain/golib/sqlutils"
)

// Applier reads data from the read-MySQL-server (typically a replica, but can be the master)
// It is used for gaining initial status and structure, and later also follow up on progress and changelog
type Applier struct {
	connectionConfig *mysql.ConnectionConfig
	db               *gosql.DB
	singletonDB      *gosql.DB
	migrationContext *base.MigrationContext
}

func NewApplier() *Applier {
	return &Applier{
		connectionConfig: base.GetMigrationContext().ApplierConnectionConfig,
		migrationContext: base.GetMigrationContext(),
	}
}

func (this *Applier) InitDBConnections() (err error) {
	applierUri := this.connectionConfig.GetDBUri(this.migrationContext.DatabaseName)
	if this.db, _, err = sqlutils.GetDB(applierUri); err != nil {
		return err
	}
	singletonApplierUri := fmt.Sprintf("%s?timeout=0", applierUri)
	if this.singletonDB, _, err = sqlutils.GetDB(singletonApplierUri); err != nil {
		return err
	}
	this.singletonDB.SetMaxOpenConns(1)
	if err := this.validateConnection(this.db); err != nil {
		return err
	}
	if err := this.validateConnection(this.singletonDB); err != nil {
		return err
	}
	return nil
}

// validateConnection issues a simple can-connect to MySQL
func (this *Applier) validateConnection(db *gosql.DB) error {
	query := `select @@global.port`
	var port int
	if err := db.QueryRow(query).Scan(&port); err != nil {
		return err
	}
	if port != this.connectionConfig.Key.Port {
		return fmt.Errorf("Unexpected database port reported: %+v", port)
	}
	log.Infof("connection validated on %+v", this.connectionConfig.Key)
	return nil
}

// CreateGhostTable creates the ghost table on the applier host
func (this *Applier) CreateGhostTable() error {
	query := fmt.Sprintf(`create /* gh-osc */ table %s.%s like %s.%s`,
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetGhostTableName()),
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.OriginalTableName),
	)
	log.Infof("Creating ghost table %s.%s",
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetGhostTableName()),
	)
	if _, err := sqlutils.ExecNoPrepare(this.db, query); err != nil {
		return err
	}
	log.Infof("Ghost table created")
	return nil
}

// AlterGhost applies `alter` statement on ghost table
func (this *Applier) AlterGhost() error {
	query := fmt.Sprintf(`alter /* gh-osc */ table %s.%s %s`,
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetGhostTableName()),
		this.migrationContext.AlterStatement,
	)
	log.Infof("Altering ghost table %s.%s",
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetGhostTableName()),
	)
	log.Debugf("ALTER statement: %s", query)
	if _, err := sqlutils.ExecNoPrepare(this.db, query); err != nil {
		return err
	}
	log.Infof("Ghost table altered")
	return nil
}

// CreateChangelogTable creates the changelog table on the applier host
func (this *Applier) CreateChangelogTable() error {
	query := fmt.Sprintf(`create /* gh-osc */ table %s.%s (
			id bigint auto_increment,
			last_update timestamp not null DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			hint varchar(64) charset ascii not null,
			value varchar(255) charset ascii not null,
			primary key(id),
			unique key hint_uidx(hint)
		) auto_increment=256
		`,
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetChangelogTableName()),
	)
	log.Infof("Creating changelog table %s.%s",
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetChangelogTableName()),
	)
	if _, err := sqlutils.ExecNoPrepare(this.db, query); err != nil {
		return err
	}
	log.Infof("Changelog table created")
	return nil
}

// dropTable drops a given table on the applied host
func (this *Applier) dropTable(tableName string) error {
	query := fmt.Sprintf(`drop /* gh-osc */ table if exists %s.%s`,
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(tableName),
	)
	log.Infof("Droppping table %s.%s",
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(tableName),
	)
	if _, err := sqlutils.ExecNoPrepare(this.db, query); err != nil {
		return err
	}
	log.Infof("Table dropped")
	return nil
}

// DropChangelogTable drops the changelog table on the applier host
func (this *Applier) DropChangelogTable() error {
	return this.dropTable(this.migrationContext.GetChangelogTableName())
}

// DropGhostTable drops the ghost table on the applier host
func (this *Applier) DropGhostTable() error {
	return this.dropTable(this.migrationContext.GetGhostTableName())
}

// WriteChangelog writes a value to the changelog table.
// It returns the hint as given, for convenience
func (this *Applier) WriteChangelog(hint, value string) (string, error) {
	explicitId := 0
	switch hint {
	case "heartbeat":
		explicitId = 1
	case "state":
		explicitId = 2
	case "throttle":
		explicitId = 3
	}
	query := fmt.Sprintf(`
			insert /* gh-osc */ into %s.%s
				(id, hint, value)
			values
				(NULLIF(?, 0), ?, ?)
			on duplicate key update
				last_update=NOW(),
				value=VALUES(value)
		`,
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetChangelogTableName()),
	)
	_, err := sqlutils.Exec(this.db, query, explicitId, hint, value)
	return hint, err
}

func (this *Applier) WriteAndLogChangelog(hint, value string) (string, error) {
	this.WriteChangelog(hint, value)
	return this.WriteChangelog(fmt.Sprintf("%s at %d", hint, time.Now().UnixNano()), value)
}

func (this *Applier) WriteChangelogState(value string) (string, error) {
	return this.WriteAndLogChangelog("state", value)
}

// InitiateHeartbeat creates a heartbeat cycle, writing to the changelog table.
// This is done asynchronously
func (this *Applier) InitiateHeartbeat(heartbeatIntervalMilliseconds int64) {
	numSuccessiveFailures := 0
	injectHeartbeat := func() error {
		if _, err := this.WriteChangelog("heartbeat", time.Now().Format(time.RFC3339Nano)); err != nil {
			numSuccessiveFailures++
			if numSuccessiveFailures > this.migrationContext.MaxRetries() {
				return log.Errore(err)
			}
		} else {
			numSuccessiveFailures = 0
		}
		return nil
	}
	injectHeartbeat()

	heartbeatTick := time.Tick(time.Duration(heartbeatIntervalMilliseconds) * time.Millisecond)
	for range heartbeatTick {
		// Generally speaking, we would issue a goroutine, but I'd actually rather
		// have this blocked rather than spam the master in the event something
		// goes wrong
		if err := injectHeartbeat(); err != nil {
			return
		}
	}
}

// ReadMigrationMinValues
func (this *Applier) ReadMigrationMinValues(uniqueKey *sql.UniqueKey) error {
	log.Debugf("Reading migration range according to key: %s", uniqueKey.Name)
	query, err := sql.BuildUniqueKeyMinValuesPreparedQuery(this.migrationContext.DatabaseName, this.migrationContext.OriginalTableName, uniqueKey.Columns.Names)
	if err != nil {
		return err
	}
	rows, err := this.db.Query(query)
	if err != nil {
		return err
	}
	for rows.Next() {
		this.migrationContext.MigrationRangeMinValues = sql.NewColumnValues(uniqueKey.Len())
		if err = rows.Scan(this.migrationContext.MigrationRangeMinValues.ValuesPointers...); err != nil {
			return err
		}
	}
	log.Infof("Migration min values: [%s]", this.migrationContext.MigrationRangeMinValues)
	return err
}

// ReadMigrationMinValues
func (this *Applier) ReadMigrationMaxValues(uniqueKey *sql.UniqueKey) error {
	log.Debugf("Reading migration range according to key: %s", uniqueKey.Name)
	query, err := sql.BuildUniqueKeyMaxValuesPreparedQuery(this.migrationContext.DatabaseName, this.migrationContext.OriginalTableName, uniqueKey.Columns.Names)
	if err != nil {
		return err
	}
	rows, err := this.db.Query(query)
	if err != nil {
		return err
	}
	for rows.Next() {
		this.migrationContext.MigrationRangeMaxValues = sql.NewColumnValues(uniqueKey.Len())
		if err = rows.Scan(this.migrationContext.MigrationRangeMaxValues.ValuesPointers...); err != nil {
			return err
		}
	}
	log.Infof("Migration max values: [%s]", this.migrationContext.MigrationRangeMaxValues)
	return err
}

func (this *Applier) ReadMigrationRangeValues() error {
	if err := this.ReadMigrationMinValues(this.migrationContext.UniqueKey); err != nil {
		return err
	}
	if err := this.ReadMigrationMaxValues(this.migrationContext.UniqueKey); err != nil {
		return err
	}
	return nil
}

// __unused_IterationIsComplete lets us know when the copy-iteration phase is complete, i.e.
// we've exhausted all rows
func (this *Applier) __unused_IterationIsComplete() (bool, error) {
	if !this.migrationContext.HasMigrationRange() {
		return false, nil
	}
	if this.migrationContext.MigrationIterationRangeMinValues == nil {
		return false, nil
	}
	args := sqlutils.Args()
	compareWithIterationRangeStart, explodedArgs, err := sql.BuildRangePreparedComparison(this.migrationContext.UniqueKey.Columns.Names, this.migrationContext.MigrationIterationRangeMinValues.AbstractValues(), sql.GreaterThanOrEqualsComparisonSign)
	if err != nil {
		return false, err
	}
	args = append(args, explodedArgs...)
	compareWithRangeEnd, explodedArgs, err := sql.BuildRangePreparedComparison(this.migrationContext.UniqueKey.Columns.Names, this.migrationContext.MigrationRangeMaxValues.AbstractValues(), sql.LessThanComparisonSign)
	if err != nil {
		return false, err
	}
	args = append(args, explodedArgs...)
	query := fmt.Sprintf(`
			select /* gh-osc IterationIsComplete */ 1
				from %s.%s
				where (%s) and (%s)
				limit 1
				`,
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.OriginalTableName),
		compareWithIterationRangeStart,
		compareWithRangeEnd,
	)

	moreRowsFound := false
	err = sqlutils.QueryRowsMap(this.db, query, func(rowMap sqlutils.RowMap) error {
		moreRowsFound = true
		return nil
	}, args...)
	if err != nil {
		return false, err
	}
	return !moreRowsFound, nil
}

// CalculateNextIterationRangeEndValues reads the next-iteration-range-end unique key values,
// which will be used for copying the next chunk of rows. Ir returns "false" if there is
// no further chunk to work through, i.e. we're past the last chunk and are done with
// itrating the range (and this done with copying row chunks)
func (this *Applier) CalculateNextIterationRangeEndValues() (hasFurtherRange bool, err error) {
	this.migrationContext.MigrationIterationRangeMinValues = this.migrationContext.MigrationIterationRangeMaxValues
	if this.migrationContext.MigrationIterationRangeMinValues == nil {
		this.migrationContext.MigrationIterationRangeMinValues = this.migrationContext.MigrationRangeMinValues
	}
	query, explodedArgs, err := sql.BuildUniqueKeyRangeEndPreparedQuery(
		this.migrationContext.DatabaseName,
		this.migrationContext.OriginalTableName,
		this.migrationContext.UniqueKey.Columns.Names,
		this.migrationContext.MigrationIterationRangeMinValues.AbstractValues(),
		this.migrationContext.MigrationRangeMaxValues.AbstractValues(),
		this.migrationContext.ChunkSize,
		fmt.Sprintf("iteration:%d", this.migrationContext.GetIteration()),
	)
	if err != nil {
		return hasFurtherRange, err
	}
	rows, err := this.db.Query(query, explodedArgs...)
	if err != nil {
		return hasFurtherRange, err
	}
	iterationRangeMaxValues := sql.NewColumnValues(this.migrationContext.UniqueKey.Len())
	for rows.Next() {
		if err = rows.Scan(iterationRangeMaxValues.ValuesPointers...); err != nil {
			return hasFurtherRange, err
		}
		hasFurtherRange = true
	}
	if !hasFurtherRange {
		log.Debugf("Iteration complete: no further range to iterate")
		return hasFurtherRange, nil
	}
	this.migrationContext.MigrationIterationRangeMaxValues = iterationRangeMaxValues
	return hasFurtherRange, nil
}

func (this *Applier) ApplyIterationInsertQuery() (chunkSize int64, rowsAffected int64, duration time.Duration, err error) {
	startTime := time.Now()
	chunkSize = atomic.LoadInt64(&this.migrationContext.ChunkSize)

	query, explodedArgs, err := sql.BuildRangeInsertPreparedQuery(
		this.migrationContext.DatabaseName,
		this.migrationContext.OriginalTableName,
		this.migrationContext.GetGhostTableName(),
		this.migrationContext.SharedColumns.Names,
		this.migrationContext.UniqueKey.Name,
		this.migrationContext.UniqueKey.Columns.Names,
		this.migrationContext.MigrationIterationRangeMinValues.AbstractValues(),
		this.migrationContext.MigrationIterationRangeMaxValues.AbstractValues(),
		this.migrationContext.GetIteration() == 0,
		this.migrationContext.IsTransactionalTable(),
	)
	if err != nil {
		return chunkSize, rowsAffected, duration, err
	}
	sqlResult, err := sqlutils.Exec(this.db, query, explodedArgs...)
	if err != nil {
		return chunkSize, rowsAffected, duration, err
	}
	rowsAffected, _ = sqlResult.RowsAffected()
	duration = time.Now().Sub(startTime)
	log.Debugf(
		"Issued INSERT on range: [%s]..[%s]; iteration: %d; chunk-size: %d",
		this.migrationContext.MigrationIterationRangeMinValues,
		this.migrationContext.MigrationIterationRangeMaxValues,
		this.migrationContext.GetIteration(),
		chunkSize)
	return chunkSize, rowsAffected, duration, nil
}

// LockTables
func (this *Applier) LockTables() error {
	// query := fmt.Sprintf(`lock /* gh-osc */ tables %s.%s write, %s.%s write, %s.%s write`,
	// 	sql.EscapeName(this.migrationContext.DatabaseName),
	// 	sql.EscapeName(this.migrationContext.OriginalTableName),
	// 	sql.EscapeName(this.migrationContext.DatabaseName),
	// 	sql.EscapeName(this.migrationContext.GetGhostTableName()),
	// 	sql.EscapeName(this.migrationContext.DatabaseName),
	// 	sql.EscapeName(this.migrationContext.GetChangelogTableName()),
	// )
	query := fmt.Sprintf(`lock /* gh-osc */ tables %s.%s write`,
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.OriginalTableName),
	)
	log.Infof("Locking tables")
	this.migrationContext.LockTablesStartTime = time.Now()
	if _, err := sqlutils.ExecNoPrepare(this.singletonDB, query); err != nil {
		return err
	}
	log.Infof("Tables locked")
	return nil
}

// UnlockTables
func (this *Applier) UnlockTables() error {
	query := `unlock /* gh-osc */ tables`
	log.Infof("Unlocking tables")
	if _, err := sqlutils.ExecNoPrepare(this.singletonDB, query); err != nil {
		return err
	}
	log.Infof("Tables unlocked")
	return nil
}

// LockTables
func (this *Applier) SwapTables() error {
	// query := fmt.Sprintf(`rename /* gh-osc */ table %s.%s to %s.%s, %s.%s to %s.%s`,
	// 	sql.EscapeName(this.migrationContext.DatabaseName),
	// 	sql.EscapeName(this.migrationContext.OriginalTableName),
	// 	sql.EscapeName(this.migrationContext.DatabaseName),
	// 	sql.EscapeName(this.migrationContext.GetOldTableName()),
	// 	sql.EscapeName(this.migrationContext.DatabaseName),
	// 	sql.EscapeName(this.migrationContext.GetGhostTableName()),
	// 	sql.EscapeName(this.migrationContext.DatabaseName),
	// 	sql.EscapeName(this.migrationContext.OriginalTableName),
	// )
	// log.Infof("Renaming tables")
	// if _, err := sqlutils.ExecNoPrepare(this.singletonDB, query); err != nil {
	// 	return err
	// }
	query := fmt.Sprintf(`alter /* gh-osc */ table %s.%s rename %s`,
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.OriginalTableName),
		sql.EscapeName(this.migrationContext.GetOldTableName()),
	)
	log.Infof("Renaming original table")
	this.migrationContext.RenameTablesStartTime = time.Now()
	if _, err := sqlutils.ExecNoPrepare(this.singletonDB, query); err != nil {
		return err
	}
	query = fmt.Sprintf(`alter /* gh-osc */ table %s.%s rename %s`,
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetGhostTableName()),
		sql.EscapeName(this.migrationContext.OriginalTableName),
	)
	log.Infof("Renaming ghost table")
	if _, err := sqlutils.ExecNoPrepare(this.db, query); err != nil {
		return err
	}
	this.migrationContext.RenameTablesEndTime = time.Now()

	log.Infof("Tables renamed")
	return nil
}

// StopSlaveIOThread is applicable with --test-on-replica; it stops the IO thread
func (this *Applier) StopSlaveIOThread() error {
	query := `stop /* gh-osc */ slave io_thread`
	log.Infof("Stopping replication")
	if _, err := sqlutils.ExecNoPrepare(this.db, query); err != nil {
		return err
	}
	log.Infof("Replication stopped")
	return nil
}

func (this *Applier) ShowStatusVariable(variableName string) (result int64, err error) {
	query := fmt.Sprintf(`show global status like '%s'`, variableName)
	if err := this.db.QueryRow(query).Scan(&variableName, &result); err != nil {
		return 0, err
	}
	return result, nil
}

func (this *Applier) buildDMLEventQuery(dmlEvent *binlog.BinlogDMLEvent) (query string, args []interface{}, err error) {
	switch dmlEvent.DML {
	case binlog.DeleteDML:
		{
			query, uniqueKeyArgs, err := sql.BuildDMLDeleteQuery(dmlEvent.DatabaseName, this.migrationContext.GetGhostTableName(), this.migrationContext.OriginalTableColumns, &this.migrationContext.UniqueKey.Columns, dmlEvent.WhereColumnValues.AbstractValues())
			return query, uniqueKeyArgs, err
		}
	case binlog.InsertDML:
		{
			query, sharedArgs, err := sql.BuildDMLInsertQuery(dmlEvent.DatabaseName, this.migrationContext.GetGhostTableName(), this.migrationContext.OriginalTableColumns, this.migrationContext.SharedColumns, dmlEvent.NewColumnValues.AbstractValues())
			return query, sharedArgs, err
		}
	case binlog.UpdateDML:
		{
			query, sharedArgs, uniqueKeyArgs, err := sql.BuildDMLUpdateQuery(dmlEvent.DatabaseName, this.migrationContext.GetGhostTableName(), this.migrationContext.OriginalTableColumns, this.migrationContext.SharedColumns, &this.migrationContext.UniqueKey.Columns, dmlEvent.NewColumnValues.AbstractValues(), dmlEvent.WhereColumnValues.AbstractValues())
			args = append(args, sharedArgs...)
			args = append(args, uniqueKeyArgs...)
			return query, args, err
		}
	}
	return "", args, fmt.Errorf("Unknown dml event type: %+v", dmlEvent.DML)
}

func (this *Applier) ApplyDMLEventQuery(dmlEvent *binlog.BinlogDMLEvent) error {
	query, args, err := this.buildDMLEventQuery(dmlEvent)
	if err != nil {
		return err
	}
	_, err = sqlutils.Exec(this.db, query, args...)
	if err == nil {
		atomic.AddInt64(&this.migrationContext.TotalDMLEventsApplied, 1)
	}
	return err
}
