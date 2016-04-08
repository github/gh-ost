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
	"github.com/github/gh-osc/go/mysql"
	"github.com/github/gh-osc/go/sql"

	"github.com/outbrain/golib/log"
	"github.com/outbrain/golib/sqlutils"
)

const (
	heartbeatIntervalSeconds = 1
)

// Applier reads data from the read-MySQL-server (typically a replica, but can be the master)
// It is used for gaining initial status and structure, and later also follow up on progress and changelog
type Applier struct {
	connectionConfig *mysql.ConnectionConfig
	db               *gosql.DB
	migrationContext *base.MigrationContext
}

func NewApplier() *Applier {
	return &Applier{
		connectionConfig: base.GetMigrationContext().MasterConnectionConfig,
		migrationContext: base.GetMigrationContext(),
	}
}

func (this *Applier) InitDBConnections() (err error) {
	ApplierUri := this.connectionConfig.GetDBUri(this.migrationContext.DatabaseName)
	if this.db, _, err = sqlutils.GetDB(ApplierUri); err != nil {
		return err
	}
	if err := this.validateConnection(); err != nil {
		return err
	}
	return nil
}

// validateConnection issues a simple can-connect to MySQL
func (this *Applier) validateConnection() error {
	query := `select @@global.port`
	var port int
	if err := this.db.QueryRow(query).Scan(&port); err != nil {
		return err
	}
	if port != this.connectionConfig.Key.Port {
		return fmt.Errorf("Unexpected database port reported: %+v", port)
	}
	log.Infof("connection validated on %+v", this.connectionConfig.Key)
	return nil
}

// CreateGhostTable creates the ghost table on the master
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

// CreateGhostTable creates the ghost table on the master
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

// CreateChangelogTable creates the changelog table on the master
func (this *Applier) CreateChangelogTable() error {
	query := fmt.Sprintf(`create /* gh-osc */ table %s.%s (
			id int auto_increment,
			last_update timestamp not null DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			hint varchar(64) charset ascii not null,
			value varchar(64) charset ascii not null,
			primary key(id),
			unique key hint_uidx(hint)
		) auto_increment=2
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

// DropChangelogTable drops the changelog table on the master
func (this *Applier) DropChangelogTable() error {
	query := fmt.Sprintf(`drop /* gh-osc */ table if exists %s.%s`,
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetChangelogTableName()),
	)
	log.Infof("Droppping changelog table %s.%s",
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetChangelogTableName()),
	)
	if _, err := sqlutils.ExecNoPrepare(this.db, query); err != nil {
		return err
	}
	log.Infof("Changelog table dropped")
	return nil
}

// WriteChangelog writes a value to the changelog table.
// It returns the hint as given, for convenience
func (this *Applier) WriteChangelog(hint, value string) (string, error) {
	query := fmt.Sprintf(`
			insert /* gh-osc */ into %s.%s
				(id, hint, value)
			values
				(NULL, ?, ?)
			on duplicate key update
				last_update=NOW(),
				value=VALUES(value)
		`,
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetChangelogTableName()),
	)
	_, err := sqlutils.Exec(this.db, query, hint, value)
	return hint, err
}

// InitiateHeartbeat creates a heartbeat cycle, writing to the changelog table.
// This is done asynchronously
func (this *Applier) InitiateHeartbeat() {
	go func() {
		numSuccessiveFailures := 0
		query := fmt.Sprintf(`
			insert /* gh-osc */ into %s.%s
				(id, hint, value)
			values
				(1, 'heartbeat', ?)
			on duplicate key update
				last_update=NOW(),
				value=VALUES(value)
		`,
			sql.EscapeName(this.migrationContext.DatabaseName),
			sql.EscapeName(this.migrationContext.GetChangelogTableName()),
		)
		injectHeartbeat := func() error {
			if _, err := sqlutils.ExecNoPrepare(this.db, query, time.Now().Format(time.RFC3339)); err != nil {
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

		heartbeatTick := time.Tick(time.Duration(heartbeatIntervalSeconds) * time.Second)
		for range heartbeatTick {
			// Generally speaking, we would issue a goroutine, but I'd actually rather
			// have this blocked rather than spam the master in the event something
			// goes wrong
			if err := injectHeartbeat(); err != nil {
				return
			}
		}
	}()
}

// ReadMigrationMinValues
func (this *Applier) ReadMigrationMinValues(uniqueKey *sql.UniqueKey) error {
	log.Debugf("Reading migration range according to key: %s", uniqueKey.Name)
	query, err := sql.BuildUniqueKeyMinValuesPreparedQuery(this.migrationContext.DatabaseName, this.migrationContext.OriginalTableName, uniqueKey.Columns)
	if err != nil {
		return err
	}
	rows, err := this.db.Query(query)
	if err != nil {
		return err
	}
	for rows.Next() {
		this.migrationContext.MigrationRangeMinValues = sql.NewColumnValues(len(uniqueKey.Columns))
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
	query, err := sql.BuildUniqueKeyMaxValuesPreparedQuery(this.migrationContext.DatabaseName, this.migrationContext.OriginalTableName, uniqueKey.Columns)
	if err != nil {
		return err
	}
	rows, err := this.db.Query(query)
	if err != nil {
		return err
	}
	for rows.Next() {
		this.migrationContext.MigrationRangeMaxValues = sql.NewColumnValues(len(uniqueKey.Columns))
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
	compareWithIterationRangeStart, explodedArgs, err := sql.BuildRangePreparedComparison(this.migrationContext.UniqueKey.Columns, this.migrationContext.MigrationIterationRangeMinValues.AbstractValues(), sql.GreaterThanOrEqualsComparisonSign)
	if err != nil {
		return false, err
	}
	args = append(args, explodedArgs...)
	compareWithRangeEnd, explodedArgs, err := sql.BuildRangePreparedComparison(this.migrationContext.UniqueKey.Columns, this.migrationContext.MigrationRangeMaxValues.AbstractValues(), sql.LessThanComparisonSign)
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
		this.migrationContext.UniqueKey.Columns,
		this.migrationContext.MigrationIterationRangeMinValues.AbstractValues(),
		this.migrationContext.MigrationRangeMaxValues.AbstractValues(),
		this.migrationContext.ChunkSize,
		fmt.Sprintf("iteration:%d", this.migrationContext.Iteration),
	)
	if err != nil {
		return hasFurtherRange, err
	}
	rows, err := this.db.Query(query, explodedArgs...)
	if err != nil {
		return hasFurtherRange, err
	}
	iterationRangeMaxValues := sql.NewColumnValues(len(this.migrationContext.UniqueKey.Columns))
	for rows.Next() {
		if err = rows.Scan(iterationRangeMaxValues.ValuesPointers...); err != nil {
			return hasFurtherRange, err
		}
		hasFurtherRange = true
	}
	if !hasFurtherRange {
		log.Debugf("Iteration complete: cannot find iteration end")
		return hasFurtherRange, nil
	}
	this.migrationContext.MigrationIterationRangeMaxValues = iterationRangeMaxValues
	log.Debugf(
		"column values: [%s]..[%s]; iteration: %d; chunk-size: %d",
		this.migrationContext.MigrationIterationRangeMinValues,
		this.migrationContext.MigrationIterationRangeMaxValues,
		this.migrationContext.Iteration,
		this.migrationContext.ChunkSize,
	)
	return hasFurtherRange, nil
}

func (this *Applier) ApplyIterationInsertQuery() (chunkSize int64, rowsAffected int64, duration time.Duration, err error) {
	startTime := time.Now()
	chunkSize = atomic.LoadInt64(&this.migrationContext.ChunkSize)

	query, explodedArgs, err := sql.BuildRangeInsertPreparedQuery(
		this.migrationContext.DatabaseName,
		this.migrationContext.OriginalTableName,
		this.migrationContext.GetGhostTableName(),
		this.migrationContext.UniqueKey.Columns,
		this.migrationContext.UniqueKey.Name,
		this.migrationContext.UniqueKey.Columns,
		this.migrationContext.MigrationIterationRangeMinValues.AbstractValues(),
		this.migrationContext.MigrationIterationRangeMaxValues.AbstractValues(),
		this.migrationContext.Iteration == 0,
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
	this.WriteChangelog(
		fmt.Sprintf("copy iteration %d", this.migrationContext.Iteration),
		fmt.Sprintf("chunk: %d; affected: %d; duration: %d", chunkSize, rowsAffected, duration),
	)
	log.Debugf(
		"Issued INSERT on range: [%s]..[%s]; iteration: %d; chunk-size: %d",
		this.migrationContext.MigrationIterationRangeMinValues,
		this.migrationContext.MigrationIterationRangeMaxValues,
		this.migrationContext.Iteration,
		chunkSize)
	return chunkSize, rowsAffected, duration, nil
}

// LockTables
func (this *Applier) LockTables() error {
	query := fmt.Sprintf(`lock /* gh-osc */ tables %s.%s write, %s.%s write, %s.%s write`,
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.OriginalTableName),
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetGhostTableName()),
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetChangelogTableName()),
	)
	log.Infof("Locking tables")
	if _, err := sqlutils.ExecNoPrepare(this.db, query); err != nil {
		return err
	}
	log.Infof("Tables locked")
	return nil
}

// UnlockTables
func (this *Applier) UnlockTables() error {
	query := `unlock /* gh-osc */ tables`
	log.Infof("Unlocking tables")
	if _, err := sqlutils.ExecNoPrepare(this.db, query); err != nil {
		return err
	}
	log.Infof("Tables unlocked")
	return nil
}
