/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-osc/blob/master/LICENSE
*/

package logic

import (
	gosql "database/sql"
	"fmt"
	"github.com/github/gh-osc/go/base"
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
	log.Infof("Table created")
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
	log.Infof("Table altered")
	return nil
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

// IterationIsComplete lets us know when the copy-iteration phase is complete, i.e.
// we've exhausted all rows
func (this *Applier) IterationIsComplete() (bool, error) {
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

func (this *Applier) CalculateNextIterationRangeEndValues() error {
	startingFromValues := this.migrationContext.MigrationRangeMinValues
	this.migrationContext.MigrationIterationRangeMinValues = this.migrationContext.MigrationIterationRangeMaxValues
	if this.migrationContext.MigrationIterationRangeMinValues != nil {
		startingFromValues = this.migrationContext.MigrationIterationRangeMinValues
	}
	query, explodedArgs, err := sql.BuildUniqueKeyRangeEndPreparedQuery(
		this.migrationContext.DatabaseName,
		this.migrationContext.OriginalTableName,
		this.migrationContext.UniqueKey.Columns,
		startingFromValues.AbstractValues(),
		this.migrationContext.MigrationRangeMaxValues.AbstractValues(),
		this.migrationContext.ChunkSize,
		fmt.Sprintf("iteration:%d", this.migrationContext.Iteration),
	)
	if err != nil {
		return err
	}
	rows, err := this.db.Query(query, explodedArgs...)
	if err != nil {
		return err
	}
	iterationRangeMaxValues := sql.NewColumnValues(len(this.migrationContext.UniqueKey.Columns))
	iterationRangeEndFound := false
	for rows.Next() {
		if err = rows.Scan(iterationRangeMaxValues.ValuesPointers...); err != nil {
			return err
		}
		iterationRangeEndFound = true
	}
	if !iterationRangeEndFound {
		log.Debugf("Iteration complete: cannot find iteration end")
		return nil
	}
	this.migrationContext.MigrationIterationRangeMaxValues = iterationRangeMaxValues
	log.Debugf("column values: %s; iteration: %d; chunk-size: %d", this.migrationContext.MigrationIterationRangeMaxValues, this.migrationContext.Iteration, this.migrationContext.ChunkSize)
	return nil
}

// IterateTable
func (this *Applier) IterateTable(uniqueKey *sql.UniqueKey) error {
	query, err := sql.BuildUniqueKeyMinValuesPreparedQuery(this.migrationContext.DatabaseName, this.migrationContext.OriginalTableName, uniqueKey.Columns)
	if err != nil {
		return err
	}
	columnValues := sql.NewColumnValues(len(uniqueKey.Columns))

	rows, err := this.db.Query(query)
	if err != nil {
		return err
	}
	for rows.Next() {
		if err = rows.Scan(columnValues.ValuesPointers...); err != nil {
			return err
		}
	}
	log.Debugf("column values: %s", columnValues)
	query = `insert into test.sample_data_dump (category, ts) values (?, ?)`
	if _, err := sqlutils.Exec(this.db, query, columnValues.AbstractValues()...); err != nil {
		return err
	}

	return nil
}
