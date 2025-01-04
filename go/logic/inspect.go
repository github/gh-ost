/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"context"
	gosql "database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"

	"github.com/openark/golib/sqlutils"
)

const startReplicationPostWait = 250 * time.Millisecond
const startReplicationMaxWait = 2 * time.Second

// Inspector reads data from the read-MySQL-server (typically a replica, but can be the master)
// It is used for gaining initial status and structure, and later also follow up on progress and changelog
type Inspector struct {
	connectionConfig    *mysql.ConnectionConfig
	db                  *gosql.DB
	informationSchemaDb *gosql.DB
	migrationContext    *base.MigrationContext
	name                string
}

func NewInspector(migrationContext *base.MigrationContext) *Inspector {
	return &Inspector{
		connectionConfig: migrationContext.InspectorConnectionConfig,
		migrationContext: migrationContext,
		name:             "inspector",
	}
}

func (this *Inspector) InitDBConnections() (err error) {
	inspectorUri := this.connectionConfig.GetDBUri(this.migrationContext.DatabaseName)
	if this.db, _, err = mysql.GetDB(this.migrationContext.Uuid, inspectorUri); err != nil {
		return err
	}

	informationSchemaUri := this.connectionConfig.GetDBUri("information_schema")
	if this.informationSchemaDb, _, err = mysql.GetDB(this.migrationContext.Uuid, informationSchemaUri); err != nil {
		return err
	}

	if err := this.validateConnection(); err != nil {
		return err
	}
	if !this.migrationContext.AliyunRDS && !this.migrationContext.GoogleCloudPlatform && !this.migrationContext.AzureMySQL {
		if impliedKey, err := mysql.GetInstanceKey(this.db); err != nil {
			return err
		} else {
			this.connectionConfig.ImpliedKey = impliedKey
		}
	}
	if err := this.validateGrants(); err != nil {
		return err
	}
	if err := this.validateBinlogs(); err != nil {
		return err
	}
	if err := this.applyBinlogFormat(); err != nil {
		return err
	}
	this.migrationContext.Log.Infof("Inspector initiated on %+v, version %+v", this.connectionConfig.ImpliedKey, this.migrationContext.InspectorMySQLVersion)
	return nil
}

func (this *Inspector) ValidateOriginalTable() (err error) {
	if err := this.validateTable(); err != nil {
		return err
	}
	if err := this.validateTableForeignKeys(this.migrationContext.DiscardForeignKeys); err != nil {
		return err
	}
	if err := this.validateTableTriggers(); err != nil {
		return err
	}
	if err := this.estimateTableRowsViaExplain(); err != nil {
		return err
	}
	return nil
}

func (this *Inspector) InspectTableColumnsAndUniqueKeys(tableName string) (columns *sql.ColumnList, virtualColumns *sql.ColumnList, uniqueKeys [](*sql.UniqueKey), err error) {
	uniqueKeys, err = this.getCandidateUniqueKeys(tableName)
	if err != nil {
		return columns, virtualColumns, uniqueKeys, err
	}
	if len(uniqueKeys) == 0 {
		return columns, virtualColumns, uniqueKeys, fmt.Errorf("No PRIMARY nor UNIQUE key found in table! Bailing out")
	}
	columns, virtualColumns, err = mysql.GetTableColumns(this.db, this.migrationContext.DatabaseName, tableName)
	if err != nil {
		return columns, virtualColumns, uniqueKeys, err
	}

	return columns, virtualColumns, uniqueKeys, nil
}

func (this *Inspector) InspectOriginalTable() (err error) {
	this.migrationContext.OriginalTableColumns, this.migrationContext.OriginalTableVirtualColumns, this.migrationContext.OriginalTableUniqueKeys, err = this.InspectTableColumnsAndUniqueKeys(this.migrationContext.OriginalTableName)
	if err != nil {
		return err
	}
	this.migrationContext.OriginalTableAutoIncrement, err = this.getAutoIncrementValue(this.migrationContext.OriginalTableName)
	if err != nil {
		return err
	}
	return nil
}

// inspectOriginalAndGhostTables compares original and ghost tables to see whether the migration
// makes sense and is valid. It extracts the list of shared columns and the chosen migration unique key
func (this *Inspector) inspectOriginalAndGhostTables() (err error) {
	originalNamesOnApplier := this.migrationContext.OriginalTableColumnsOnApplier.Names()
	originalNames := this.migrationContext.OriginalTableColumns.Names()
	if !reflect.DeepEqual(originalNames, originalNamesOnApplier) {
		return fmt.Errorf("It seems like table structure is not identical between master and replica. This scenario is not supported.")
	}

	this.migrationContext.GhostTableColumns, this.migrationContext.GhostTableVirtualColumns, this.migrationContext.GhostTableUniqueKeys, err = this.InspectTableColumnsAndUniqueKeys(this.migrationContext.GetGhostTableName())
	if err != nil {
		return err
	}
	sharedUniqueKeys := this.getSharedUniqueKeys(this.migrationContext.OriginalTableUniqueKeys, this.migrationContext.GhostTableUniqueKeys)
	for i, sharedUniqueKey := range sharedUniqueKeys {
		this.applyColumnTypes(this.migrationContext.DatabaseName, this.migrationContext.OriginalTableName, &sharedUniqueKey.Columns)
		uniqueKeyIsValid := true
		for _, column := range sharedUniqueKey.Columns.Columns() {
			switch column.Type {
			case sql.FloatColumnType:
				{
					this.migrationContext.Log.Warningf("Will not use %+v as shared key due to FLOAT data type", sharedUniqueKey.Name)
					uniqueKeyIsValid = false
				}
			case sql.JSONColumnType:
				{
					// Noteworthy that at this time MySQL does not allow JSON indexing anyhow, but this code
					// will remain in place to potentially handle the future case where JSON is supported in indexes.
					this.migrationContext.Log.Warningf("Will not use %+v as shared key due to JSON data type", sharedUniqueKey.Name)
					uniqueKeyIsValid = false
				}
			}
		}
		if uniqueKeyIsValid {
			this.migrationContext.UniqueKey = sharedUniqueKeys[i]
			break
		}
	}
	if this.migrationContext.UniqueKey == nil {
		return fmt.Errorf("No shared unique key can be found after ALTER! Bailing out")
	}
	this.migrationContext.Log.Infof("Chosen shared unique key is %s", this.migrationContext.UniqueKey.Name)
	if this.migrationContext.UniqueKey.HasNullable {
		if this.migrationContext.NullableUniqueKeyAllowed {
			this.migrationContext.Log.Warningf("Chosen key (%s) has nullable columns. You have supplied with --allow-nullable-unique-key and so this migration proceeds. As long as there aren't NULL values in this key's column, migration should be fine. NULL values will corrupt migration's data", this.migrationContext.UniqueKey)
		} else {
			return fmt.Errorf("Chosen key (%s) has nullable columns. Bailing out. To force this operation to continue, supply --allow-nullable-unique-key flag. Only do so if you are certain there are no actual NULL values in this key. As long as there aren't, migration should be fine. NULL values in columns of this key will corrupt migration's data", this.migrationContext.UniqueKey)
		}
	}

	this.migrationContext.SharedColumns, this.migrationContext.MappedSharedColumns = this.getSharedColumns(this.migrationContext.OriginalTableColumns, this.migrationContext.GhostTableColumns, this.migrationContext.OriginalTableVirtualColumns, this.migrationContext.GhostTableVirtualColumns, this.migrationContext.ColumnRenameMap)
	this.migrationContext.Log.Infof("Shared columns are %s", this.migrationContext.SharedColumns)
	// By fact that a non-empty unique key exists we also know the shared columns are non-empty

	// This additional step looks at which columns are unsigned. We could have merged this within
	// the `getTableColumns()` function, but it's a later patch and introduces some complexity; I feel
	// comfortable in doing this as a separate step.
	this.applyColumnTypes(this.migrationContext.DatabaseName, this.migrationContext.OriginalTableName, this.migrationContext.OriginalTableColumns, this.migrationContext.SharedColumns, &this.migrationContext.UniqueKey.Columns)
	this.applyColumnTypes(this.migrationContext.DatabaseName, this.migrationContext.GetGhostTableName(), this.migrationContext.GhostTableColumns, this.migrationContext.MappedSharedColumns)

	for i := range this.migrationContext.SharedColumns.Columns() {
		column := this.migrationContext.SharedColumns.Columns()[i]
		mappedColumn := this.migrationContext.MappedSharedColumns.Columns()[i]
		if column.Name == mappedColumn.Name && column.Type == sql.DateTimeColumnType && mappedColumn.Type == sql.TimestampColumnType {
			this.migrationContext.MappedSharedColumns.SetConvertDatetimeToTimestamp(column.Name, this.migrationContext.ApplierTimeZone)
		}
		if column.Name == mappedColumn.Name && column.Type == sql.EnumColumnType && mappedColumn.Charset != "" {
			this.migrationContext.MappedSharedColumns.SetEnumToTextConversion(column.Name)
			this.migrationContext.MappedSharedColumns.SetEnumValues(column.Name, column.EnumValues)
		}
		if column.Name == mappedColumn.Name && column.Charset != mappedColumn.Charset {
			this.migrationContext.SharedColumns.SetCharsetConversion(column.Name, column.Charset, mappedColumn.Charset)
		}
	}

	for _, column := range this.migrationContext.UniqueKey.Columns.Columns() {
		if this.migrationContext.GhostTableVirtualColumns.GetColumn(column.Name) != nil {
			// this is a virtual column
			continue
		}
		if this.migrationContext.MappedSharedColumns.HasTimezoneConversion(column.Name) {
			return fmt.Errorf("No support at this time for converting a column from DATETIME to TIMESTAMP that is also part of the chosen unique key. Column: %s, key: %s", column.Name, this.migrationContext.UniqueKey.Name)
		}
	}

	return nil
}

// validateConnection issues a simple can-connect to MySQL
func (this *Inspector) validateConnection() error {
	if len(this.connectionConfig.Password) > mysql.MaxReplicationPasswordLength {
		return fmt.Errorf("MySQL replication length limited to 32 characters. See https://dev.mysql.com/doc/refman/5.7/en/assigning-passwords.html")
	}

	version, err := base.ValidateConnection(this.db, this.connectionConfig, this.migrationContext, this.name)
	this.migrationContext.InspectorMySQLVersion = version
	return err
}

// validateGrants verifies the user by which we're executing has necessary grants
// to do its thing.
func (this *Inspector) validateGrants() error {
	query := `show /* gh-ost */ grants for current_user()`
	foundAll := false
	foundSuper := false
	foundReplicationClient := false
	foundReplicationSlave := false
	foundDBAll := false

	err := sqlutils.QueryRowsMap(this.db, query, func(rowMap sqlutils.RowMap) error {
		for _, grantData := range rowMap {
			grant := grantData.String
			if strings.Contains(grant, `GRANT ALL PRIVILEGES ON *.*`) {
				foundAll = true
			}
			if strings.Contains(grant, `SUPER`) && strings.Contains(grant, ` ON *.*`) {
				foundSuper = true
			}
			if strings.Contains(grant, `REPLICATION CLIENT`) && strings.Contains(grant, ` ON *.*`) {
				foundReplicationClient = true
			}
			if strings.Contains(grant, `REPLICATION SLAVE`) && strings.Contains(grant, ` ON *.*`) {
				foundReplicationSlave = true
			}
			if strings.Contains(grant, fmt.Sprintf("GRANT ALL PRIVILEGES ON `%s`.*", this.migrationContext.DatabaseName)) {
				foundDBAll = true
			}
			if strings.Contains(grant, fmt.Sprintf("GRANT ALL PRIVILEGES ON `%s`.*", strings.Replace(this.migrationContext.DatabaseName, "_", "\\_", -1))) {
				foundDBAll = true
			}
			if base.StringContainsAll(grant, `ALTER`, `CREATE`, `DELETE`, `DROP`, `INDEX`, `INSERT`, `LOCK TABLES`, `SELECT`, `TRIGGER`, `UPDATE`, ` ON *.*`) {
				foundDBAll = true
			}
			if base.StringContainsAll(grant, `ALTER`, `CREATE`, `DELETE`, `DROP`, `INDEX`, `INSERT`, `LOCK TABLES`, `SELECT`, `TRIGGER`, `UPDATE`, fmt.Sprintf(" ON `%s`.*", this.migrationContext.DatabaseName)) {
				foundDBAll = true
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	this.migrationContext.HasSuperPrivilege = foundSuper

	if foundAll {
		this.migrationContext.Log.Infof("User has ALL privileges")
		return nil
	}
	if foundSuper && foundReplicationSlave && foundDBAll {
		this.migrationContext.Log.Infof("User has SUPER, REPLICATION SLAVE privileges, and has ALL privileges on %s.*", sql.EscapeName(this.migrationContext.DatabaseName))
		return nil
	}
	if foundReplicationClient && foundReplicationSlave && foundDBAll {
		this.migrationContext.Log.Infof("User has REPLICATION CLIENT, REPLICATION SLAVE privileges, and has ALL privileges on %s.*", sql.EscapeName(this.migrationContext.DatabaseName))
		return nil
	}
	this.migrationContext.Log.Debugf("Privileges: Super: %t, REPLICATION CLIENT: %t, REPLICATION SLAVE: %t, ALL on *.*: %t, ALL on %s.*: %t", foundSuper, foundReplicationClient, foundReplicationSlave, foundAll, sql.EscapeName(this.migrationContext.DatabaseName), foundDBAll)
	return this.migrationContext.Log.Errorf("User has insufficient privileges for migration. Needed: SUPER|REPLICATION CLIENT, REPLICATION SLAVE and ALL on %s.*", sql.EscapeName(this.migrationContext.DatabaseName))
}

// restartReplication is required so that we are _certain_ the binlog format and
// row image settings have actually been applied to the replication thread.
// It is entirely possible, for example, that the replication is using 'STATEMENT'
// binlog format even as the variable says 'ROW'
func (this *Inspector) restartReplication() error {
	this.migrationContext.Log.Infof("Restarting replication on %s to make sure binlog settings apply to replication thread", this.connectionConfig.Key.String())

	masterKey, _ := mysql.GetMasterKeyFromSlaveStatus(this.connectionConfig)
	if masterKey == nil {
		// This is not a replica
		return nil
	}

	var stopError, startError error
	_, stopError = sqlutils.ExecNoPrepare(this.db, `stop slave`)
	_, startError = sqlutils.ExecNoPrepare(this.db, `start slave`)
	if stopError != nil {
		return stopError
	}
	if startError != nil {
		return startError
	}

	// loop until replication is running unless we hit a max timeout.
	startTime := time.Now()
	for {
		replicationRunning, err := this.validateReplicationRestarted()
		if err != nil {
			return fmt.Errorf("Failed to validate if replication had been restarted: %w", err)
		}
		if replicationRunning {
			break
		}
		if time.Since(startTime) > startReplicationMaxWait {
			return fmt.Errorf("Replication did not restart within the maximum wait time of %s", startReplicationMaxWait)
		}
		this.migrationContext.Log.Debugf("Replication not yet restarted, waiting...")
		time.Sleep(startReplicationPostWait)
	}

	this.migrationContext.Log.Debugf("Replication restarted")
	return nil
}

// validateReplicationRestarted checks that the Slave_IO_Running and Slave_SQL_Running are both 'Yes'
// returns true if both are 'Yes', false otherwise
func (this *Inspector) validateReplicationRestarted() (bool, error) {
	errNotRunning := fmt.Errorf("Replication not running on %s", this.connectionConfig.Key.String())
	query := `show /* gh-ost */ slave status`
	err := sqlutils.QueryRowsMap(this.db, query, func(rowMap sqlutils.RowMap) error {
		if rowMap.GetString("Slave_IO_Running") != "Yes" || rowMap.GetString("Slave_SQL_Running") != "Yes" {
			return errNotRunning
		}
		return nil
	})

	if err != nil {
		// If the error is that replication is not running, return that and not an error
		if errors.Is(err, errNotRunning) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// applyBinlogFormat sets ROW binlog format and restarts replication to make
// the replication thread apply it.
func (this *Inspector) applyBinlogFormat() error {
	if this.migrationContext.RequiresBinlogFormatChange() {
		if !this.migrationContext.SwitchToRowBinlogFormat {
			return fmt.Errorf("Existing binlog_format is %s. Am not switching it to ROW unless you specify --switch-to-rbr", this.migrationContext.OriginalBinlogFormat)
		}
		if _, err := sqlutils.ExecNoPrepare(this.db, `set global binlog_format='ROW'`); err != nil {
			return err
		}
		if _, err := sqlutils.ExecNoPrepare(this.db, `set session binlog_format='ROW'`); err != nil {
			return err
		}
		if err := this.restartReplication(); err != nil {
			return err
		}
		this.migrationContext.Log.Debugf("'ROW' binlog format applied")
		return nil
	}
	// We already have RBR, no explicit switch
	if !this.migrationContext.AssumeRBR {
		if err := this.restartReplication(); err != nil {
			return err
		}
	}
	return nil
}

// validateBinlogs checks that binary log configuration is good to go
func (this *Inspector) validateBinlogs() error {
	query := `select /* gh-ost */ @@global.log_bin, @@global.binlog_format`
	var hasBinaryLogs bool
	if err := this.db.QueryRow(query).Scan(&hasBinaryLogs, &this.migrationContext.OriginalBinlogFormat); err != nil {
		return err
	}
	if !hasBinaryLogs {
		return fmt.Errorf("%s must have binary logs enabled", this.connectionConfig.Key.String())
	}
	if this.migrationContext.RequiresBinlogFormatChange() {
		if !this.migrationContext.SwitchToRowBinlogFormat {
			return fmt.Errorf("You must be using ROW binlog format. I can switch it for you, provided --switch-to-rbr and that %s doesn't have replicas", this.connectionConfig.Key.String())
		}
		query := `show /* gh-ost */ slave hosts`
		countReplicas := 0
		err := sqlutils.QueryRowsMap(this.db, query, func(rowMap sqlutils.RowMap) error {
			countReplicas++
			return nil
		})
		if err != nil {
			return err
		}
		if countReplicas > 0 {
			return fmt.Errorf("%s has %s binlog_format, but I'm too scared to change it to ROW because it has replicas. Bailing out", this.connectionConfig.Key.String(), this.migrationContext.OriginalBinlogFormat)
		}
		this.migrationContext.Log.Infof("%s has %s binlog_format. I will change it to ROW, and will NOT change it back, even in the event of failure.", this.connectionConfig.Key.String(), this.migrationContext.OriginalBinlogFormat)
	}
	query = `select /* gh-ost */ @@global.binlog_row_image`
	if err := this.db.QueryRow(query).Scan(&this.migrationContext.OriginalBinlogRowImage); err != nil {
		return err
	}
	this.migrationContext.OriginalBinlogRowImage = strings.ToUpper(this.migrationContext.OriginalBinlogRowImage)
	if this.migrationContext.OriginalBinlogRowImage != "FULL" {
		return fmt.Errorf("%s has '%s' binlog_row_image, and only 'FULL' is supported. This operation cannot proceed. You may `set global binlog_row_image='full'` and try again", this.connectionConfig.Key.String(), this.migrationContext.OriginalBinlogRowImage)
	}

	this.migrationContext.Log.Infof("binary logs validated on %s", this.connectionConfig.Key.String())
	return nil
}

// validateLogSlaveUpdates checks that binary log log_slave_updates is set. This test is not required when migrating on replica or when migrating directly on master
func (this *Inspector) validateLogSlaveUpdates() error {
	query := `select /* gh-ost */ @@global.log_slave_updates`
	var logSlaveUpdates bool
	if err := this.db.QueryRow(query).Scan(&logSlaveUpdates); err != nil {
		return err
	}

	if logSlaveUpdates {
		this.migrationContext.Log.Infof("log_slave_updates validated on %s", this.connectionConfig.Key.String())
		return nil
	}

	if this.migrationContext.IsTungsten {
		this.migrationContext.Log.Warningf("log_slave_updates not found on %s, but --tungsten provided, so I'm proceeding", this.connectionConfig.Key.String())
		return nil
	}

	if this.migrationContext.TestOnReplica || this.migrationContext.MigrateOnReplica {
		return fmt.Errorf("%s must have log_slave_updates enabled for testing/migrating on replica", this.connectionConfig.Key.String())
	}

	if this.migrationContext.InspectorIsAlsoApplier() {
		this.migrationContext.Log.Warningf("log_slave_updates not found on %s, but executing directly on master, so I'm proceeding", this.connectionConfig.Key.String())
		return nil
	}

	return fmt.Errorf("%s must have log_slave_updates enabled for executing migration", this.connectionConfig.Key.String())
}

// validateTable makes sure the table we need to operate on actually exists
func (this *Inspector) validateTable() error {
	query := fmt.Sprintf(`show /* gh-ost */ table status from %s like '%s'`, sql.EscapeName(this.migrationContext.DatabaseName), this.migrationContext.OriginalTableName)

	tableFound := false
	err := sqlutils.QueryRowsMap(this.db, query, func(rowMap sqlutils.RowMap) error {
		this.migrationContext.TableEngine = rowMap.GetString("Engine")
		this.migrationContext.RowsEstimate = rowMap.GetInt64("Rows")
		this.migrationContext.UsedRowsEstimateMethod = base.TableStatusRowsEstimate
		if rowMap.GetString("Comment") == "VIEW" {
			return fmt.Errorf("%s.%s is a VIEW, not a real table. Bailing out", sql.EscapeName(this.migrationContext.DatabaseName), sql.EscapeName(this.migrationContext.OriginalTableName))
		}
		tableFound = true

		return nil
	})
	if err != nil {
		return err
	}
	if !tableFound {
		return this.migrationContext.Log.Errorf("Cannot find table %s.%s!", sql.EscapeName(this.migrationContext.DatabaseName), sql.EscapeName(this.migrationContext.OriginalTableName))
	}
	this.migrationContext.Log.Infof("Table found. Engine=%s", this.migrationContext.TableEngine)
	this.migrationContext.Log.Debugf("Estimated number of rows via STATUS: %d", this.migrationContext.RowsEstimate)
	return nil
}

// validateTableForeignKeys makes sure no foreign keys exist on the migrated table
func (this *Inspector) validateTableForeignKeys(allowChildForeignKeys bool) error {
	if this.migrationContext.SkipForeignKeyChecks {
		this.migrationContext.Log.Warning("--skip-foreign-key-checks provided: will not check for foreign keys")
		return nil
	}
	query := `
		SELECT /* gh-ost */
			SUM(REFERENCED_TABLE_NAME IS NOT NULL AND TABLE_SCHEMA=? AND TABLE_NAME=?) as num_child_side_fk,
			SUM(REFERENCED_TABLE_NAME IS NOT NULL AND REFERENCED_TABLE_SCHEMA=? AND REFERENCED_TABLE_NAME=?) as num_parent_side_fk
		FROM
			INFORMATION_SCHEMA.KEY_COLUMN_USAGE
		WHERE
			REFERENCED_TABLE_NAME IS NOT NULL
			AND (
				(TABLE_SCHEMA=? AND TABLE_NAME=?)
				OR
				(REFERENCED_TABLE_SCHEMA=? AND REFERENCED_TABLE_NAME=?)
			)`
	numParentForeignKeys := 0
	numChildForeignKeys := 0
	err := sqlutils.QueryRowsMap(this.db, query, func(m sqlutils.RowMap) error {
		numChildForeignKeys = m.GetInt("num_child_side_fk")
		numParentForeignKeys = m.GetInt("num_parent_side_fk")
		return nil
	},
		this.migrationContext.DatabaseName,
		this.migrationContext.OriginalTableName,
		this.migrationContext.DatabaseName,
		this.migrationContext.OriginalTableName,
		this.migrationContext.DatabaseName,
		this.migrationContext.OriginalTableName,
		this.migrationContext.DatabaseName,
		this.migrationContext.OriginalTableName,
	)
	if err != nil {
		return err
	}
	if numParentForeignKeys > 0 {
		return this.migrationContext.Log.Errorf("Found %d parent-side foreign keys on %s.%s. Parent-side foreign keys are not supported. Bailing out", numParentForeignKeys, sql.EscapeName(this.migrationContext.DatabaseName), sql.EscapeName(this.migrationContext.OriginalTableName))
	}
	if numChildForeignKeys > 0 {
		if allowChildForeignKeys {
			this.migrationContext.Log.Debugf("Foreign keys found and will be dropped, as per given --discard-foreign-keys flag")
			return nil
		}
		return this.migrationContext.Log.Errorf("Found %d child-side foreign keys on %s.%s. Child-side foreign keys are not supported. Bailing out", numChildForeignKeys, sql.EscapeName(this.migrationContext.DatabaseName), sql.EscapeName(this.migrationContext.OriginalTableName))
	}
	this.migrationContext.Log.Debugf("Validated no foreign keys exist on table")
	return nil
}

// validateTableTriggers makes sure no triggers exist on the migrated table
func (this *Inspector) validateTableTriggers() error {
	query := `
		SELECT /* gh-ost */ COUNT(*) AS num_triggers
		FROM
			INFORMATION_SCHEMA.TRIGGERS
		WHERE
			TRIGGER_SCHEMA=?
			AND EVENT_OBJECT_TABLE=?`
	numTriggers := 0
	err := sqlutils.QueryRowsMap(this.db, query, func(rowMap sqlutils.RowMap) error {
		numTriggers = rowMap.GetInt("num_triggers")

		return nil
	},
		this.migrationContext.DatabaseName,
		this.migrationContext.OriginalTableName,
	)
	if err != nil {
		return err
	}
	if numTriggers > 0 {
		return this.migrationContext.Log.Errorf("Found triggers on %s.%s. Triggers are not supported at this time. Bailing out", sql.EscapeName(this.migrationContext.DatabaseName), sql.EscapeName(this.migrationContext.OriginalTableName))
	}
	this.migrationContext.Log.Debugf("Validated no triggers exist on table")
	return nil
}

// estimateTableRowsViaExplain estimates number of rows on original table
func (this *Inspector) estimateTableRowsViaExplain() error {
	query := fmt.Sprintf(`explain select /* gh-ost */ * from %s.%s where 1=1`, sql.EscapeName(this.migrationContext.DatabaseName), sql.EscapeName(this.migrationContext.OriginalTableName))

	outputFound := false
	err := sqlutils.QueryRowsMap(this.db, query, func(rowMap sqlutils.RowMap) error {
		this.migrationContext.RowsEstimate = rowMap.GetInt64("rows")
		this.migrationContext.UsedRowsEstimateMethod = base.ExplainRowsEstimate
		outputFound = true

		return nil
	})
	if err != nil {
		return err
	}
	if !outputFound {
		return this.migrationContext.Log.Errorf("Cannot run EXPLAIN on %s.%s!", sql.EscapeName(this.migrationContext.DatabaseName), sql.EscapeName(this.migrationContext.OriginalTableName))
	}
	this.migrationContext.Log.Infof("Estimated number of rows via EXPLAIN: %d", this.migrationContext.RowsEstimate)
	return nil
}

// CountTableRows counts exact number of rows on the original table
func (this *Inspector) CountTableRows(ctx context.Context) error {
	atomic.StoreInt64(&this.migrationContext.CountingRowsFlag, 1)
	defer atomic.StoreInt64(&this.migrationContext.CountingRowsFlag, 0)

	this.migrationContext.Log.Infof("As instructed, I'm issuing a SELECT COUNT(*) on the table. This may take a while")

	conn, err := this.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	var connectionID string
	if err := conn.QueryRowContext(ctx, `SELECT /* gh-ost */ CONNECTION_ID()`).Scan(&connectionID); err != nil {
		return err
	}

	query := fmt.Sprintf(`select /* gh-ost */ count(*) as count_rows from %s.%s`, sql.EscapeName(this.migrationContext.DatabaseName), sql.EscapeName(this.migrationContext.OriginalTableName))
	var rowsEstimate int64
	if err := conn.QueryRowContext(ctx, query).Scan(&rowsEstimate); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			this.migrationContext.Log.Infof("exact row count cancelled (%s), likely because I'm about to cut over. I'm going to kill that query.", ctx.Err())
			return mysql.Kill(this.db, connectionID)
		}
		return err
	}

	// row count query finished. nil out the cancel func, so the main migration thread
	// doesn't bother calling it after row copy is done.
	this.migrationContext.SetCountTableRowsCancelFunc(nil)

	atomic.StoreInt64(&this.migrationContext.RowsEstimate, rowsEstimate)
	this.migrationContext.UsedRowsEstimateMethod = base.CountRowsEstimate

	this.migrationContext.Log.Infof("Exact number of rows via COUNT: %d", rowsEstimate)

	return nil
}

// applyColumnTypes
func (this *Inspector) applyColumnTypes(databaseName, tableName string, columnsLists ...*sql.ColumnList) error {
	query := `
		select /* gh-ost */ *
		from
			information_schema.columns
		where
			table_schema=?
			and table_name=?`
	err := sqlutils.QueryRowsMap(this.db, query, func(m sqlutils.RowMap) error {
		columnName := m.GetString("COLUMN_NAME")
		columnType := m.GetString("COLUMN_TYPE")
		columnOctetLength := m.GetUint("CHARACTER_OCTET_LENGTH")
		extra := m.GetString("EXTRA")
		for _, columnsList := range columnsLists {
			column := columnsList.GetColumn(columnName)
			if column == nil {
				continue
			}

			if strings.Contains(columnType, "unsigned") {
				column.IsUnsigned = true
			}
			if strings.Contains(columnType, "mediumint") {
				column.Type = sql.MediumIntColumnType
			}
			if strings.Contains(columnType, "timestamp") {
				column.Type = sql.TimestampColumnType
			}
			if strings.Contains(columnType, "datetime") {
				column.Type = sql.DateTimeColumnType
			}
			if strings.Contains(columnType, "json") {
				column.Type = sql.JSONColumnType
			}
			if strings.Contains(columnType, "float") {
				column.Type = sql.FloatColumnType
			}
			if strings.HasPrefix(columnType, "enum") {
				column.Type = sql.EnumColumnType
				column.EnumValues = sql.ParseEnumValues(m.GetString("COLUMN_TYPE"))
			}
			if strings.HasPrefix(columnType, "binary") {
				column.Type = sql.BinaryColumnType
				column.BinaryOctetLength = columnOctetLength
			}
			if strings.Contains(extra, " GENERATED") {
				column.IsVirtual = true
			}
			if charset := m.GetString("CHARACTER_SET_NAME"); charset != "" {
				column.Charset = charset
			}
		}
		return nil
	}, databaseName, tableName)
	return err
}

// getAutoIncrementValue get's the original table's AUTO_INCREMENT value, if exists (0 value if not exists)
func (this *Inspector) getAutoIncrementValue(tableName string) (autoIncrement uint64, err error) {
	query := `
		SELECT /* gh-ost */ AUTO_INCREMENT
		FROM
			INFORMATION_SCHEMA.TABLES
		WHERE
			TABLES.TABLE_SCHEMA = ?
			AND TABLES.TABLE_NAME = ?
			AND AUTO_INCREMENT IS NOT NULL`
	err = sqlutils.QueryRowsMap(this.db, query, func(m sqlutils.RowMap) error {
		autoIncrement = m.GetUint64("AUTO_INCREMENT")
		return nil
	}, this.migrationContext.DatabaseName, tableName)
	return autoIncrement, err
}

// getCandidateUniqueKeys investigates a table and returns the list of unique keys
// candidate for chunking
func (this *Inspector) getCandidateUniqueKeys(tableName string) (uniqueKeys [](*sql.UniqueKey), err error) {
	query := `
		SELECT /* gh-ost */
			COLUMNS.TABLE_SCHEMA,
			COLUMNS.TABLE_NAME,
			COLUMNS.COLUMN_NAME,
			UNIQUES.INDEX_NAME,
			UNIQUES.COLUMN_NAMES,
			UNIQUES.COUNT_COLUMN_IN_INDEX,
			COLUMNS.DATA_TYPE,
			COLUMNS.CHARACTER_SET_NAME,
			LOCATE('auto_increment', EXTRA) > 0 as is_auto_increment,
			has_nullable
		FROM
			INFORMATION_SCHEMA.COLUMNS
		INNER JOIN (
			SELECT
				TABLE_SCHEMA,
				TABLE_NAME,
				INDEX_NAME,
				COUNT(*) AS COUNT_COLUMN_IN_INDEX,
				GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX ASC) AS COLUMN_NAMES,
				SUBSTRING_INDEX(GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX ASC), ',', 1) AS FIRST_COLUMN_NAME,
				SUM(NULLABLE='YES') > 0 AS has_nullable
			FROM
				INFORMATION_SCHEMA.STATISTICS
			WHERE
				NON_UNIQUE=0
				AND TABLE_SCHEMA = ?
				AND TABLE_NAME = ?
			GROUP BY
				TABLE_SCHEMA,
				TABLE_NAME,
				INDEX_NAME
		) AS UNIQUES
		ON (
			COLUMNS.COLUMN_NAME = UNIQUES.FIRST_COLUMN_NAME
		)
		WHERE
			COLUMNS.TABLE_SCHEMA = ?
			AND COLUMNS.TABLE_NAME = ?
		ORDER BY
			COLUMNS.TABLE_SCHEMA, COLUMNS.TABLE_NAME,
			CASE UNIQUES.INDEX_NAME
				WHEN 'PRIMARY' THEN 0
				ELSE 1
			END,
			CASE has_nullable
				WHEN 0 THEN 0
				ELSE 1
			END,
			CASE IFNULL(CHARACTER_SET_NAME, '')
				WHEN '' THEN 0
				ELSE 1
			END,
			CASE DATA_TYPE
				WHEN 'tinyint' THEN 0
				WHEN 'smallint' THEN 1
				WHEN 'int' THEN 2
				WHEN 'bigint' THEN 3
				ELSE 100
			END,
			COUNT_COLUMN_IN_INDEX`
	err = sqlutils.QueryRowsMap(this.db, query, func(m sqlutils.RowMap) error {
		uniqueKey := &sql.UniqueKey{
			Name:            m.GetString("INDEX_NAME"),
			Columns:         *sql.ParseColumnList(m.GetString("COLUMN_NAMES")),
			HasNullable:     m.GetBool("has_nullable"),
			IsAutoIncrement: m.GetBool("is_auto_increment"),
		}
		uniqueKeys = append(uniqueKeys, uniqueKey)
		return nil
	}, this.migrationContext.DatabaseName, tableName, this.migrationContext.DatabaseName, tableName)
	if err != nil {
		return uniqueKeys, err
	}
	this.migrationContext.Log.Debugf("Potential unique keys in %+v: %+v", tableName, uniqueKeys)
	return uniqueKeys, nil
}

// getSharedUniqueKeys returns the intersection of two given unique keys,
// testing by list of columns
func (this *Inspector) getSharedUniqueKeys(originalUniqueKeys, ghostUniqueKeys []*sql.UniqueKey) (uniqueKeys []*sql.UniqueKey) {
	// We actually do NOT rely on key name, just on the set of columns. This is because maybe
	// the ALTER is on the name itself...
	for _, originalUniqueKey := range originalUniqueKeys {
		for _, ghostUniqueKey := range ghostUniqueKeys {
			if originalUniqueKey.Columns.IsSubsetOf(&ghostUniqueKey.Columns) {
				uniqueKeys = append(uniqueKeys, originalUniqueKey)
				break
			}
		}
	}
	return uniqueKeys
}

// getSharedColumns returns the intersection of two lists of columns in same order as the first list
func (this *Inspector) getSharedColumns(originalColumns, ghostColumns *sql.ColumnList, originalVirtualColumns, ghostVirtualColumns *sql.ColumnList, columnRenameMap map[string]string) (*sql.ColumnList, *sql.ColumnList) {
	sharedColumnNames := []string{}
	for _, originalColumn := range originalColumns.Names() {
		isSharedColumn := false
		for _, ghostColumn := range ghostColumns.Names() {
			if strings.EqualFold(originalColumn, ghostColumn) {
				isSharedColumn = true
				break
			}
			if strings.EqualFold(columnRenameMap[originalColumn], ghostColumn) {
				isSharedColumn = true
				break
			}
		}
		for droppedColumn := range this.migrationContext.DroppedColumnsMap {
			if strings.EqualFold(originalColumn, droppedColumn) {
				isSharedColumn = false
				break
			}
		}
		for _, virtualColumn := range originalVirtualColumns.Names() {
			if strings.EqualFold(originalColumn, virtualColumn) {
				isSharedColumn = false
			}
		}
		for _, virtualColumn := range ghostVirtualColumns.Names() {
			if strings.EqualFold(originalColumn, virtualColumn) {
				isSharedColumn = false
			}
		}
		if isSharedColumn {
			sharedColumnNames = append(sharedColumnNames, originalColumn)
		}
	}
	mappedSharedColumnNames := []string{}
	for _, columnName := range sharedColumnNames {
		if mapped, ok := columnRenameMap[columnName]; ok {
			mappedSharedColumnNames = append(mappedSharedColumnNames, mapped)
		} else {
			mappedSharedColumnNames = append(mappedSharedColumnNames, columnName)
		}
	}
	return sql.NewColumnList(sharedColumnNames), sql.NewColumnList(mappedSharedColumnNames)
}

// showCreateTable returns the `show create table` statement for given table
func (this *Inspector) showCreateTable(tableName string) (createTableStatement string, err error) {
	var dummy string
	query := fmt.Sprintf(`show /* gh-ost */ create table %s.%s`, sql.EscapeName(this.migrationContext.DatabaseName), sql.EscapeName(tableName))
	err = this.db.QueryRow(query).Scan(&dummy, &createTableStatement)
	return createTableStatement, err
}

// readChangelogState reads changelog hints
func (this *Inspector) readChangelogState(hint string) (string, error) {
	query := fmt.Sprintf(`
		select /* gh-ost */ hint, value
		from
			%s.%s
		where
			hint = ? and id <= 255`,
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetChangelogTableName()),
	)
	result := ""
	err := sqlutils.QueryRowsMap(this.db, query, func(m sqlutils.RowMap) error {
		result = m.GetString("value")
		return nil
	}, hint)
	return result, err
}

func (this *Inspector) getMasterConnectionConfig() (applierConfig *mysql.ConnectionConfig, err error) {
	this.migrationContext.Log.Infof("Recursively searching for replication master")
	visitedKeys := mysql.NewInstanceKeyMap()
	return mysql.GetMasterConnectionConfigSafe(this.connectionConfig, visitedKeys, this.migrationContext.AllowedMasterMaster)
}

func (this *Inspector) getReplicationLag() (replicationLag time.Duration, err error) {
	replicationLag, err = mysql.GetReplicationLagFromSlaveStatus(
		this.informationSchemaDb,
	)
	return replicationLag, err
}

func (this *Inspector) Teardown() {
	this.db.Close()
	this.informationSchemaDb.Close()
}
