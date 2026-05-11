/*
   Copyright 2025 GitHub Inc.
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
	dbVersion           string
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

func (isp *Inspector) InitDBConnections() (err error) {
	inspectorUri := isp.connectionConfig.GetDBUri(isp.migrationContext.DatabaseName)
	if isp.db, _, err = mysql.GetDB(isp.migrationContext.Uuid, inspectorUri); err != nil {
		return err
	}

	informationSchemaUri := isp.connectionConfig.GetDBUri("information_schema")
	if isp.informationSchemaDb, _, err = mysql.GetDB(isp.migrationContext.Uuid, informationSchemaUri); err != nil {
		return err
	}

	if err := isp.validateConnection(); err != nil {
		return err
	}
	isp.dbVersion = isp.migrationContext.InspectorMySQLVersion

	if !isp.migrationContext.AliyunRDS && !isp.migrationContext.GoogleCloudPlatform && !isp.migrationContext.AzureMySQL {
		if impliedKey, err := mysql.GetInstanceKey(isp.db); err != nil {
			return err
		} else {
			isp.connectionConfig.ImpliedKey = impliedKey
		}
	}
	if err := isp.validateGrants(); err != nil {
		return err
	}
	if err := isp.validateBinlogs(); err != nil {
		return err
	}
	if isp.migrationContext.UseGTIDs {
		if err := isp.validateGTIDConfig(); err != nil {
			return err
		}
	}
	if err := isp.applyBinlogFormat(); err != nil {
		return err
	}
	isp.migrationContext.Log.Infof("Inspector initiated on %+v, version %+v", isp.connectionConfig.ImpliedKey, isp.migrationContext.InspectorMySQLVersion)
	return nil
}

func (isp *Inspector) ValidateOriginalTable() (err error) {
	if err := isp.validateTable(); err != nil {
		return err
	}
	if err := isp.validateTableForeignKeys(isp.migrationContext.DiscardForeignKeys); err != nil {
		return err
	}
	if err := isp.validateTableTriggers(); err != nil {
		return err
	}
	if err := isp.estimateTableRowsViaExplain(); err != nil {
		return err
	}
	return nil
}

func (isp *Inspector) InspectTableColumnsAndUniqueKeys(tableName string) (columns *sql.ColumnList, virtualColumns *sql.ColumnList, uniqueKeys [](*sql.UniqueKey), err error) {
	uniqueKeys, err = isp.getCandidateUniqueKeys(tableName)
	if err != nil {
		return columns, virtualColumns, uniqueKeys, err
	}
	if len(uniqueKeys) == 0 {
		return columns, virtualColumns, uniqueKeys, fmt.Errorf("no PRIMARY nor UNIQUE key found in table! Bailing out")
	}
	columns, virtualColumns, err = mysql.GetTableColumns(isp.db, isp.migrationContext.DatabaseName, tableName)
	if err != nil {
		return columns, virtualColumns, uniqueKeys, err
	}

	return columns, virtualColumns, uniqueKeys, nil
}

func (isp *Inspector) InspectOriginalTable() (err error) {
	isp.migrationContext.OriginalTableColumns, isp.migrationContext.OriginalTableVirtualColumns, isp.migrationContext.OriginalTableUniqueKeys, err = isp.InspectTableColumnsAndUniqueKeys(isp.migrationContext.OriginalTableName)
	if err != nil {
		return err
	}
	isp.migrationContext.OriginalTableAutoIncrement, err = isp.getAutoIncrementValue(isp.migrationContext.OriginalTableName)
	if err != nil {
		return err
	}
	return nil
}

// inspectOriginalAndGhostTables compares original and ghost tables to see whether the migration
// makes sense and is valid. It extracts the list of shared columns and the chosen migration unique key
func (isp *Inspector) inspectOriginalAndGhostTables() (err error) {
	originalNamesOnApplier := isp.migrationContext.OriginalTableColumnsOnApplier.Names()
	originalNames := isp.migrationContext.OriginalTableColumns.Names()
	if !reflect.DeepEqual(originalNames, originalNamesOnApplier) {
		return fmt.Errorf("it seems like table structure is not identical between master and replica. This scenario is not supported")
	}

	isp.migrationContext.GhostTableColumns, isp.migrationContext.GhostTableVirtualColumns, isp.migrationContext.GhostTableUniqueKeys, err = isp.InspectTableColumnsAndUniqueKeys(isp.migrationContext.GetGhostTableName())
	if err != nil {
		return err
	}
	sharedUniqueKeys := isp.getSharedUniqueKeys(isp.migrationContext.OriginalTableUniqueKeys, isp.migrationContext.GhostTableUniqueKeys)
	for i, sharedUniqueKey := range sharedUniqueKeys {
		isp.applyColumnTypes(isp.migrationContext.DatabaseName, isp.migrationContext.OriginalTableName, &sharedUniqueKey.Columns)
		uniqueKeyIsValid := true
		for _, column := range sharedUniqueKey.Columns.Columns() {
			switch column.Type {
			case sql.FloatColumnType:
				{
					isp.migrationContext.Log.Warningf("Will not use %+v as shared key due to FLOAT data type", sharedUniqueKey.Name)
					uniqueKeyIsValid = false
				}
			case sql.JSONColumnType:
				{
					// Noteworthy that at this time MySQL does not allow JSON indexing anyhow, but this code
					// will remain in place to potentially handle the future case where JSON is supported in indexes.
					isp.migrationContext.Log.Warningf("Will not use %+v as shared key due to JSON data type", sharedUniqueKey.Name)
					uniqueKeyIsValid = false
				}
			}
		}
		if uniqueKeyIsValid {
			isp.migrationContext.UniqueKey = sharedUniqueKeys[i]
			break
		}
	}
	if isp.migrationContext.UniqueKey == nil {
		return fmt.Errorf("no shared unique key can be found after ALTER! Bailing out")
	}
	isp.migrationContext.Log.Infof("Chosen shared unique key is %s", isp.migrationContext.UniqueKey.Name)
	if isp.migrationContext.UniqueKey.HasNullable {
		if isp.migrationContext.NullableUniqueKeyAllowed {
			isp.migrationContext.Log.Warningf("Chosen key (%s) has nullable columns. You have supplied with --allow-nullable-unique-key and so this migration proceeds. As long as there aren't NULL values in this key's column, migration should be fine. NULL values will corrupt migration's data", isp.migrationContext.UniqueKey)
		} else {
			return fmt.Errorf("chosen key (%s) has nullable columns. Bailing out. To force this operation to continue, supply --allow-nullable-unique-key flag. Only do so if you are certain there are no actual NULL values in this key. As long as there aren't, migration should be fine. NULL values in columns of this key will corrupt migration's data", isp.migrationContext.UniqueKey)
		}
	}

	isp.migrationContext.SharedColumns, isp.migrationContext.MappedSharedColumns = isp.getSharedColumns(isp.migrationContext.OriginalTableColumns, isp.migrationContext.GhostTableColumns, isp.migrationContext.OriginalTableVirtualColumns, isp.migrationContext.GhostTableVirtualColumns, isp.migrationContext.ColumnRenameMap)
	isp.migrationContext.Log.Infof("Shared columns are %s", isp.migrationContext.SharedColumns)
	// By fact that a non-empty unique key exists we also know the shared columns are non-empty

	// This additional step looks at which columns are unsigned. We could have merged this within
	// the `getTableColumns()` function, but it's a later patch and introduces some complexity; I feel
	// comfortable in doing this as a separate step.
	isp.applyColumnTypes(isp.migrationContext.DatabaseName, isp.migrationContext.OriginalTableName, isp.migrationContext.OriginalTableColumns, isp.migrationContext.SharedColumns, &isp.migrationContext.UniqueKey.Columns)
	isp.applyColumnTypes(isp.migrationContext.DatabaseName, isp.migrationContext.GetGhostTableName(), isp.migrationContext.GhostTableColumns, isp.migrationContext.MappedSharedColumns)

	for i := range isp.migrationContext.SharedColumns.Columns() {
		column := isp.migrationContext.SharedColumns.Columns()[i]
		mappedColumn := isp.migrationContext.MappedSharedColumns.Columns()[i]
		if column.Name == mappedColumn.Name && column.Type == sql.DateTimeColumnType && mappedColumn.Type == sql.TimestampColumnType {
			isp.migrationContext.MappedSharedColumns.SetConvertDatetimeToTimestamp(column.Name, isp.migrationContext.ApplierTimeZone)
		}
		if column.Name == mappedColumn.Name && column.Type == sql.EnumColumnType && mappedColumn.Charset != "" {
			isp.migrationContext.MappedSharedColumns.SetEnumToTextConversion(column.Name)
			isp.migrationContext.MappedSharedColumns.SetEnumValues(column.Name, column.EnumValues)
		}
		if column.Name == mappedColumn.Name && column.Charset != mappedColumn.Charset {
			isp.migrationContext.SharedColumns.SetCharsetConversion(column.Name, column.Charset, mappedColumn.Charset)
		}
	}

	for _, column := range isp.migrationContext.UniqueKey.Columns.Columns() {
		if isp.migrationContext.GhostTableVirtualColumns.GetColumn(column.Name) != nil {
			// this is a virtual column
			continue
		}
		if isp.migrationContext.MappedSharedColumns.HasTimezoneConversion(column.Name) {
			return fmt.Errorf("no support at this time for converting a column from DATETIME to TIMESTAMP that is also part of the chosen unique key. Column: %s, key: %s", column.Name, isp.migrationContext.UniqueKey.Name)
		}
	}

	return nil
}

// validateConnection issues a simple can-connect to MySQL
func (isp *Inspector) validateConnection() error {
	version, err := base.ValidateConnection(isp.db, isp.connectionConfig, isp.migrationContext, isp.name)
	isp.migrationContext.InspectorMySQLVersion = version
	return err
}

// validateGrants verifies the user by which we're executing has necessary grants
// to do its thing.
func (isp *Inspector) validateGrants() error {
	query := `show /* gh-ost */ grants for current_user()`
	foundAll := false
	foundSuper := false
	foundReplicationClient := false
	foundReplicationSlave := false
	foundDBAll := false

	err := sqlutils.QueryRowsMap(isp.db, query, func(rowMap sqlutils.RowMap) error {
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
			if strings.Contains(grant, fmt.Sprintf("GRANT ALL PRIVILEGES ON `%s`.*", isp.migrationContext.DatabaseName)) {
				foundDBAll = true
			}
			if strings.Contains(grant, fmt.Sprintf("GRANT ALL PRIVILEGES ON `%s`.*", strings.ReplaceAll(isp.migrationContext.DatabaseName, "_", "\\_"))) {
				foundDBAll = true
			}
			if base.StringContainsAll(grant, `ALTER`, `CREATE`, `DELETE`, `DROP`, `INDEX`, `INSERT`, `LOCK TABLES`, `SELECT`, `TRIGGER`, `UPDATE`, ` ON *.*`) {
				foundDBAll = true
			}
			if base.StringContainsAll(grant, `ALTER`, `CREATE`, `DELETE`, `DROP`, `INDEX`, `INSERT`, `LOCK TABLES`, `SELECT`, `TRIGGER`, `UPDATE`, fmt.Sprintf(" ON `%s`.*", isp.migrationContext.DatabaseName)) {
				foundDBAll = true
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	isp.migrationContext.HasSuperPrivilege = foundSuper

	if foundAll {
		isp.migrationContext.Log.Infof("User has ALL privileges")
		return nil
	}
	if foundSuper && foundReplicationSlave && foundDBAll {
		isp.migrationContext.Log.Infof("User has SUPER, REPLICATION SLAVE privileges, and has ALL privileges on %s.*", sql.EscapeName(isp.migrationContext.DatabaseName))
		return nil
	}
	if foundReplicationClient && foundReplicationSlave && foundDBAll {
		isp.migrationContext.Log.Infof("User has REPLICATION CLIENT, REPLICATION SLAVE privileges, and has ALL privileges on %s.*", sql.EscapeName(isp.migrationContext.DatabaseName))
		return nil
	}
	isp.migrationContext.Log.Debugf("Privileges: Super: %t, REPLICATION CLIENT: %t, REPLICATION SLAVE: %t, ALL on *.*: %t, ALL on %s.*: %t", foundSuper, foundReplicationClient, foundReplicationSlave, foundAll, sql.EscapeName(isp.migrationContext.DatabaseName), foundDBAll)
	return isp.migrationContext.Log.Errorf("user has insufficient privileges for migration. Needed: SUPER|REPLICATION CLIENT, REPLICATION SLAVE and ALL on %s.*", sql.EscapeName(isp.migrationContext.DatabaseName))
}

// restartReplication is required so that we are _certain_ the binlog format and
// row image settings have actually been applied to the replication thread.
// It is entirely possible, for example, that the replication is using 'STATEMENT'
// binlog format even as the variable says 'ROW'
func (isp *Inspector) restartReplication() error {
	isp.migrationContext.Log.Infof("Restarting replication on %s to make sure binlog settings apply to replication thread", isp.connectionConfig.Key.String())

	masterKey, _ := mysql.GetMasterKeyFromSlaveStatus(isp.dbVersion, isp.connectionConfig)
	if masterKey == nil {
		// This is not a replica
		return nil
	}

	var stopError, startError error
	replicaTerm := mysql.ReplicaTermFor(isp.dbVersion, `slave`)
	_, stopError = sqlutils.ExecNoPrepare(isp.db, fmt.Sprintf("stop %s", replicaTerm))
	_, startError = sqlutils.ExecNoPrepare(isp.db, fmt.Sprintf("start %s", replicaTerm))
	if stopError != nil {
		return stopError
	}
	if startError != nil {
		return startError
	}

	// loop until replication is running unless we hit a max timeout.
	startTime := time.Now()
	for {
		replicationRunning, err := isp.validateReplicationRestarted()
		if err != nil {
			return fmt.Errorf("failed to validate if replication had been restarted: %w", err)
		}
		if replicationRunning {
			break
		}
		if time.Since(startTime) > startReplicationMaxWait {
			return fmt.Errorf("replication did not restart within the maximum wait time of %s", startReplicationMaxWait)
		}
		isp.migrationContext.Log.Debugf("Replication not yet restarted, waiting...")
		time.Sleep(startReplicationPostWait)
	}

	isp.migrationContext.Log.Debugf("Replication restarted")
	return nil
}

// validateReplicationRestarted checks that the Slave_IO_Running and Slave_SQL_Running are both 'Yes'
// returns true if both are 'Yes', false otherwise
func (isp *Inspector) validateReplicationRestarted() (bool, error) {
	errNotRunning := fmt.Errorf("replication not running on %s", isp.connectionConfig.Key.String())
	query := fmt.Sprintf("show /* gh-ost */ %s", mysql.ReplicaTermFor(isp.dbVersion, "slave status"))
	err := sqlutils.QueryRowsMap(isp.db, query, func(rowMap sqlutils.RowMap) error {
		ioRunningTerm := mysql.ReplicaTermFor(isp.dbVersion, "Slave_IO_Running")
		sqlRunningTerm := mysql.ReplicaTermFor(isp.dbVersion, "Slave_SQL_Running")
		if rowMap.GetString(ioRunningTerm) != "Yes" || rowMap.GetString(sqlRunningTerm) != "Yes" {
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
func (isp *Inspector) applyBinlogFormat() error {
	if isp.migrationContext.RequiresBinlogFormatChange() {
		if !isp.migrationContext.SwitchToRowBinlogFormat {
			return fmt.Errorf("existing binlog_format is %s. Am not switching it to ROW unless you specify --switch-to-rbr", isp.migrationContext.OriginalBinlogFormat)
		}
		if _, err := sqlutils.ExecNoPrepare(isp.db, `set global binlog_format='ROW'`); err != nil {
			return err
		}
		if _, err := sqlutils.ExecNoPrepare(isp.db, `set session binlog_format='ROW'`); err != nil {
			return err
		}
		if err := isp.restartReplication(); err != nil {
			return err
		}
		isp.migrationContext.Log.Debugf("'ROW' binlog format applied")
		return nil
	}
	// We already have RBR, no explicit switch
	if !isp.migrationContext.AssumeRBR {
		if err := isp.restartReplication(); err != nil {
			return err
		}
	}
	return nil
}

// validateBinlogs checks that binary log configuration is good to go
func (isp *Inspector) validateBinlogs() error {
	query := `select /* gh-ost */@@global.log_bin, @@global.binlog_format`
	var hasBinaryLogs bool
	if err := isp.db.QueryRow(query).Scan(&hasBinaryLogs, &isp.migrationContext.OriginalBinlogFormat); err != nil {
		return err
	}
	if !hasBinaryLogs {
		return fmt.Errorf("%s must have binary logs enabled", isp.connectionConfig.Key.String())
	}
	if isp.migrationContext.RequiresBinlogFormatChange() {
		if !isp.migrationContext.SwitchToRowBinlogFormat {
			return fmt.Errorf("you must be using ROW binlog format. I can switch it for you, provided --switch-to-rbr and that %s doesn't have replicas", isp.connectionConfig.Key.String())
		}
		query := fmt.Sprintf("show /* gh-ost */ %s", mysql.ReplicaTermFor(isp.dbVersion, `slave hosts`))
		countReplicas := 0
		err := sqlutils.QueryRowsMap(isp.db, query, func(rowMap sqlutils.RowMap) error {
			countReplicas++
			return nil
		})
		if err != nil {
			return err
		}
		if countReplicas > 0 {
			return fmt.Errorf("%s has %s binlog_format, but I'm too scared to change it to ROW because it has replicas. Bailing out", isp.connectionConfig.Key.String(), isp.migrationContext.OriginalBinlogFormat)
		}
		isp.migrationContext.Log.Infof("%s has %s binlog_format. I will change it to ROW, and will NOT change it back, even in the event of failure.", isp.connectionConfig.Key.String(), isp.migrationContext.OriginalBinlogFormat)
	}
	query = `select /* gh-ost */ @@global.binlog_row_image`
	if err := isp.db.QueryRow(query).Scan(&isp.migrationContext.OriginalBinlogRowImage); err != nil {
		return err
	}
	isp.migrationContext.OriginalBinlogRowImage = strings.ToUpper(isp.migrationContext.OriginalBinlogRowImage)
	if isp.migrationContext.OriginalBinlogRowImage != "FULL" {
		return fmt.Errorf("%s has '%s' binlog_row_image, and only 'FULL' is supported. This operation cannot proceed. You may `set global binlog_row_image='full'` and try again", isp.connectionConfig.Key.String(), isp.migrationContext.OriginalBinlogRowImage)
	}

	isp.migrationContext.Log.Infof("binary logs validated on %s", isp.connectionConfig.Key.String())
	return nil
}

// validateGTIDConfig checks that the GTID configuration is good to go
func (isp *Inspector) validateGTIDConfig() error {
	var gtidMode, enforceGtidConsistency string
	query := `select @@global.gtid_mode, @@global.enforce_gtid_consistency`
	if err := isp.db.QueryRow(query).Scan(&gtidMode, &enforceGtidConsistency); err != nil {
		return err
	}
	enforceGtidConsistency = strings.ToUpper(enforceGtidConsistency)
	if strings.ToUpper(gtidMode) != "ON" || (enforceGtidConsistency != "ON" && enforceGtidConsistency != "1") {
		return fmt.Errorf("%s must have gtid_mode=ON and enforce_gtid_consistency=ON to use GTID support", isp.connectionConfig.Key.String())
	}

	isp.migrationContext.Log.Infof("gtid config validated on %s", isp.connectionConfig.Key.String())
	return nil
}

// validateLogSlaveUpdates checks that binary log log_slave_updates is set. This test is not required when migrating on replica or when migrating directly on master
func (isp *Inspector) validateLogSlaveUpdates() error {
	query := `select /* gh-ost */ @@global.log_slave_updates`
	var logSlaveUpdates bool
	if err := isp.db.QueryRow(query).Scan(&logSlaveUpdates); err != nil {
		return err
	}

	if logSlaveUpdates {
		isp.migrationContext.Log.Infof("log_slave_updates validated on %s", isp.connectionConfig.Key.String())
		return nil
	}

	if isp.migrationContext.IsTungsten {
		isp.migrationContext.Log.Warningf("log_slave_updates not found on %s, but --tungsten provided, so I'm proceeding", isp.connectionConfig.Key.String())
		return nil
	}

	if isp.migrationContext.TestOnReplica || isp.migrationContext.MigrateOnReplica {
		return fmt.Errorf("%s must have log_slave_updates enabled for testing/migrating on replica", isp.connectionConfig.Key.String())
	}

	if isp.migrationContext.InspectorIsAlsoApplier() {
		isp.migrationContext.Log.Warningf("log_slave_updates not found on %s, but executing directly on master, so I'm proceeding", isp.connectionConfig.Key.String())
		return nil
	}

	return fmt.Errorf("%s must have log_slave_updates enabled for executing migration", isp.connectionConfig.Key.String())
}

// validateTable makes sure the table we need to operate on actually exists
func (isp *Inspector) validateTable() error {
	query := fmt.Sprintf(`show /* gh-ost */ table status from %s like '%s'`, sql.EscapeName(isp.migrationContext.DatabaseName), isp.migrationContext.OriginalTableName)

	tableFound := false
	err := sqlutils.QueryRowsMap(isp.db, query, func(rowMap sqlutils.RowMap) error {
		isp.migrationContext.TableEngine = rowMap.GetString("Engine")
		isp.migrationContext.RowsEstimate = rowMap.GetInt64("Rows")
		isp.migrationContext.UsedRowsEstimateMethod = base.TableStatusRowsEstimate
		if rowMap.GetString("Comment") == "VIEW" {
			return fmt.Errorf("%s.%s is a VIEW, not a real table. Bailing out", sql.EscapeName(isp.migrationContext.DatabaseName), sql.EscapeName(isp.migrationContext.OriginalTableName))
		}
		tableFound = true

		return nil
	})
	if err != nil {
		return err
	}
	if !tableFound {
		return isp.migrationContext.Log.Errorf("cannot find table %s.%s!", sql.EscapeName(isp.migrationContext.DatabaseName), sql.EscapeName(isp.migrationContext.OriginalTableName))
	}
	isp.migrationContext.Log.Infof("Table found. Engine=%s", isp.migrationContext.TableEngine)
	isp.migrationContext.Log.Debugf("Estimated number of rows via STATUS: %d", isp.migrationContext.RowsEstimate)
	return nil
}

// validateTableForeignKeys makes sure no foreign keys exist on the migrated table
func (isp *Inspector) validateTableForeignKeys(allowChildForeignKeys bool) error {
	if isp.migrationContext.SkipForeignKeyChecks {
		isp.migrationContext.Log.Warning("--skip-foreign-key-checks provided: will not check for foreign keys")
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
	err := sqlutils.QueryRowsMap(isp.db, query, func(m sqlutils.RowMap) error {
		numChildForeignKeys = m.GetInt("num_child_side_fk")
		numParentForeignKeys = m.GetInt("num_parent_side_fk")
		return nil
	},
		isp.migrationContext.DatabaseName,
		isp.migrationContext.OriginalTableName,
		isp.migrationContext.DatabaseName,
		isp.migrationContext.OriginalTableName,
		isp.migrationContext.DatabaseName,
		isp.migrationContext.OriginalTableName,
		isp.migrationContext.DatabaseName,
		isp.migrationContext.OriginalTableName,
	)
	if err != nil {
		return err
	}
	if numParentForeignKeys > 0 {
		return isp.migrationContext.Log.Errorf("found %d parent-side foreign keys on %s.%s. Parent-side foreign keys are not supported. Bailing out", numParentForeignKeys, sql.EscapeName(isp.migrationContext.DatabaseName), sql.EscapeName(isp.migrationContext.OriginalTableName))
	}
	if numChildForeignKeys > 0 {
		if allowChildForeignKeys {
			isp.migrationContext.Log.Debugf("Foreign keys found and will be dropped, as per given --discard-foreign-keys flag")
			return nil
		}
		return isp.migrationContext.Log.Errorf("found %d child-side foreign keys on %s.%s. Child-side foreign keys are not supported. Bailing out", numChildForeignKeys, sql.EscapeName(isp.migrationContext.DatabaseName), sql.EscapeName(isp.migrationContext.OriginalTableName))
	}
	isp.migrationContext.Log.Debugf("Validated no foreign keys exist on table")
	return nil
}

// validateTableTriggers makes sure no triggers exist on the migrated table. if --include_triggers is used then it fetches the triggers
func (isp *Inspector) validateTableTriggers() error {
	query := `
		SELECT /* gh-ost */ COUNT(*) AS num_triggers
		FROM
			INFORMATION_SCHEMA.TRIGGERS
		WHERE
			TRIGGER_SCHEMA=?
			AND EVENT_OBJECT_TABLE=?`
	numTriggers := 0
	err := sqlutils.QueryRowsMap(isp.db, query, func(rowMap sqlutils.RowMap) error {
		numTriggers = rowMap.GetInt("num_triggers")

		return nil
	},
		isp.migrationContext.DatabaseName,
		isp.migrationContext.OriginalTableName,
	)
	if err != nil {
		return err
	}
	if numTriggers > 0 {
		if isp.migrationContext.IncludeTriggers {
			isp.migrationContext.Log.Infof("Found %d triggers on %s.%s.", numTriggers, sql.EscapeName(isp.migrationContext.DatabaseName), sql.EscapeName(isp.migrationContext.OriginalTableName))
			isp.migrationContext.Triggers, err = mysql.GetTriggers(isp.db, isp.migrationContext.DatabaseName, isp.migrationContext.OriginalTableName)
			if err != nil {
				return err
			}
			if err := isp.validateGhostTriggersDontExist(); err != nil {
				return err
			}
			if err := isp.validateGhostTriggersLength(); err != nil {
				return err
			}
			return nil
		}
		return isp.migrationContext.Log.Errorf("found triggers on %s.%s. Tables with triggers are supported only when using \"include-triggers\" flag. Bailing out", sql.EscapeName(isp.migrationContext.DatabaseName), sql.EscapeName(isp.migrationContext.OriginalTableName))
	}
	isp.migrationContext.Log.Debugf("Validated no triggers exist on table")
	return nil
}

// verifyTriggersDontExist verifies before createing new triggers we want to make sure these triggers dont exist already in the DB
func (isp *Inspector) validateGhostTriggersDontExist() error {
	if len(isp.migrationContext.Triggers) > 0 {
		var foundTriggers []string
		for _, trigger := range isp.migrationContext.Triggers {
			triggerName := isp.migrationContext.GetGhostTriggerName(trigger.Name)
			query := "select 1 from information_schema.triggers where trigger_name = ? and trigger_schema = ?"
			err := sqlutils.QueryRowsMap(isp.db, query, func(rowMap sqlutils.RowMap) error {
				triggerExists := rowMap.GetInt("1")
				if triggerExists == 1 {
					foundTriggers = append(foundTriggers, triggerName)
				}
				return nil
			},
				triggerName,
				isp.migrationContext.DatabaseName,
			)
			if err != nil {
				return err
			}
		}
		if len(foundTriggers) > 0 {
			return isp.migrationContext.Log.Errorf("found gh-ost triggers (%s). Please use a different suffix or drop them. Bailing out", strings.Join(foundTriggers, ","))
		}
	}

	return nil
}

func (isp *Inspector) validateGhostTriggersLength() error {
	if len(isp.migrationContext.Triggers) > 0 {
		var foundTriggers []string
		for _, trigger := range isp.migrationContext.Triggers {
			triggerName := isp.migrationContext.GetGhostTriggerName(trigger.Name)
			if ok := isp.migrationContext.ValidateGhostTriggerLengthBelowMaxLength(triggerName); !ok {
				foundTriggers = append(foundTriggers, triggerName)
			}
		}
		if len(foundTriggers) > 0 {
			return isp.migrationContext.Log.Errorf("gh-ost triggers (%s) length > %d characters. Bailing out", strings.Join(foundTriggers, ","), mysql.MaxTableNameLength)
		}
	}
	return nil
}

// estimateTableRowsViaExplain estimates number of rows on original table
func (isp *Inspector) estimateTableRowsViaExplain() error {
	query := fmt.Sprintf(`explain select /* gh-ost */ * from %s.%s where 1=1`, sql.EscapeName(isp.migrationContext.DatabaseName), sql.EscapeName(isp.migrationContext.OriginalTableName))

	outputFound := false
	err := sqlutils.QueryRowsMap(isp.db, query, func(rowMap sqlutils.RowMap) error {
		isp.migrationContext.RowsEstimate = rowMap.GetInt64("rows")
		isp.migrationContext.UsedRowsEstimateMethod = base.ExplainRowsEstimate
		outputFound = true

		return nil
	})
	if err != nil {
		return err
	}
	if !outputFound {
		return isp.migrationContext.Log.Errorf("cannot run EXPLAIN on %s.%s!", sql.EscapeName(isp.migrationContext.DatabaseName), sql.EscapeName(isp.migrationContext.OriginalTableName))
	}
	isp.migrationContext.Log.Infof("Estimated number of rows via EXPLAIN: %d", isp.migrationContext.RowsEstimate)
	return nil
}

// CountTableRows counts exact number of rows on the original table
func (isp *Inspector) CountTableRows(ctx context.Context) error {
	atomic.StoreInt64(&isp.migrationContext.CountingRowsFlag, 1)
	defer atomic.StoreInt64(&isp.migrationContext.CountingRowsFlag, 0)

	isp.migrationContext.Log.Infof("As instructed, I'm issuing a SELECT COUNT(*) on the table. This may take a while")

	conn, err := isp.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	var connectionID string
	if err := conn.QueryRowContext(ctx, `SELECT /* gh-ost */ CONNECTION_ID()`).Scan(&connectionID); err != nil {
		return err
	}

	query := fmt.Sprintf(`select /* gh-ost */ count(*) as count_rows from %s.%s`, sql.EscapeName(isp.migrationContext.DatabaseName), sql.EscapeName(isp.migrationContext.OriginalTableName))
	var rowsEstimate int64
	if err := conn.QueryRowContext(ctx, query).Scan(&rowsEstimate); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			isp.migrationContext.Log.Infof("exact row count cancelled (%s), likely because I'm about to cut over. I'm going to kill that query.", ctx.Err())
			return mysql.Kill(isp.db, connectionID)
		}
		return err
	}

	// row count query finished. nil out the cancel func, so the main migration thread
	// doesn't bother calling it after row copy is done.
	isp.migrationContext.SetCountTableRowsCancelFunc(nil)

	atomic.StoreInt64(&isp.migrationContext.RowsEstimate, rowsEstimate)
	isp.migrationContext.UsedRowsEstimateMethod = base.CountRowsEstimate

	isp.migrationContext.Log.Infof("Exact number of rows via COUNT: %d", rowsEstimate)

	return nil
}

// applyColumnTypes
func (isp *Inspector) applyColumnTypes(databaseName, tableName string, columnsLists ...*sql.ColumnList) error {
	query := `
		select /* gh-ost */ *
		from
			information_schema.columns
		where
			table_schema=?
			and table_name=?`
	err := sqlutils.QueryRowsMap(isp.db, query, func(m sqlutils.RowMap) error {
		columnName := m.GetString("COLUMN_NAME")
		columnType := m.GetString("COLUMN_TYPE")
		columnOctetLength := m.GetUint("CHARACTER_OCTET_LENGTH")
		isNullable := m.GetString("IS_NULLABLE")
		extra := m.GetString("EXTRA")
		for _, columnsList := range columnsLists {
			column := columnsList.GetColumn(columnName)
			if column == nil {
				continue
			}
			column.MySQLType = columnType
			if isNullable == "YES" {
				column.Nullable = true
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
func (isp *Inspector) getAutoIncrementValue(tableName string) (autoIncrement uint64, err error) {
	query := `
		SELECT /* gh-ost */ AUTO_INCREMENT
		FROM
			INFORMATION_SCHEMA.TABLES
		WHERE
			TABLES.TABLE_SCHEMA = ?
			AND TABLES.TABLE_NAME = ?
			AND AUTO_INCREMENT IS NOT NULL`
	err = sqlutils.QueryRowsMap(isp.db, query, func(m sqlutils.RowMap) error {
		autoIncrement = m.GetUint64("AUTO_INCREMENT")
		return nil
	}, isp.migrationContext.DatabaseName, tableName)
	return autoIncrement, err
}

// getCandidateUniqueKeys investigates a table and returns the list of unique keys
// candidate for chunking
func (isp *Inspector) getCandidateUniqueKeys(tableName string) (uniqueKeys [](*sql.UniqueKey), err error) {
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
	err = sqlutils.QueryRowsMap(isp.db, query, func(m sqlutils.RowMap) error {
		uniqueKey := &sql.UniqueKey{
			Name:            m.GetString("INDEX_NAME"),
			Columns:         *sql.ParseColumnList(m.GetString("COLUMN_NAMES")),
			HasNullable:     m.GetBool("has_nullable"),
			IsAutoIncrement: m.GetBool("is_auto_increment"),
		}
		uniqueKeys = append(uniqueKeys, uniqueKey)
		return nil
	}, isp.migrationContext.DatabaseName, tableName, isp.migrationContext.DatabaseName, tableName)
	if err != nil {
		return uniqueKeys, err
	}
	isp.migrationContext.Log.Debugf("Potential unique keys in %+v: %+v", tableName, uniqueKeys)
	return uniqueKeys, nil
}

// getSharedUniqueKeys returns the intersection of two given unique keys,
// testing by list of columns
func (isp *Inspector) getSharedUniqueKeys(originalUniqueKeys, ghostUniqueKeys []*sql.UniqueKey) (uniqueKeys []*sql.UniqueKey) {
	// We actually do NOT rely on key name, just on the set of columns. This is because maybe
	// the ALTER is on the name itself...
	for _, originalUniqueKey := range originalUniqueKeys {
		for _, ghostUniqueKey := range ghostUniqueKeys {
			if originalUniqueKey.Columns.IsSubsetOf(&ghostUniqueKey.Columns) {
				// In case the unique key gets renamed in -alter, PanicOnWarnings needs to rely on the new name
				// to check SQL warnings on the ghost table, so return new name here.
				originalUniqueKey.NameInGhostTable = ghostUniqueKey.Name
				uniqueKeys = append(uniqueKeys, originalUniqueKey)
				break
			}
		}
	}
	return uniqueKeys
}

// getSharedColumns returns the intersection of two lists of columns in same order as the first list
func (isp *Inspector) getSharedColumns(originalColumns, ghostColumns *sql.ColumnList, originalVirtualColumns, ghostVirtualColumns *sql.ColumnList, columnRenameMap map[string]string) (*sql.ColumnList, *sql.ColumnList) {
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
		for droppedColumn := range isp.migrationContext.DroppedColumnsMap {
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
func (isp *Inspector) showCreateTable(tableName string) (createTableStatement string, err error) {
	var dummy string
	query := fmt.Sprintf(`show /* gh-ost */ create table %s.%s`, sql.EscapeName(isp.migrationContext.DatabaseName), sql.EscapeName(tableName))
	err = isp.db.QueryRow(query).Scan(&dummy, &createTableStatement)
	return createTableStatement, err
}

// readChangelogState reads changelog hints
func (isp *Inspector) readChangelogState(hint string) (string, error) {
	query := fmt.Sprintf(`
		select /* gh-ost */ hint, value
		from
			%s.%s
		where
			hint = ? and id <= 255`,
		sql.EscapeName(isp.migrationContext.DatabaseName),
		sql.EscapeName(isp.migrationContext.GetChangelogTableName()),
	)
	result := ""
	err := sqlutils.QueryRowsMap(isp.db, query, func(m sqlutils.RowMap) error {
		result = m.GetString("value")
		return nil
	}, hint)
	return result, err
}

func (isp *Inspector) getMasterConnectionConfig() (applierConfig *mysql.ConnectionConfig, err error) {
	isp.migrationContext.Log.Infof("Recursively searching for replication master")
	visitedKeys := mysql.NewInstanceKeyMap()
	return mysql.GetMasterConnectionConfigSafe(isp.dbVersion, isp.connectionConfig, visitedKeys, isp.migrationContext.AllowedMasterMaster)
}

func (isp *Inspector) getReplicationLag() (replicationLag time.Duration, err error) {
	replicationLag, err = mysql.GetReplicationLagFromSlaveStatus(
		isp.dbVersion,
		isp.informationSchemaDb,
	)
	return replicationLag, err
}

func (isp *Inspector) Teardown() {
	isp.db.Close()
	isp.informationSchemaDb.Close()
}
