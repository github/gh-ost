/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	gosql "database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"

	"github.com/outbrain/golib/log"
	"github.com/outbrain/golib/sqlutils"
)

const startSlavePostWaitMilliseconds = 500 * time.Millisecond

// Inspector reads data from the read-MySQL-server (typically a replica, but can be the master)
// It is used for gaining initial status and structure, and later also follow up on progress and changelog
type Inspector struct {
	connectionConfig *mysql.ConnectionConfig
	db               *gosql.DB
	migrationContext *base.MigrationContext
}

func NewInspector() *Inspector {
	return &Inspector{
		connectionConfig: base.GetMigrationContext().InspectorConnectionConfig,
		migrationContext: base.GetMigrationContext(),
	}
}

func (this *Inspector) InitDBConnections() (err error) {
	inspectorUri := this.connectionConfig.GetDBUri(this.migrationContext.DatabaseName)
	if this.db, _, err = sqlutils.GetDB(inspectorUri); err != nil {
		return err
	}
	if err := this.validateConnection(); err != nil {
		return err
	}
	if impliedKey, err := mysql.GetInstanceKey(this.db); err != nil {
		return err
	} else {
		this.connectionConfig.ImpliedKey = impliedKey
	}

	// 验证当前的授权是否OK
	if err := this.validateGrants(); err != nil {
		return err
	}
	if err := this.validateBinlogs(); err != nil {
		return err
	}
	if err := this.applyBinlogFormat(); err != nil {
		return err
	}
	log.Infof("Inspector initiated on %+v, version %+v", this.connectionConfig.ImpliedKey, this.migrationContext.InspectorMySQLVersion)
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

func (this *Inspector) InspectTableColumnsAndUniqueKeys(tableName string) (columns *sql.ColumnList, uniqueKeys [](*sql.UniqueKey), err error) {
	uniqueKeys, err = this.getCandidateUniqueKeys(tableName)
	if err != nil {
		return columns, uniqueKeys, err
	}
	if len(uniqueKeys) == 0 {
		return columns, uniqueKeys, fmt.Errorf("No PRIMARY nor UNIQUE key found in table! Bailing out")
	}
	columns, err = mysql.GetTableColumns(this.db, this.migrationContext.DatabaseName, tableName)
	if err != nil {
		return columns, uniqueKeys, err
	}

	return columns, uniqueKeys, nil
}

func (this *Inspector) InspectOriginalTable() (err error) {
	this.migrationContext.OriginalTableColumns, this.migrationContext.OriginalTableUniqueKeys, err = this.InspectTableColumnsAndUniqueKeys(this.migrationContext.OriginalTableName)
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

	this.migrationContext.GhostTableColumns, this.migrationContext.GhostTableUniqueKeys, err = this.InspectTableColumnsAndUniqueKeys(this.migrationContext.GetGhostTableName())
	if err != nil {
		return err
	}

	// 如何选择一组可靠的Uniq Keys呢?
	// 可能由于删除字段，导致两者无法共有一组相同的UniqKey, 导致迁移无法j进行
	sharedUniqueKeys, err := this.getSharedUniqueKeys(this.migrationContext.OriginalTableUniqueKeys, this.migrationContext.GhostTableUniqueKeys)
	if err != nil {
		return err
	}
	if len(sharedUniqueKeys) == 0 {
		return fmt.Errorf("No shared unique key can be found after ALTER! Bailing out")
	}
	this.migrationContext.UniqueKey = sharedUniqueKeys[0]
	log.Infof("Chosen shared unique key is %s", this.migrationContext.UniqueKey.Name)

	// 是否允许Nullable？
	if this.migrationContext.UniqueKey.HasNullable {
		if this.migrationContext.NullableUniqueKeyAllowed {
			log.Warningf("Chosen key (%s) has nullable columns. You have supplied with --allow-nullable-unique-key and so this migration proceeds. As long as there aren't NULL values in this key's column, migration should be fine. NULL values will corrupt migration's data", this.migrationContext.UniqueKey)
		} else {
			return fmt.Errorf("Chosen key (%s) has nullable columns. Bailing out. To force this operation to continue, supply --allow-nullable-unique-key flag. Only do so if you are certain there are no actual NULL values in this key. As long as there aren't, migration should be fine. NULL values in columns of this key will corrupt migration's data", this.migrationContext.UniqueKey)
		}
	}

	// 是否为Primary Key呢?
	// 默认选择: binlog_row_image = full, 这样binlog中就有所需要的全部信息了
	if !this.migrationContext.UniqueKey.IsPrimary() {
		if this.migrationContext.OriginalBinlogRowImage != "FULL" {
			return fmt.Errorf("binlog_row_image is '%s' and chosen key is %s, which is not the primary key. This operation cannot proceed. You may `set global binlog_row_image='full'` and try again", this.migrationContext.OriginalBinlogRowImage, this.migrationContext.UniqueKey)
		}
	}

	this.migrationContext.SharedColumns, this.migrationContext.MappedSharedColumns = this.getSharedColumns(this.migrationContext.OriginalTableColumns,
		this.migrationContext.GhostTableColumns,
		this.migrationContext.ColumnRenameMap)
	log.Infof("Shared columns are %s", this.migrationContext.SharedColumns)
	// By fact that a non-empty unique key exists we also know the shared columns are non-empty

	// This additional step looks at which columns are unsigned. We could have merged this within
	// the `getTableColumns()` function, but it's a later patch and introduces some complexity; I feel
	// comfortable in doing this as a separate step.
	this.applyColumnTypes(this.migrationContext.DatabaseName, this.migrationContext.OriginalTableName,
		this.migrationContext.OriginalTableColumns, this.migrationContext.SharedColumns)
	this.applyColumnTypes(this.migrationContext.DatabaseName, this.migrationContext.OriginalTableName,
		&this.migrationContext.UniqueKey.Columns)
	this.applyColumnTypes(this.migrationContext.DatabaseName, this.migrationContext.GetGhostTableName(),
		this.migrationContext.GhostTableColumns, this.migrationContext.MappedSharedColumns)

	for i := range this.migrationContext.SharedColumns.Columns() {
		column := this.migrationContext.SharedColumns.Columns()[i]
		mappedColumn := this.migrationContext.MappedSharedColumns.Columns()[i]
		if column.Name == mappedColumn.Name && column.Type == sql.DateTimeColumnType && mappedColumn.Type == sql.TimestampColumnType {
			this.migrationContext.MappedSharedColumns.SetConvertDatetimeToTimestamp(column.Name, this.migrationContext.ApplierTimeZone)
		}
	}

	for _, column := range this.migrationContext.UniqueKey.Columns.Columns() {
		if this.migrationContext.MappedSharedColumns.HasTimezoneConversion(column.Name) {
			return fmt.Errorf("No support at this time for converting a column from DATETIME to TIMESTAMP that is also part of the chosen unique key. Column: %s, key: %s", column.Name, this.migrationContext.UniqueKey.Name)
		}
	}

	return nil
}

// validateConnection issues a simple can-connect to MySQL
func (this *Inspector) validateConnection() error {
	query := `select @@global.port, @@global.version`
	var port int
	if err := this.db.QueryRow(query).Scan(&port, &this.migrationContext.InspectorMySQLVersion); err != nil {
		return err
	}
	if port != this.connectionConfig.Key.Port {
		return fmt.Errorf("Unexpected database port reported: %+v", port)
	}
	log.Infof("connection validated on %+v", this.connectionConfig.Key)
	return nil
}

// validateGrants verifies the user by which we're executing has necessary grants
// to do its thang.
// 权限的验证
//
func (this *Inspector) validateGrants() error {
	query := `show /* gh-ost */ grants for current_user()`
	foundAll := false
	foundSuper := false
	foundReplicationClient := false
	foundReplicationSlave := false
	foundDBAll := false

	// 如何验证权限呢?
	// GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES,
	// LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER ON *.* TO 'root'@'%' WITH GRANT OPTION
	//
	// ON *.* 表示 对当前用户开发的权限
	err := sqlutils.QueryRowsMap(this.db, query, func(rowMap sqlutils.RowMap) error {
		for _, grantData := range rowMap {
			grant := grantData.String

			// 1. 全部授权
			//   *.* 这个问题如何解决?
			if strings.Contains(grant, `GRANT ALL PRIVILEGES ON *.*`) {
				foundAll = true
			}
			// 2. SUPER 全部
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
		log.Infof("User has ALL privileges")
		return nil
	}
	if foundSuper && foundReplicationSlave && foundDBAll {
		log.Infof("User has SUPER, REPLICATION SLAVE privileges, and has ALL privileges on %s.*", sql.EscapeName(this.migrationContext.DatabaseName))
		return nil
	}
	if foundReplicationClient && foundReplicationSlave && foundDBAll {
		log.Infof("User has REPLICATION CLIENT, REPLICATION SLAVE privileges, and has ALL privileges on %s.*", sql.EscapeName(this.migrationContext.DatabaseName))
		return nil
	}
	log.Debugf("Privileges: Super: %t, REPLICATION CLIENT: %t, REPLICATION SLAVE: %t, ALL on *.*: %t, ALL on %s.*: %t", foundSuper, foundReplicationClient, foundReplicationSlave, foundAll, sql.EscapeName(this.migrationContext.DatabaseName), foundDBAll)
	return log.Errorf("User has insufficient privileges for migration. Needed: SUPER|REPLICATION CLIENT, REPLICATION SLAVE and ALL on %s.*", sql.EscapeName(this.migrationContext.DatabaseName))
}

// restartReplication is required so that we are _certain_ the binlog format and
// row image settings have actually been applied to the replication thread.
// It is entriely possible, for example, that the replication is using 'STATEMENT'
// binlog format even as the variable says 'ROW'
func (this *Inspector) restartReplication() error {
	log.Infof("Restarting replication on %s:%d to make sure binlog settings apply to replication thread", this.connectionConfig.Key.Hostname, this.connectionConfig.Key.Port)

	masterKey, _ := mysql.GetMasterKeyFromSlaveStatus(this.connectionConfig)
	if masterKey == nil {
		// This is not a replica
		return nil
	}

	// stop slave
	// stop slave io_thread
	// stop slave sql_thread
	//
	var stopError, startError error
	_, stopError = sqlutils.ExecNoPrepare(this.db, `stop slave`)
	_, startError = sqlutils.ExecNoPrepare(this.db, `start slave`)
	if stopError != nil {
		return stopError
	}
	if startError != nil {
		return startError
	}
	time.Sleep(startSlavePostWaitMilliseconds)

	log.Debugf("Replication restarted")
	return nil
}

// applyBinlogFormat sets ROW binlog format and restarts replication to make
// the replication thread apply it.
func (this *Inspector) applyBinlogFormat() error {
	if this.migrationContext.RequiresBinlogFormatChange() {
		if !this.migrationContext.SwitchToRowBinlogFormat {
			return fmt.Errorf("Existing binlog_format is %s. Am not switching it to ROW unless you specify --switch-to-rbr", this.migrationContext.OriginalBinlogFormat)
		}

		// 修改global
		// 修改session
		// global才会导致所有其他session的binlog ok
		if _, err := sqlutils.ExecNoPrepare(this.db, `set global binlog_format='ROW'`); err != nil {
			return err
		}
		if _, err := sqlutils.ExecNoPrepare(this.db, `set session binlog_format='ROW'`); err != nil {
			return err
		}
		if err := this.restartReplication(); err != nil {
			return err
		}
		log.Debugf("'ROW' binlog format applied")
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

//
// validateBinlogs checks that binary log configuration is good to go
// binlog_row_image = "full"
// has_binlog
// binlog_format: row
//
func (this *Inspector) validateBinlogs() error {
	query := `select @@global.log_bin, @@global.binlog_format`
	var hasBinaryLogs bool
	if err := this.db.QueryRow(query).Scan(&hasBinaryLogs, &this.migrationContext.OriginalBinlogFormat); err != nil {
		return err
	}
	if !hasBinaryLogs {
		return fmt.Errorf("%s:%d must have binary logs enabled", this.connectionConfig.Key.Hostname, this.connectionConfig.Key.Port)
	}

	// Binlog格式不正确; 是否自动切换呢?
	if this.migrationContext.RequiresBinlogFormatChange() {
		if !this.migrationContext.SwitchToRowBinlogFormat {
			return fmt.Errorf("You must be using ROW binlog format. I can switch it for you, provided --switch-to-rbr and that %s:%d doesn't have replicas", this.connectionConfig.Key.Hostname, this.connectionConfig.Key.Port)
		}

		// Replicas的个数
		query := fmt.Sprintf(`show /* gh-ost */ slave hosts`)
		countReplicas := 0
		err := sqlutils.QueryRowsMap(this.db, query, func(rowMap sqlutils.RowMap) error {
			countReplicas++
			return nil
		})
		if err != nil {
			return err
		}

		// 如果有不少slave, 则不要轻易调整Row format, 其实调整了问题也不大
		if countReplicas > 0 {
			return fmt.Errorf("%s:%d has %s binlog_format, but I'm too scared to change it to ROW because it has replicas. Bailing out", this.connectionConfig.Key.Hostname, this.connectionConfig.Key.Port, this.migrationContext.OriginalBinlogFormat)
		}
		log.Infof("%s:%d has %s binlog_format. I will change it to ROW, and will NOT change it back, even in the event of failure.", this.connectionConfig.Key.Hostname, this.connectionConfig.Key.Port, this.migrationContext.OriginalBinlogFormat)
	}
	query = `select @@global.binlog_row_image`
	if err := this.db.QueryRow(query).Scan(&this.migrationContext.OriginalBinlogRowImage); err != nil {
		// Only as of 5.6. We wish to support 5.5 as well
		this.migrationContext.OriginalBinlogRowImage = "FULL"
	}
	this.migrationContext.OriginalBinlogRowImage = strings.ToUpper(this.migrationContext.OriginalBinlogRowImage)

	log.Infof("binary logs validated on %s:%d", this.connectionConfig.Key.Hostname, this.connectionConfig.Key.Port)
	return nil
}

// validateLogSlaveUpdates checks that binary log log_slave_updates is set. This test is not required when migrating on replica or when migrating directly on master
func (this *Inspector) validateLogSlaveUpdates() error {
	query := `select @@global.log_slave_updates`
	var logSlaveUpdates bool
	if err := this.db.QueryRow(query).Scan(&logSlaveUpdates); err != nil {
		return err
	}

	if logSlaveUpdates {
		log.Infof("log_slave_updates validated on %s:%d", this.connectionConfig.Key.Hostname, this.connectionConfig.Key.Port)
		return nil
	}

	if this.migrationContext.IsTungsten {
		log.Warningf("log_slave_updates not found on %s:%d, but --tungsten provided, so I'm proceeding", this.connectionConfig.Key.Hostname, this.connectionConfig.Key.Port)
		return nil
	}

	if this.migrationContext.TestOnReplica || this.migrationContext.MigrateOnReplica {
		return fmt.Errorf("%s:%d must have log_slave_updates enabled for testing/migrating on replica", this.connectionConfig.Key.Hostname, this.connectionConfig.Key.Port)
	}

	if this.migrationContext.InspectorIsAlsoApplier() {
		log.Warningf("log_slave_updates not found on %s:%d, but executing directly on master, so I'm proceeeding", this.connectionConfig.Key.Hostname, this.connectionConfig.Key.Port)
		return nil
	}

	return fmt.Errorf("%s:%d must have log_slave_updates enabled for executing migration", this.connectionConfig.Key.Hostname, this.connectionConfig.Key.Port)
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
		return log.Errorf("Cannot find table %s.%s!", sql.EscapeName(this.migrationContext.DatabaseName), sql.EscapeName(this.migrationContext.OriginalTableName))
	}
	log.Infof("Table found. Engine=%s", this.migrationContext.TableEngine)
	log.Debugf("Estimated number of rows via STATUS: %d", this.migrationContext.RowsEstimate)
	return nil
}

// validateTableForeignKeys makes sure no foreign keys exist on the migrated table
func (this *Inspector) validateTableForeignKeys(allowChildForeignKeys bool) error {
	if this.migrationContext.SkipForeignKeyChecks {
		log.Warning("--skip-foreign-key-checks provided: will not check for foreign keys")
		return nil
	}
	query := `
		SELECT
			SUM(REFERENCED_TABLE_NAME IS NOT NULL AND TABLE_SCHEMA=? AND TABLE_NAME=?) as num_child_side_fk,
			SUM(REFERENCED_TABLE_NAME IS NOT NULL AND REFERENCED_TABLE_SCHEMA=? AND REFERENCED_TABLE_NAME=?) as num_parent_side_fk
		FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
		WHERE
				REFERENCED_TABLE_NAME IS NOT NULL
				AND ((TABLE_SCHEMA=? AND TABLE_NAME=?)
					OR (REFERENCED_TABLE_SCHEMA=? AND REFERENCED_TABLE_NAME=?)
				)
	`
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
		return log.Errorf("Found %d parent-side foreign keys on %s.%s. Parent-side foreign keys are not supported. Bailing out", numParentForeignKeys, sql.EscapeName(this.migrationContext.DatabaseName), sql.EscapeName(this.migrationContext.OriginalTableName))
	}
	if numChildForeignKeys > 0 {
		if allowChildForeignKeys {
			log.Debugf("Foreign keys found and will be dropped, as per given --discard-foreign-keys flag")
			return nil
		}
		return log.Errorf("Found %d child-side foreign keys on %s.%s. Child-side foreign keys are not supported. Bailing out", numChildForeignKeys, sql.EscapeName(this.migrationContext.DatabaseName), sql.EscapeName(this.migrationContext.OriginalTableName))
	}
	log.Debugf("Validated no foreign keys exist on table")
	return nil
}

// validateTableTriggers makes sure no triggers exist on the migrated table
func (this *Inspector) validateTableTriggers() error {
	query := `
		SELECT COUNT(*) AS num_triggers
			FROM INFORMATION_SCHEMA.TRIGGERS
			WHERE
				TRIGGER_SCHEMA=?
				AND EVENT_OBJECT_TABLE=?
	`
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
		return log.Errorf("Found triggers on %s.%s. Triggers are not supported at this time. Bailing out", sql.EscapeName(this.migrationContext.DatabaseName), sql.EscapeName(this.migrationContext.OriginalTableName))
	}
	log.Debugf("Validated no triggers exist on table")
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
		return log.Errorf("Cannot run EXPLAIN on %s.%s!", sql.EscapeName(this.migrationContext.DatabaseName), sql.EscapeName(this.migrationContext.OriginalTableName))
	}
	log.Infof("Estimated number of rows via EXPLAIN: %d", this.migrationContext.RowsEstimate)
	return nil
}

// CountTableRows counts exact number of rows on the original table
// 目前的这种模式比较适合在从库上执行: select count(*), 主库上设置有max-execution-time, 容易失败
//
func (this *Inspector) CountTableRows() error {
	atomic.StoreInt64(&this.migrationContext.CountingRowsFlag, 1)
	defer atomic.StoreInt64(&this.migrationContext.CountingRowsFlag, 0)

	log.Infof("As instructed, I'm issuing a SELECT COUNT(*) on the table. This may take a while")

	query := fmt.Sprintf(`select /* gh-ost */ count(*) as rows from %s.%s`, sql.EscapeName(this.migrationContext.DatabaseName), sql.EscapeName(this.migrationContext.OriginalTableName))
	var rowsEstimate int64
	if err := this.db.QueryRow(query).Scan(&rowsEstimate); err != nil {
		return err
	}
	atomic.StoreInt64(&this.migrationContext.RowsEstimate, rowsEstimate)
	this.migrationContext.UsedRowsEstimateMethod = base.CountRowsEstimate

	log.Infof("Exact number of rows via COUNT: %d", rowsEstimate)

	return nil
}

// applyColumnTypes
func (this *Inspector) applyColumnTypes(databaseName, tableName string, columnsLists ...*sql.ColumnList) error {
	query := `
		select
				*
			from
				information_schema.columns
			where
				table_schema=?
				and table_name=?
		`
	// 目前为止: *sql.ColumnList 只有Column的Name信息，没有太的meta信息, 因此需要完善补充
	err := sqlutils.QueryRowsMap(this.db, query, func(m sqlutils.RowMap) error {
		columnName := m.GetString("COLUMN_NAME")
		columnType := m.GetString("COLUMN_TYPE")

		// 其他类型如何处理呢？例如：int, varchar.....
		if strings.Contains(columnType, "unsigned") {
			// 将对应的column：ColumnName 设置为unsigned
			for _, columnsList := range columnsLists {
				columnsList.SetUnsigned(columnName)
			}
		}
		if strings.Contains(columnType, "mediumint") {
			for _, columnsList := range columnsLists {
				columnsList.GetColumn(columnName).Type = sql.MediumIntColumnType
			}
		}
		if strings.Contains(columnType, "timestamp") {
			for _, columnsList := range columnsLists {
				columnsList.GetColumn(columnName).Type = sql.TimestampColumnType
			}
		}
		if strings.Contains(columnType, "datetime") {
			for _, columnsList := range columnsLists {
				columnsList.GetColumn(columnName).Type = sql.DateTimeColumnType
			}
		}
		if strings.HasPrefix(columnType, "enum") {
			for _, columnsList := range columnsLists {
				columnsList.GetColumn(columnName).Type = sql.EnumColumnType
			}
		}
		if charset := m.GetString("CHARACTER_SET_NAME"); charset != "" {
			for _, columnsList := range columnsLists {
				columnsList.SetCharset(columnName, charset)
			}
		}
		return nil
	}, databaseName, tableName)
	return err
}

// getCandidateUniqueKeys investigates a table and returns the list of unique keys
// candidate for chunking
func (this *Inspector) getCandidateUniqueKeys(tableName string) (uniqueKeys [](*sql.UniqueKey), err error) {
	query := `
    SELECT
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
    FROM INFORMATION_SCHEMA.COLUMNS INNER JOIN (
      SELECT
        TABLE_SCHEMA,
        TABLE_NAME,
        INDEX_NAME,
        COUNT(*) AS COUNT_COLUMN_IN_INDEX,
        GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX ASC) AS COLUMN_NAMES,
        SUBSTRING_INDEX(GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX ASC), ',', 1) AS FIRST_COLUMN_NAME,
        SUM(NULLABLE='YES') > 0 AS has_nullable
      FROM INFORMATION_SCHEMA.STATISTICS
      WHERE
				NON_UNIQUE=0
				AND TABLE_SCHEMA = ?
      	AND TABLE_NAME = ?
      GROUP BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME
    ) AS UNIQUES
    ON (
      COLUMNS.TABLE_SCHEMA = UNIQUES.TABLE_SCHEMA AND
      COLUMNS.TABLE_NAME = UNIQUES.TABLE_NAME AND
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
      COUNT_COLUMN_IN_INDEX
  `
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
	log.Debugf("Potential unique keys in %+v: %+v", tableName, uniqueKeys)
	return uniqueKeys, nil
}

// getSharedUniqueKeys returns the intersection of two given unique keys,
// testing by list of columns
func (this *Inspector) getSharedUniqueKeys(originalUniqueKeys, ghostUniqueKeys [](*sql.UniqueKey)) (uniqueKeys [](*sql.UniqueKey), err error) {
	// We actually do NOT rely on key name, just on the set of columns. This is because maybe
	// the ALTER is on the name itself...
	for _, originalUniqueKey := range originalUniqueKeys {
		for _, ghostUniqueKey := range ghostUniqueKeys {
			if originalUniqueKey.Columns.EqualsByNames(&ghostUniqueKey.Columns) {
				uniqueKeys = append(uniqueKeys, originalUniqueKey)
			}
		}
	}
	return uniqueKeys, nil
}

// getSharedColumns returns the intersection of two lists of columns in same order as the first list
// 获取两个table的共享的columns
// *sql.ColumnList <==>  *sql.ColumnList
//
func (this *Inspector) getSharedColumns(originalColumns, ghostColumns *sql.ColumnList, columnRenameMap map[string]string) (*sql.ColumnList, *sql.ColumnList) {
	columnsInGhost := make(map[string]bool)
	for _, ghostColumn := range ghostColumns.Names() {
		columnsInGhost[ghostColumn] = true
	}

	// 共享的columns
	// 1. 完全相同，或 renames
	sharedColumnNames := []string{}
	for _, originalColumn := range originalColumns.Names() {
		if columnsInGhost[originalColumn] || columnsInGhost[columnRenameMap[originalColumn]] {
			sharedColumnNames = append(sharedColumnNames, originalColumn)
		}
	}

	// 2. ghost table中的对应的names
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
		select hint, value from %s.%s where hint = ? and id <= 255
		`,
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
	log.Infof("Recursively searching for replication master")
	visitedKeys := mysql.NewInstanceKeyMap()
	return mysql.GetMasterConnectionConfigSafe(this.connectionConfig, visitedKeys, this.migrationContext.AllowedMasterMaster)
}

func (this *Inspector) getReplicationLag() (replicationLag time.Duration, err error) {
	replicationLag, err = mysql.GetReplicationLag(
		this.migrationContext.InspectorConnectionConfig,
	)
	return replicationLag, err
}
