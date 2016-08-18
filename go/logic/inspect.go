/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	gosql "database/sql"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"

	"github.com/outbrain/golib/log"
	"github.com/outbrain/golib/sqlutils"
)

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
	if err := this.validateGrants(); err != nil {
		return err
	}
	if err := this.validateBinlogs(); err != nil {
		return err
	}
	if err := this.applyBinlogFormat(); err != nil {
		return err
	}
	return nil
}

func (this *Inspector) ValidateOriginalTable() (err error) {
	if err := this.validateTable(); err != nil {
		return err
	}
	if err := this.validateTableForeignKeys(); err != nil {
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
	columns, err = this.getTableColumns(this.migrationContext.DatabaseName, tableName)
	if err != nil {
		return columns, uniqueKeys, err
	}

	return columns, uniqueKeys, nil
}

func (this *Inspector) InspectOriginalTable() (err error) {
	this.migrationContext.OriginalTableColumns, this.migrationContext.OriginalTableUniqueKeys, err = this.InspectTableColumnsAndUniqueKeys(this.migrationContext.OriginalTableName)
	if err == nil {
		return err
	}
	return nil
}

// InspectOriginalAndGhostTables compares original and ghost tables to see whether the migration
// makes sense and is valid. It extracts the list of shared columns and the chosen migration unique key
func (this *Inspector) InspectOriginalAndGhostTables() (err error) {
	this.migrationContext.GhostTableColumns, this.migrationContext.GhostTableUniqueKeys, err = this.InspectTableColumnsAndUniqueKeys(this.migrationContext.GetGhostTableName())
	if err != nil {
		return err
	}
	sharedUniqueKeys, err := this.getSharedUniqueKeys(this.migrationContext.OriginalTableUniqueKeys, this.migrationContext.GhostTableUniqueKeys)
	if err != nil {
		return err
	}
	if len(sharedUniqueKeys) == 0 {
		return fmt.Errorf("No shared unique key can be found after ALTER! Bailing out")
	}
	this.migrationContext.UniqueKey = sharedUniqueKeys[0]
	log.Infof("Chosen shared unique key is %s", this.migrationContext.UniqueKey.Name)
	if this.migrationContext.UniqueKey.HasNullable {
		if this.migrationContext.NullableUniqueKeyAllowed {
			log.Warningf("Chosen key (%s) has nullable columns. You have supplied with --allow-nullable-unique-key and so this migration proceeds. As long as there aren't NULL values in this key's column, migration should be fine. NULL values will corrupt migration's data", this.migrationContext.UniqueKey)
		} else {
			return fmt.Errorf("Chosen key (%s) has nullable columns. Bailing out. To force this operation to continue, supply --allow-nullable-unique-key flag. Only do so if you are certain there are no actual NULL values in this key. As long as there aren't, migration should be fine. NULL values in columns of this key will corrupt migration's data", this.migrationContext.UniqueKey)
		}
	}
	if !this.migrationContext.UniqueKey.IsPrimary() {
		if this.migrationContext.OriginalBinlogRowImage != "FULL" {
			return fmt.Errorf("binlog_row_image is '%s' and chosen key is %s, which is not the primary key. This operation cannot proceed. You may `set global binlog_row_image='full'` and try again", this.migrationContext.OriginalBinlogRowImage, this.migrationContext.UniqueKey)
		}
	}

	this.migrationContext.SharedColumns, this.migrationContext.MappedSharedColumns = this.getSharedColumns(this.migrationContext.OriginalTableColumns, this.migrationContext.GhostTableColumns, this.migrationContext.ColumnRenameMap)
	log.Infof("Shared columns are %s", this.migrationContext.SharedColumns)
	// By fact that a non-empty unique key exists we also know the shared columns are non-empty

	// This additional step looks at which columns are unsigned. We could have merged this within
	// the `getTableColumns()` function, but it's a later patch and introduces some complexity; I feel
	// comfortable in doing this as a separate step.
	this.applyUnsignedColumns(this.migrationContext.DatabaseName, this.migrationContext.OriginalTableName, this.migrationContext.OriginalTableColumns, this.migrationContext.SharedColumns)
	this.applyUnsignedColumns(this.migrationContext.DatabaseName, this.migrationContext.GetGhostTableName(), this.migrationContext.GhostTableColumns, this.migrationContext.MappedSharedColumns)

	return nil
}

// validateConnection issues a simple can-connect to MySQL
func (this *Inspector) validateConnection() error {
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

// validateGrants verifies the user by which we're executing has necessary grants
// to do its thang.
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
	return log.Errorf("User has insufficient privileges for migration. Needed: SUPER, REPLICATION SLAVE and ALL on %s.*", sql.EscapeName(this.migrationContext.DatabaseName))
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

	var stopError, startError error
	_, stopError = sqlutils.ExecNoPrepare(this.db, `stop slave`)
	_, startError = sqlutils.ExecNoPrepare(this.db, `start slave`)
	if stopError != nil {
		return stopError
	}
	if startError != nil {
		return startError
	}
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

// validateBinlogs checks that binary log configuration is good to go
func (this *Inspector) validateBinlogs() error {
	query := `select @@global.log_bin, @@global.binlog_format`
	var hasBinaryLogs bool
	if err := this.db.QueryRow(query).Scan(&hasBinaryLogs, &this.migrationContext.OriginalBinlogFormat); err != nil {
		return err
	}
	if !hasBinaryLogs {
		return fmt.Errorf("%s:%d must have binary logs enabled", this.connectionConfig.Key.Hostname, this.connectionConfig.Key.Port)
	}
	if this.migrationContext.RequiresBinlogFormatChange() {
		if !this.migrationContext.SwitchToRowBinlogFormat {
			return fmt.Errorf("You must be using ROW binlog format. I can switch it for you, provided --switch-to-rbr and that %s:%d doesn't have replicas", this.connectionConfig.Key.Hostname, this.connectionConfig.Key.Port)
		}
		query := fmt.Sprintf(`show /* gh-ost */ slave hosts`)
		countReplicas := 0
		err := sqlutils.QueryRowsMap(this.db, query, func(rowMap sqlutils.RowMap) error {
			countReplicas++
			return nil
		})
		if err != nil {
			return err
		}
		if countReplicas > 0 {
			return fmt.Errorf("%s:%d has %s binlog_format, but I'm too scared to change it to ROW because it has replicas. Bailing out", this.connectionConfig.Key.Hostname, this.connectionConfig.Key.Port, this.migrationContext.OriginalBinlogFormat)
		}
		log.Infof("%s:%d has %s binlog_format. I will change it to ROW, and will NOT change it back, even in the event of failure.", this.connectionConfig.Key.Hostname, this.connectionConfig.Key.Port, this.migrationContext.OriginalBinlogFormat)
	}
	query = `select @@global.binlog_row_image`
	if err := this.db.QueryRow(query).Scan(&this.migrationContext.OriginalBinlogRowImage); err != nil {
		// Only as of 5.6. We wish to support 5.5 as well
		this.migrationContext.OriginalBinlogRowImage = ""
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
	if !logSlaveUpdates && !this.migrationContext.InspectorIsAlsoApplier() {
		return fmt.Errorf("%s:%d must have log_slave_updates enabled", this.connectionConfig.Key.Hostname, this.connectionConfig.Key.Port)
	}

	log.Infof("binary logs updates validated on %s:%d", this.connectionConfig.Key.Hostname, this.connectionConfig.Key.Port)
	return nil
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
func (this *Inspector) validateTableForeignKeys() error {
	query := `
		SELECT COUNT(*) AS num_foreign_keys
		FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
		WHERE
				REFERENCED_TABLE_NAME IS NOT NULL
				AND ((TABLE_SCHEMA=? AND TABLE_NAME=?)
					OR (REFERENCED_TABLE_SCHEMA=? AND REFERENCED_TABLE_NAME=?)
				)
	`
	numForeignKeys := 0
	err := sqlutils.QueryRowsMap(this.db, query, func(rowMap sqlutils.RowMap) error {
		numForeignKeys = rowMap.GetInt("num_foreign_keys")

		return nil
	},
		this.migrationContext.DatabaseName,
		this.migrationContext.OriginalTableName,
		this.migrationContext.DatabaseName,
		this.migrationContext.OriginalTableName,
	)
	if err != nil {
		return err
	}
	if numForeignKeys > 0 {
		return log.Errorf("Found %d foreign keys on %s.%s. Foreign keys are not supported. Bailing out", numForeignKeys, sql.EscapeName(this.migrationContext.DatabaseName), sql.EscapeName(this.migrationContext.OriginalTableName))
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
func (this *Inspector) CountTableRows() error {
	atomic.StoreInt64(&this.migrationContext.CountingRowsFlag, 1)
	defer atomic.StoreInt64(&this.migrationContext.CountingRowsFlag, 0)

	log.Infof("As instructed, I'm issuing a SELECT COUNT(*) on the table. This may take a while")

	query := fmt.Sprintf(`select /* gh-ost */ count(*) as rows from %s.%s`, sql.EscapeName(this.migrationContext.DatabaseName), sql.EscapeName(this.migrationContext.OriginalTableName))
	if err := this.db.QueryRow(query).Scan(&this.migrationContext.RowsEstimate); err != nil {
		return err
	}
	this.migrationContext.UsedRowsEstimateMethod = base.CountRowsEstimate

	log.Infof("Exact number of rows via COUNT: %d", this.migrationContext.RowsEstimate)

	return nil
}

// getTableColumns reads column list from given table
func (this *Inspector) getTableColumns(databaseName, tableName string) (*sql.ColumnList, error) {
	query := fmt.Sprintf(`
		show columns from %s.%s
		`,
		sql.EscapeName(databaseName),
		sql.EscapeName(tableName),
	)
	columnNames := []string{}
	err := sqlutils.QueryRowsMap(this.db, query, func(rowMap sqlutils.RowMap) error {
		columnNames = append(columnNames, rowMap.GetString("Field"))
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(columnNames) == 0 {
		return nil, log.Errorf("Found 0 columns on %s.%s. Bailing out",
			sql.EscapeName(databaseName),
			sql.EscapeName(tableName),
		)
	}
	return sql.NewColumnList(columnNames), nil
}

// applyUnsignedColumns
func (this *Inspector) applyUnsignedColumns(databaseName, tableName string, columnsLists ...*sql.ColumnList) error {
	query := fmt.Sprintf(`
		show columns from %s.%s
		`,
		sql.EscapeName(databaseName),
		sql.EscapeName(tableName),
	)
	err := sqlutils.QueryRowsMap(this.db, query, func(rowMap sqlutils.RowMap) error {
		columnName := rowMap.GetString("Field")
		if strings.Contains(rowMap.GetString("Type"), "unsigned") {
			for _, columnsList := range columnsLists {
				columnsList.SetUnsigned(columnName)
			}
		}
		return nil
	})
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
			if originalUniqueKey.Columns.Equals(&ghostUniqueKey.Columns) {
				uniqueKeys = append(uniqueKeys, originalUniqueKey)
			}
		}
	}
	return uniqueKeys, nil
}

// getSharedColumns returns the intersection of two lists of columns in same order as the first list
func (this *Inspector) getSharedColumns(originalColumns, ghostColumns *sql.ColumnList, columnRenameMap map[string]string) (*sql.ColumnList, *sql.ColumnList) {
	columnsInGhost := make(map[string]bool)
	for _, ghostColumn := range ghostColumns.Names {
		columnsInGhost[ghostColumn] = true
	}
	sharedColumnNames := []string{}
	for _, originalColumn := range originalColumns.Names {
		if columnsInGhost[originalColumn] || columnsInGhost[columnRenameMap[originalColumn]] {
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
func (this *Inspector) readChangelogState() (map[string]string, error) {
	query := fmt.Sprintf(`
		select hint, value from %s.%s where id <= 255
		`,
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetChangelogTableName()),
	)
	result := make(map[string]string)
	err := sqlutils.QueryRowsMap(this.db, query, func(m sqlutils.RowMap) error {
		result[m.GetString("hint")] = m.GetString("value")
		return nil
	})
	return result, err
}

func (this *Inspector) getMasterConnectionConfig() (applierConfig *mysql.ConnectionConfig, err error) {
	visitedKeys := mysql.NewInstanceKeyMap()
	return mysql.GetMasterConnectionConfigSafe(this.connectionConfig, visitedKeys, this.migrationContext.AllowedMasterMaster)
}
