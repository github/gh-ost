/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-osc/blob/master/LICENSE
*/

package logic

import (
	gosql "database/sql"
	"fmt"
	"strings"

	"github.com/github/gh-osc/go/base"
	"github.com/github/gh-osc/go/mysql"
	"github.com/github/gh-osc/go/sql"

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
	if err := this.validateGrants(); err != nil {
		return err
	}
	if err := this.validateBinlogs(); err != nil {
		return err
	}
	if err := this.validateTable(); err != nil {
		return err
	}
	if err := this.validateTableForeignKeys(); err != nil {
		return err
	}
	if this.migrationContext.CountTableRows {
		if err := this.countTableRows(); err != nil {
			return err
		}
	} else {
		if err := this.estimateTableRowsViaExplain(); err != nil {
			return err
		}
	}
	return nil
}

func (this *Inspector) InspectOriginalTable() (uniqueKeys [](*sql.UniqueKey), err error) {
	uniqueKeys, err = this.getCandidateUniqueKeys(this.migrationContext.OriginalTableName)
	if err != nil {
		return uniqueKeys, err
	}
	if len(uniqueKeys) == 0 {
		return uniqueKeys, fmt.Errorf("No PRIMARY nor UNIQUE key found in table! Bailing out")
	}
	return uniqueKeys, err
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
	query := `show /* gh-osc */ grants for current_user()`
	foundAll := false
	foundSuper := false
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
			if strings.Contains(grant, `REPLICATION SLAVE`) && strings.Contains(grant, ` ON *.*`) {
				foundReplicationSlave = true
			}
			if strings.Contains(grant, fmt.Sprintf("GRANT ALL PRIVILEGES ON `%s`.*", this.migrationContext.DatabaseName)) {
				foundDBAll = true
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	if foundAll {
		log.Infof("User has ALL privileges")
		return nil
	}
	if foundSuper && foundReplicationSlave && foundDBAll {
		log.Infof("User has SUPER, REPLICATION SLAVE privileges, and has ALL privileges on `%s`", this.migrationContext.DatabaseName)
		return nil
	}
	return log.Errorf("User has insufficient privileges for migration.")
}

// validateBinlogs checks that binary log configuration is good to go
func (this *Inspector) validateBinlogs() error {
	query := `select @@global.log_bin, @@global.log_slave_updates, @@global.binlog_format`
	var hasBinaryLogs, logSlaveUpdates bool
	if err := this.db.QueryRow(query).Scan(&hasBinaryLogs, &logSlaveUpdates, &this.migrationContext.OriginalBinlogFormat); err != nil {
		return err
	}
	if !hasBinaryLogs {
		return fmt.Errorf("%s:%d must have binary logs enabled", this.connectionConfig.Key.Hostname, this.connectionConfig.Key.Port)
	}
	if !logSlaveUpdates {
		return fmt.Errorf("%s:%d must have log_slave_updates enabled", this.connectionConfig.Key.Hostname, this.connectionConfig.Key.Port)
	}
	if this.migrationContext.RequiresBinlogFormatChange() {
		query := fmt.Sprintf(`show /* gh-osc */ slave hosts`)
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
		log.Infof("%s:%d has %s binlog_format. I will change it to ROW for the duration of this migration.", this.connectionConfig.Key.Hostname, this.connectionConfig.Key.Port, this.migrationContext.OriginalBinlogFormat)
	}
	query = `select @@global.binlog_row_image`
	if err := this.db.QueryRow(query).Scan(&this.migrationContext.OriginalBinlogRowImage); err != nil {
		// Only as of 5.6. We wish to support 5.5 as well
		this.migrationContext.OriginalBinlogRowImage = ""
	}

	log.Infof("binary logs validated on %s:%d", this.connectionConfig.Key.Hostname, this.connectionConfig.Key.Port)
	return nil
}

// validateTable makes sure the table we need to operate on actually exists
func (this *Inspector) validateTable() error {
	query := fmt.Sprintf(`show /* gh-osc */ table status from %s like '%s'`, sql.EscapeName(this.migrationContext.DatabaseName), this.migrationContext.OriginalTableName)

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

func (this *Inspector) estimateTableRowsViaExplain() error {
	query := fmt.Sprintf(`explain select /* gh-osc */ * from %s.%s where 1=1`, sql.EscapeName(this.migrationContext.DatabaseName), sql.EscapeName(this.migrationContext.OriginalTableName))

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

func (this *Inspector) countTableRows() error {
	log.Infof("As instructed, I'm issuing a SELECT COUNT(*) on the table. This may take a while")
	query := fmt.Sprintf(`select /* gh-osc */ count(*) as rows from %s.%s`, sql.EscapeName(this.migrationContext.DatabaseName), sql.EscapeName(this.migrationContext.OriginalTableName))
	if err := this.db.QueryRow(query).Scan(&this.migrationContext.RowsEstimate); err != nil {
		return err
	}
	this.migrationContext.UsedRowsEstimateMethod = base.CountRowsEstimate
	log.Infof("Exact number of rows via COUNT: %d", this.migrationContext.RowsEstimate)
	return nil
}

func (this *Inspector) getTableColumns(databaseName, tableName string) (columns sql.ColumnList, err error) {
	query := fmt.Sprintf(`
		show columns from %s.%s
		`,
		sql.EscapeName(databaseName),
		sql.EscapeName(tableName),
	)
	err = sqlutils.QueryRowsMap(this.db, query, func(rowMap sqlutils.RowMap) error {
		columns = append(columns, rowMap.GetString("Field"))
		return nil
	})
	if err != nil {
		return columns, err
	}
	if len(columns) == 0 {
		return columns, log.Errorf("Found 0 columns on %s.%s. Bailing out",
			sql.EscapeName(databaseName),
			sql.EscapeName(tableName),
		)
	}
	return columns, nil
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
      WHERE NON_UNIQUE=0
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
	err = sqlutils.QueryRowsMap(this.db, query, func(rowMap sqlutils.RowMap) error {
		uniqueKey := &sql.UniqueKey{
			Name:        rowMap.GetString("INDEX_NAME"),
			Columns:     *sql.ParseColumnList(rowMap.GetString("COLUMN_NAMES")),
			HasNullable: rowMap.GetBool("has_nullable"),
		}
		uniqueKeys = append(uniqueKeys, uniqueKey)
		return nil
	}, this.migrationContext.DatabaseName, tableName)
	if err != nil {
		return uniqueKeys, err
	}
	log.Debugf("Potential unique keys: %+v", uniqueKeys)
	return uniqueKeys, nil
}

// getCandidateUniqueKeys investigates a table and returns the list of unique keys
// candidate for chunking
func (this *Inspector) getSharedUniqueKeys() (uniqueKeys [](*sql.UniqueKey), err error) {
	originalUniqueKeys, err := this.getCandidateUniqueKeys(this.migrationContext.OriginalTableName)
	if err != nil {
		return uniqueKeys, err
	}
	ghostUniqueKeys, err := this.getCandidateUniqueKeys(this.migrationContext.GetGhostTableName())
	if err != nil {
		return uniqueKeys, err
	}
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

func (this *Inspector) getMasterConnectionConfig() (masterConfig *mysql.ConnectionConfig, err error) {
	visitedKeys := mysql.NewInstanceKeyMap()
	return getMasterConnectionConfigSafe(this.connectionConfig, this.migrationContext.DatabaseName, visitedKeys)
}

func getMasterConnectionConfigSafe(connectionConfig *mysql.ConnectionConfig, databaseName string, visitedKeys *mysql.InstanceKeyMap) (masterConfig *mysql.ConnectionConfig, err error) {
	log.Debugf("Looking for master on %+v", connectionConfig.Key)

	currentUri := connectionConfig.GetDBUri(databaseName)
	db, _, err := sqlutils.GetDB(currentUri)
	if err != nil {
		return nil, err
	}

	hasMaster := false
	masterConfig = connectionConfig.Duplicate()
	err = sqlutils.QueryRowsMap(db, `show slave status`, func(rowMap sqlutils.RowMap) error {
		masterKey := mysql.InstanceKey{
			Hostname: rowMap.GetString("Master_Host"),
			Port:     rowMap.GetInt("Master_Port"),
		}
		if masterKey.IsValid() {
			masterConfig.Key = masterKey
			hasMaster = true
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if hasMaster {
		log.Debugf("Master of %+v is %+v", connectionConfig.Key, masterConfig.Key)
		if visitedKeys.HasKey(masterConfig.Key) {
			return nil, fmt.Errorf("There seems to be a master-master setup at %+v. This is unsupported. Bailing out", masterConfig.Key)
		}
		visitedKeys.AddKey(masterConfig.Key)
		return getMasterConnectionConfigSafe(masterConfig, databaseName, visitedKeys)
	}
	return masterConfig, nil
}
