/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package mysql

import (
	gosql "database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/github/gh-ost/go/sql"

	"github.com/openark/golib/log"
	"github.com/openark/golib/sqlutils"
)

const (
	MaxTableNameLength           = 64
	MaxReplicationPasswordLength = 32
	MaxDBPoolConnections         = 3
)

type ReplicationLagResult struct {
	Key InstanceKey
	Lag time.Duration
	Err error
}

type Trigger struct {
	Name      string
	Event     string
	Statement string
	Timing    string
}

func NewNoReplicationLagResult() *ReplicationLagResult {
	return &ReplicationLagResult{Lag: 0, Err: nil}
}

func (this *ReplicationLagResult) HasLag() bool {
	return this.Lag > 0
}

// knownDBs is a DB cache by uri
var knownDBs map[string]*gosql.DB = make(map[string]*gosql.DB)
var knownDBsMutex = &sync.Mutex{}

func GetDB(migrationUuid string, mysql_uri string) (db *gosql.DB, exists bool, err error) {
	cacheKey := migrationUuid + ":" + mysql_uri

	knownDBsMutex.Lock()
	defer knownDBsMutex.Unlock()

	if db, exists = knownDBs[cacheKey]; !exists {
		db, err = gosql.Open("mysql", mysql_uri)
		if err != nil {
			return nil, false, err
		}
		db.SetMaxOpenConns(MaxDBPoolConnections)
		db.SetMaxIdleConns(MaxDBPoolConnections)
		knownDBs[cacheKey] = db
	}
	return db, exists, nil
}

// GetReplicationLagFromSlaveStatus returns replication lag for a given db; via SHOW SLAVE STATUS
func GetReplicationLagFromSlaveStatus(dbVersion string, informationSchemaDb *gosql.DB) (replicationLag time.Duration, err error) {
	showReplicaStatusQuery := fmt.Sprintf("show %s", ReplicaTermFor(dbVersion, `slave status`))
	err = sqlutils.QueryRowsMap(informationSchemaDb, showReplicaStatusQuery, func(m sqlutils.RowMap) error {
		ioRunningTerm := ReplicaTermFor(dbVersion, "Slave_IO_Running")
		sqlRunningTerm := ReplicaTermFor(dbVersion, "Slave_SQL_Running")
		slaveIORunning := m.GetString(ioRunningTerm)
		slaveSQLRunning := m.GetString(sqlRunningTerm)
		secondsBehindMaster := m.GetNullInt64(ReplicaTermFor(dbVersion, "Seconds_Behind_Master"))
		if !secondsBehindMaster.Valid {
			return fmt.Errorf("replication not running; %s=%+v, %s=%+v", ioRunningTerm, slaveIORunning, sqlRunningTerm, slaveSQLRunning)
		}
		replicationLag = time.Duration(secondsBehindMaster.Int64) * time.Second
		return nil
	})

	return replicationLag, err
}

func GetMasterKeyFromSlaveStatus(dbVersion string, connectionConfig *ConnectionConfig) (masterKey *InstanceKey, err error) {
	currentUri := connectionConfig.GetDBUri("information_schema")
	// This function is only called once, okay to not have a cached connection pool
	db, err := gosql.Open("mysql", currentUri)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	showReplicaStatusQuery := fmt.Sprintf("show %s", ReplicaTermFor(dbVersion, `slave status`))
	err = sqlutils.QueryRowsMap(db, showReplicaStatusQuery, func(rowMap sqlutils.RowMap) error {
		// We wish to recognize the case where the topology's master actually has replication configuration.
		// This can happen when a DBA issues a `RESET SLAVE` instead of `RESET SLAVE ALL`.

		// An empty log file indicates this is a master:
		if rowMap.GetString(ReplicaTermFor(dbVersion, "Master_Log_File")) == "" {
			return nil
		}

		ioRunningTerm := ReplicaTermFor(dbVersion, "Slave_IO_Running")
		sqlRunningTerm := ReplicaTermFor(dbVersion, "Slave_SQL_Running")
		slaveIORunning := rowMap.GetString(ioRunningTerm)
		slaveSQLRunning := rowMap.GetString(sqlRunningTerm)

		if slaveIORunning != "Yes" || slaveSQLRunning != "Yes" {
			return fmt.Errorf("Replication on %+v is broken: %s: %s, %s: %s. Please make sure replication runs before using gh-ost.",
				connectionConfig.Key,
				ioRunningTerm,
				slaveIORunning,
				sqlRunningTerm,
				slaveSQLRunning,
			)
		}

		masterKey = &InstanceKey{
			Hostname: rowMap.GetString(ReplicaTermFor(dbVersion, "Master_Host")),
			Port:     rowMap.GetInt(ReplicaTermFor(dbVersion, "Master_Port")),
		}
		return nil
	})

	return masterKey, err
}

func GetMasterConnectionConfigSafe(dbVersion string, connectionConfig *ConnectionConfig, visitedKeys *InstanceKeyMap, allowMasterMaster bool) (masterConfig *ConnectionConfig, err error) {
	log.Debugf("Looking for %s on %+v", ReplicaTermFor(dbVersion, "master"), connectionConfig.Key)

	masterKey, err := GetMasterKeyFromSlaveStatus(dbVersion, connectionConfig)
	if err != nil {
		return nil, err
	}
	if masterKey == nil {
		return connectionConfig, nil
	}
	if !masterKey.IsValid() {
		return connectionConfig, nil
	}

	masterConfig = connectionConfig.DuplicateCredentials(*masterKey)
	if err := masterConfig.RegisterTLSConfig(); err != nil {
		return nil, err
	}

	log.Debugf("%s of %+v is %+v", ReplicaTermFor(dbVersion, "master"), connectionConfig.Key, masterConfig.Key)
	if visitedKeys.HasKey(masterConfig.Key) {
		if allowMasterMaster {
			return connectionConfig, nil
		}
		return nil, fmt.Errorf("There seems to be a master-master setup at %+v. This is unsupported. Bailing out", masterConfig.Key)
	}
	visitedKeys.AddKey(masterConfig.Key)
	return GetMasterConnectionConfigSafe(dbVersion, masterConfig, visitedKeys, allowMasterMaster)
}

func GetReplicationBinlogCoordinates(dbVersion string, db *gosql.DB) (readBinlogCoordinates *BinlogCoordinates, executeBinlogCoordinates *BinlogCoordinates, err error) {
	showReplicaStatusQuery := fmt.Sprintf("show %s", ReplicaTermFor(dbVersion, `slave status`))
	err = sqlutils.QueryRowsMap(db, showReplicaStatusQuery, func(m sqlutils.RowMap) error {
		readBinlogCoordinates = &BinlogCoordinates{
			LogFile: m.GetString(ReplicaTermFor(dbVersion, "Master_Log_File")),
			LogPos:  m.GetInt64(ReplicaTermFor(dbVersion, "Read_Master_Log_Pos")),
		}
		executeBinlogCoordinates = &BinlogCoordinates{
			LogFile: m.GetString(ReplicaTermFor(dbVersion, "Relay_Master_Log_File")),
			LogPos:  m.GetInt64(ReplicaTermFor(dbVersion, "Exec_Master_Log_Pos")),
		}
		return nil
	})
	return readBinlogCoordinates, executeBinlogCoordinates, err
}

func GetSelfBinlogCoordinates(dbVersion string, db *gosql.DB) (selfBinlogCoordinates *BinlogCoordinates, err error) {
	binaryLogStatusTerm := ReplicaTermFor(dbVersion, "master status")
	err = sqlutils.QueryRowsMap(db, fmt.Sprintf("show %s", binaryLogStatusTerm), func(m sqlutils.RowMap) error {
		selfBinlogCoordinates = &BinlogCoordinates{
			LogFile: m.GetString("File"),
			LogPos:  m.GetInt64("Position"),
		}
		return nil
	})
	return selfBinlogCoordinates, err
}

// GetInstanceKey reads hostname and port on given DB
func GetInstanceKey(db *gosql.DB) (instanceKey *InstanceKey, err error) {
	instanceKey = &InstanceKey{}
	err = db.QueryRow(`select @@global.hostname, @@global.port`).Scan(&instanceKey.Hostname, &instanceKey.Port)
	return instanceKey, err
}

// GetTableColumns reads column list from given table
func GetTableColumns(db *gosql.DB, databaseName, tableName string) (*sql.ColumnList, *sql.ColumnList, error) {
	query := fmt.Sprintf(`
		show columns from %s.%s
		`,
		sql.EscapeName(databaseName),
		sql.EscapeName(tableName),
	)
	columnNames := []string{}
	virtualColumnNames := []string{}
	err := sqlutils.QueryRowsMap(db, query, func(rowMap sqlutils.RowMap) error {
		columnName := rowMap.GetString("Field")
		columnNames = append(columnNames, columnName)
		if strings.Contains(rowMap.GetString("Extra"), " GENERATED") {
			log.Debugf("%s is a generated column", columnName)
			virtualColumnNames = append(virtualColumnNames, columnName)
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	if len(columnNames) == 0 {
		return nil, nil, log.Errorf("Found 0 columns on %s.%s. Bailing out",
			sql.EscapeName(databaseName),
			sql.EscapeName(tableName),
		)
	}
	return sql.NewColumnList(columnNames), sql.NewColumnList(virtualColumnNames), nil
}

// Kill executes a KILL QUERY by connection id
func Kill(db *gosql.DB, connectionID string) error {
	_, err := db.Exec(`KILL QUERY %s`, connectionID)
	return err
}

// GetTriggers reads trigger list from given table
func GetTriggers(db *gosql.DB, databaseName, tableName string) (triggers []Trigger, err error) {
	query := fmt.Sprintf(`select trigger_name as name, event_manipulation as event, action_statement as statement, action_timing as timing
	from information_schema.triggers 
	where trigger_schema = '%s' and event_object_table = '%s'`, databaseName, tableName)

	err = sqlutils.QueryRowsMap(db, query, func(rowMap sqlutils.RowMap) error {
		triggers = append(triggers, Trigger{
			Name:      rowMap.GetString("name"),
			Event:     rowMap.GetString("event"),
			Statement: rowMap.GetString("statement"),
			Timing:    rowMap.GetString("timing"),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return triggers, nil
}

func versionTokens(version string, digits int) []int {
	v := strings.Split(version, "-")[0]
	tokens := strings.Split(v, ".")
	intTokens := make([]int, digits)
	for i := range tokens {
		if i >= digits {
			break
		}
		intTokens[i], _ = strconv.Atoi(tokens[i])
	}
	return intTokens
}

func isSmallerVersion(version string, otherVersion string, digits int) bool {
	v := versionTokens(version, digits)
	o := versionTokens(otherVersion, digits)
	for i := 0; i < len(v); i++ {
		if v[i] < o[i] {
			return true
		}
		if v[i] > o[i] {
			return false
		}
		if i == digits {
			break
		}
	}
	return false
}

// IsSmallerMajorVersion tests two versions against another and returns true if
// the former is a smaller "major" version than the latter.
// e.g. 5.5.36 is NOT a smaller major version as compared to 5.5.40, but IS as compared to 5.6.9
func IsSmallerMajorVersion(version string, otherVersion string) bool {
	return isSmallerVersion(version, otherVersion, 2)
}

// IsSmallerMinorVersion tests two versions against another and returns true if
// the former is a smaller "minor" version than the latter.
// e.g. 5.5.36 is a smaller major version as compared to 5.5.40, as well as compared to 5.6.7
func IsSmallerMinorVersion(version string, otherVersion string) bool {
	return isSmallerVersion(version, otherVersion, 3)
}
