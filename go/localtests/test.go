package localtests

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"

	"github.com/openark/golib/sqlutils"
)

// Test represents a single test.
type Test struct {
	Name                string
	Path                string
	CreateSQLFile       string
	DestroySQLFile      string
	ExtraArgs           []string
	ExpectedFailure     string
	SQLMode             *string
	IgnoreVersions      []string
	ValidateOrderBy     string
	ValidateColumns     []string
	ValidateOrigColumns []string
	//
	origPrimaryInfo *MysqlInfo
}

func (test *Test) prepareDBPrimary(primary *sql.DB) (err error) {
	test.origPrimaryInfo, err = getMysqlHostInfo(primary)
	if err != nil {
		return err
	}

	if test.SQLMode != nil {
		if err = setDBGlobalSqlMode(primary, *test.SQLMode); err != nil {
			return err
		}
		log.Printf("[%s] sql_mode set to %q on primary", test.Name, *test.SQLMode)
	}

	return err
}

func (test *Test) resetDBPrimary(config Config, primary *sql.DB) {
	if test.SQLMode != nil && test.origPrimaryInfo != nil {
		log.Printf("[%s] resetting primary to sql_mode: %s", test.Name, test.origPrimaryInfo.SQLMode)
		if err := setDBGlobalSqlMode(primary, test.origPrimaryInfo.SQLMode); err != nil {
			log.Printf("[%s] failed to reset primary to sql_mode: %+v", test.Name, err)
		}
	}

	if test.DestroySQLFile != "" {
		log.Printf("[%s] running destroy.sql file", test.Name)
		stdin, stderr, err := execSQLFile(config, test.DestroySQLFile)
		if err != nil {
			log.Printf("[%s] failed to destroy test schema %s%s%s: %+v", test.Name, failedEmoji, failedEmoji, failedEmoji, stderr.String())
			log.Printf("[%s] destroy.sql: %s", test.Name, stdin.String())
		}
	}
}

// Prepare inits a test and runs a 'mysql' client/shell command to populate
// the test schema. The create.sql file is read by golang and passed to
// 'mysql' over stdin.
func (test *Test) Prepare(config Config, primary *sql.DB) error {
	if test.CreateSQLFile == "" {
		return nil
	}

	if err := test.prepareDBPrimary(primary); err != nil {
		return err
	}

	stdin, stderr, err := execSQLFile(config, test.CreateSQLFile)
	if err != nil {
		log.Printf("[%s] failed to prepare test schema %s%s%s: %+v", test.Name, failedEmoji, failedEmoji, failedEmoji, stderr.String())
		log.Printf("[%s] create.sql: %s", test.Name, stdin.String())
	}
	return err
}

// Migrate runs the migration test.
func (test *Test) Migrate(config Config, primary, replica *sql.DB) (err error) {
	defer test.resetDBPrimary(config, primary)

	replicaInfo, err := getMysqlHostInfo(replica)
	if err != nil {
		return err
	}

	flags := []string{
		fmt.Sprintf("--user=%s", config.Username),
		fmt.Sprintf("--password=%s", config.Password),
		fmt.Sprintf("--host=%s", config.Host),
		fmt.Sprintf("--port=%d", config.Port),
		fmt.Sprintf("--assume-master-host=%s:%d", PrimaryHost, replicaInfo.Port), // TODO: fix this
		fmt.Sprintf("--database=%s", testDatabase),
		fmt.Sprintf("--table=%s", testTable),
		fmt.Sprintf("--chunk-size=%d", testChunkSize),
		fmt.Sprintf("--default-retries=%d", testDefaultRetries),
		fmt.Sprintf("--throttle-query=%s", throttleQuery),
		fmt.Sprintf("--throttle-flag-file=%s", throttleFlagFile),
		fmt.Sprintf("--serve-socket-file=%s", testSocketFile),
		fmt.Sprintf("--storage-engine=%s", config.StorageEngine),
		"--allow-on-master",
		"--assume-rbr",
		"--debug",
		"--exact-rowcount",
		"--execute",
		"--initially-drop-old-table",
		"--initially-drop-ghost-table",
		"--initially-drop-socket-file",
		"--stack",
		"--verbose",
	}
	if !flagsSliceContainsAlter(test.ExtraArgs) {
		test.ExtraArgs = append(test.ExtraArgs, fmt.Sprintf(`--alter=ENGINE=%s`, config.StorageEngine))
	}
	if len(test.ExtraArgs) > 0 {
		flags = append(flags, test.ExtraArgs...)
	}

	log.Printf("[%s] running gh-ost command with extra args: %+v", test.Name, test.ExtraArgs)

	var stderr bytes.Buffer
	cmd := exec.Command(config.GhostBinary, flags...)
	cmd.Stderr = io.MultiWriter(os.Stderr, &stderr)
	cmd.Stdout = os.Stdout

	if isGitHubActions() {
		fmt.Printf("::group::%s gh-ost output\n", test.Name)
	}

	err = cmd.Run()

	if isGitHubActions() {
		fmt.Println("::endgroup::")
	}

	if err != nil {
		if isExpectedFailureOutput(&stderr, test.ExpectedFailure) {
			log.Printf("[%s] test got expected failure: %s", test.ExpectedFailure)
			return nil
		}
		log.Printf("[%s] test failed: %+v", test.Name, stderr.String())
	}
	return err
}

func getTablePrimaryKey(db *sql.DB, database, table string) (string, error) {
	return "id", nil // TODO: fix this
}

type validationResult struct {
	Source string
	Rows   sqlutils.RowMap
}

// Validate performs a validation of the migration test results.
func (test *Test) Validate(config Config, db *sql.DB) error {
	if len(test.ValidateColumns) == 0 || len(test.ValidateOrigColumns) == 0 {
		return nil
	}

	primaryKey, err := getTablePrimaryKey(db, testDatabase, testTable)
	if err != nil {
		return err
	}

	limit := 10
	orderBy := primaryKey
	if test.ValidateOrderBy != "" {
		orderBy = test.ValidateOrderBy
	}

	outChan := make(chan validationResult, 2)
	getTableMap := func(wg *sync.WaitGroup, database, table string, columns []string, outChan chan validationResult) {
		defer wg.Done()

		query := fmt.Sprintf("select %s from %s.%s order by %s limit %d",
			strings.Join(columns, ", "),
			database, table,
			orderBy, limit,
		)
		err := sqlutils.QueryRowsMap(db, query, func(m sqlutils.RowMap) error {
			outChan <- validationResult{
				Source: table,
				Rows:   m,
			}
			return nil
		})
		if err != nil {
			log.Printf("[%s] failed to validate table %s: %+v", test.Name, table, err)
		}
	}

	var wg sync.WaitGroup
	go getTableMap(&wg, testDatabase, testTable, test.ValidateColumns, outChan)
	go getTableMap(&wg, testDatabase, fmt.Sprintf("_%s_del", testTable), test.ValidateOrigColumns, outChan)
	wg.Add(2)
	wg.Wait()

	for result := range outChan {
		log.Printf("[%s] result for %s: %+v", test.Name, result.Source, result.Rows)
	}
	return nil
}
