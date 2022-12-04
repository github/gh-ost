package localtests

import (
	"bufio"
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
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

	var output, stderr bytes.Buffer
	cmd := exec.Command(config.GhostBinary, flags...)
	cmd.Stderr = &stderr
	cmd.Stdout = &output

	if strings.TrimSpace(os.Getenv("GITHUB_ACTION")) == "true" {
		go func(reader io.Reader) {
			scanner := bufio.NewScanner(&output)
			fmt.Printf("::group::%s stdout\n", test.Name)
			for scanner.Scan() {
				fmt.Println(scanner.Text())
			}
			fmt.Println("::endgroup::")
		}(&output)
	}

	if err = cmd.Run(); err != nil {
		if isExpectedFailureOutput(&stderr, test.ExpectedFailure) {
			return nil
		}
		log.Printf("[%s] test failed: %+v", test.Name, stderr.String())
	}
	return err
}

/*
func getPrimaryOrUniqueKey(db *sql.DB, database, table string) (string, error) {
	return "id", nil // TODO: fix this
}
*/

// Validate performs a validation of the migration test results.
func (test *Test) Validate(config Config, primary, replica *sql.DB) error {
	if len(test.ValidateColumns) == 0 || len(test.ValidateOrigColumns) == 0 {
		return nil
	}

	/*
		primaryKey, err := getPrimaryOrUniqueKey(replica, testDatabase, testTable)
		if err != nil {
			return err
		}

		var query string
		var maxPrimaryKeyVal interface{}
		if maxPrimaryKeyVal == nil {
			query = fmt.Sprintf("select * from %s.%s limit 10", testDatabase, testTable)
		} else {
			query = fmt.Sprintf("select * from %s.%s where %s > %+v limit 10",
				testDatabase, testTable, primaryKey, maxPrimaryKeyVal,
			)
		}
		var rowMap sqlutils.RowMap
		err = sqlutils.QueryRowsMap(replica, query, func(m sqlutils.RowMap) error {
			for _, col := range test.ValidateColumns {
				if val, found := m[col]; found {
					rowMap[col] = val
				}
			}
		})

		values := make([]interface{}, 0)
		for range test.ValidateOrigColumns {
			var val interface{}
			values = append(values, &val)
		}
		maxPrimaryKeyVal = values[0]

		for rows.Next() {
			if err = rows.Scan(values...); err != nil {
				return err
			}
			for i, value := range values {
				if value == nil {
					continue
				}
				log.Printf("[%s] row value for %q col: %d", test.Name, test.ValidateOrigColumns[i], value)
			}
		}
	*/

	return nil
}
