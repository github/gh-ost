package localtests

import (
	"database/sql"
	"errors"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/shlex"
)

const (
	PrimaryHost              = "primary"
	DefaultHost              = "replica"
	DefaultPort        int64 = 3306
	DefaultUsername          = "gh-ost"
	DefaultPassword          = "gh-ost"
	testDatabase             = "test"
	testTable                = "gh_ost_test"
	testSocketFile           = "/tmp/gh-ost.test.sock"
	testChunkSize      int64 = 10
	testDefaultRetries int64 = 3
	throttleFlagFile         = "/tmp/gh-ost-test.ghost.throttle.flag"
	throttleQuery            = "select timestampdiff(second, min(last_update), now()) < 5 from _gh_ost_test_ghc"
	//
	failedEmoji  = "\u274C"
	successEmoji = "\u2705"
)

// Config represents the configuration.
type Config struct {
	Host          string
	Port          int64
	Username      string
	Password      string
	GhostBinary   string
	MysqlBinary   string
	StorageEngine string
	TestsDir      string
}

type Tester struct {
	config  Config
	primary *sql.DB
	replica *sql.DB
}

func NewTester(config Config, primary, replica *sql.DB) *Tester {
	return &Tester{
		config:  config,
		primary: primary,
		replica: replica,
	}
}

// WaitForMySQLAvailable waits for MySQL to become ready for
// testing on both the primary and replica.
func (t *Tester) WaitForMySQLAvailable() error {
	interval := 2 * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-time.After(5 * time.Minute):
			return errors.New("timed out waiting for mysql")
		case <-ticker.C:
			err := func() error {
				primaryGTIDExec, err := pingAndGetGTIDExecuted(t.primary, interval)
				if err != nil {
					return err
				}
				replicaGTIDExec, err := pingAndGetGTIDExecuted(t.replica, interval)
				if err != nil {
					return err
				}

				if !replicaGTIDExec.Contain(primaryGTIDExec) {
					return errors.New("replica/primary GTID not equal")
				}
				return nil
			}()
			if err != nil {
				log.Printf("Waiting for MySQL primary/replica to init: %+v", err)
				continue
			}

			var info *MysqlInfo
			if info, err = getMysqlHostInfo(t.primary); err == nil {
				log.Printf("MySQL primary %s:%d, version %s (%s) available %s", info.Host, info.Port, info.Version,
					info.VersionComment, successEmoji)
			}
			if info, err = getMysqlHostInfo(t.replica); err == nil {
				log.Printf("MySQL replica %s:%d, version %s (%s) available %s", info.Host, info.Port, info.Version,
					info.VersionComment, successEmoji)
			}

			return err
		}
	}
}

// ReadTests reads test configurations from a directory. As single test isee
// returned when specificTestName is specified.
func (t *Tester) ReadTests(specificTestName string) (tests []Test, err error) {
	subdirs, err := ioutil.ReadDir(t.config.TestsDir)
	if err != nil {
		return tests, err
	}

	for _, subdir := range subdirs {
		test := Test{
			Name: subdir.Name(),
			Path: filepath.Join(t.config.TestsDir, subdir.Name()),
		}

		stat, err := os.Stat(test.Path)
		if err != nil || !stat.IsDir() {
			continue
		}

		if specificTestName != "" && !strings.EqualFold(test.Name, specificTestName) {
			continue
		}

		test.CreateSQLFile = filepath.Join(test.Path, "create.sql")
		if _, err = os.Stat(test.CreateSQLFile); err != nil {
			log.Printf("Failed to find create.sql file %q: %+v", test.CreateSQLFile, err)
			return tests, err
		}

		destroySQLFile := filepath.Join(test.Path, "destroy.sql")
		if _, err = os.Stat(destroySQLFile); err == nil {
			test.DestroySQLFile = destroySQLFile
		}

		expectFailureFile := filepath.Join(test.Path, "expect_failure")
		test.ExpectedFailure, _ = readTestFile(expectFailureFile)

		sqlModeFile := filepath.Join(test.Path, "sql_mode")
		if sqlMode, err := readTestFile(sqlModeFile); err == nil {
			test.SQLMode = &sqlMode
		}

		orderByFile := filepath.Join(test.Path, "order_by")
		test.ValidateOrderBy, _ = readTestFile(orderByFile)

		origColumnsFile := filepath.Join(test.Path, "orig_columns")
		if origColumns, err := readTestFile(origColumnsFile); err == nil {
			origColumns = strings.Replace(origColumns, " ", "", -1)
			test.ValidateOrigColumns = strings.Split(origColumns, ",")
		}

		ghostColumnsFile := filepath.Join(test.Path, "ghost_columns")
		if ghostColumns, err := readTestFile(ghostColumnsFile); err == nil {
			ghostColumns = strings.Replace(ghostColumns, " ", "", -1)
			test.ValidateColumns = strings.Split(ghostColumns, ",")
		}

		extraArgsFile := filepath.Join(test.Path, "extra_args")
		if _, err = os.Stat(extraArgsFile); err == nil {
			extraArgsStr, err := readTestFile(extraArgsFile)
			if err != nil {
				log.Printf("Failed to read extra_args file %q: %+v", extraArgsFile, err)
				return tests, err
			}
			if test.ExtraArgs, err = shlex.Split(extraArgsStr); err != nil {
				log.Printf("Failed to read extra_args file %q: %+v", extraArgsFile, err)
				return tests, err
			}
		}

		tests = append(tests, test)
	}

	return tests, err
}

// RunTest prepares and runs a single test.
func (t *Tester) RunTest(test Test) (err error) {
	if err = test.Prepare(t.config, t.primary); err != nil {
		return err
	}
	log.Printf("[%s] prepared test %s", test.Name, successEmoji)

	if err = test.Migrate(t.config, t.primary, t.replica); err != nil {
		log.Printf("[%s] failed to migrate test %s%s%s", test.Name, failedEmoji,
			failedEmoji, failedEmoji)
		return err
	}
	log.Printf("[%s] successfully migrated test %s", test.Name, successEmoji)

	if err = test.Validate(t.config, t.primary, t.replica); err != nil {
		log.Printf("[%s] failed to validate test %s%s%s", test.Name, failedEmoji,
			failedEmoji, failedEmoji)
		return err
	}
	log.Printf("[%s] successfully validated test %s", test.Name, successEmoji)

	return err
}
