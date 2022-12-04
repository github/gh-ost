package localtests

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
)

func execSQLFile(config Config, sqlFile string) (stdin, stderr bytes.Buffer, err error) {
	defaultsFile, err := writeMysqlClientDefaultsFile(config)
	if err != nil {
		return stderr, stdin, err
	}
	defer os.Remove(defaultsFile)

	flags := []string{
		fmt.Sprintf("--defaults-file=%s", defaultsFile),
		fmt.Sprintf("--host=%s", PrimaryHost), // TODO: fix this
		fmt.Sprintf("--port=%d", config.Port),
		"--default-character-set=utf8mb4",
		testDatabase,
	}

	f, err := os.Open(sqlFile)
	if err != nil {
		return stderr, stdin, err
	}
	defer f.Close()

	cmd := exec.Command(config.MysqlBinary, flags...)
	cmd.Stdin = io.TeeReader(f, &stdin)
	cmd.Stderr = &stderr
	cmd.Stdout = os.Stdout

	return stdin, stderr, cmd.Run()
}

func flagsSliceContainsAlter(flags []string) bool {
	for _, flag := range flags {
		if strings.HasPrefix(flag, "--alter") || strings.HasPrefix(flag, "-alter") {
			return true
		}
	}
	return false
}

type MysqlInfo struct {
	Host           string
	Port           int64
	Version        string
	VersionComment string
	SQLMode        string
}

func getMysqlHostInfo(db *sql.DB) (*MysqlInfo, error) {
	var info MysqlInfo
	res := db.QueryRow("select @@hostname, @@port, @@version, @@version_comment, @@global.sql_mode")
	if res.Err() != nil {
		return nil, res.Err()
	}
	err := res.Scan(&info.Host, &info.Port, &info.Version, &info.VersionComment, &info.SQLMode)
	return &info, err
}

func isExpectedFailureOutput(output io.Reader, expectedFailure string) bool {
	scanner := bufio.NewScanner(output)
	for scanner.Scan() {
		if !strings.Contains(scanner.Text(), "FATAL") {
			continue
		} else if strings.Contains(scanner.Text(), expectedFailure) {
			return true
		}
	}
	return false
}

func pingAndGetGTIDExecuted(db *sql.DB, timeout time.Duration) (*mysql.UUIDSet, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return nil, err
	}

	row := db.QueryRowContext(ctx, "select @@global.gtid_executed")
	if err := row.Err(); err != nil {
		return nil, err
	}

	var uuidSet string
	if err := row.Scan(&uuidSet); err != nil {
		return nil, err
	} else if uuidSet == "" {
		return nil, errors.New("gtid_executed is undefined")
	}
	return mysql.ParseUUIDSet(uuidSet)
}

func readTestFile(file string) (string, error) {
	bytes, err := ioutil.ReadFile(file)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(bytes)), nil
}

func setDBGlobalSqlMode(db *sql.DB, sqlMode string) (err error) {
	_, err = db.Exec("set @@global.sql_mode=?", sqlMode)
	return err
}

func writeMysqlClientDefaultsFile(config Config) (string, error) {
	defaultsFile, err := os.CreateTemp("", "gh-ost-localtests.my.cnf")
	if err != nil {
		return "", err
	}
	defer defaultsFile.Close()

	_, err = defaultsFile.Write([]byte(fmt.Sprintf(
		"[client]\nuser=%s\npassword=%s\n",
		config.Username, config.Password,
	)))
	return defaultsFile.Name(), err
}
