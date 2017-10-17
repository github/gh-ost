/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package base

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	gosql "database/sql"
	"github.com/github/gh-ost/go/mysql"
	"github.com/outbrain/golib/log"
)

var (
	prettifyDurationRegexp = regexp.MustCompile("([.][0-9]+)")
)

func PrettifyDurationOutput(d time.Duration) string {
	if d < time.Second {
		return "0s"
	}
	result := fmt.Sprintf("%s", d)
	result = prettifyDurationRegexp.ReplaceAllString(result, "")
	return result
}

func FileExists(fileName string) bool {
	if _, err := os.Stat(fileName); err == nil {
		return true
	}
	return false
}

// StringContainsAll returns true if `s` contains all non empty given `substrings`
// The function returns `false` if no non-empty arguments are given.
func StringContainsAll(s string, substrings ...string) bool {
	nonEmptyStringsFound := false
	for _, substring := range substrings {
		if substring == "" {
			continue
		}
		if strings.Contains(s, substring) {
			nonEmptyStringsFound = true
		} else {
			// Immediate failure
			return false
		}
	}
	return nonEmptyStringsFound
}

func ValidateConnection(db *gosql.DB, connectionConfig *mysql.ConnectionConfig) (string, error) {
	query := `select @@global.port, @@global.version`
	var port, extraPort int
	var version string
	if err := db.QueryRow(query).Scan(&port, &version); err != nil {
		return "", err
	}
	extraPortQuery := `select @@global.extra_port`
	if err := db.QueryRow(extraPortQuery).Scan(&extraPort); err != nil {
		// swallow this error. not all servers support extra_port
	}

	if connectionConfig.Key.Port == port || (extraPort > 0 && connectionConfig.Key.Port == extraPort) {
		log.Infof("connection validated on %+v", connectionConfig.Key)
		return version, nil
	} else if extraPort == 0 {
		return "", fmt.Errorf("Unexpected database port reported: %+v", port)
	} else {
		return "", fmt.Errorf("Unexpected database port reported: %+v / extra_port: %+v", port, extraPort)
	}
}
