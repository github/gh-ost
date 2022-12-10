package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"

	_ "github.com/go-sql-driver/mysql"

	"github.com/github/gh-ost/go/localtests"
)

var AppVersion string

func envStringVarOrDefault(envVar, defaultVal string) string {
	if val := os.Getenv(envVar); val != "" {
		return val
	}
	return defaultVal
}

func main() {
	// flags
	var printVersion, testNoop bool
	var testName string
	var cnf localtests.Config
	flag.StringVar(&cnf.Host, "host", localtests.DefaultHost, "mysql host")
	flag.Int64Var(&cnf.Port, "port", localtests.DefaultPort, "mysql port")
	flag.StringVar(&cnf.Username, "username", localtests.DefaultUsername, "mysql username")
	flag.StringVar(&cnf.Password, "password", localtests.DefaultPassword, "mysql password")
	flag.StringVar(&cnf.TestsDir, "tests-dir", "/etc/localtests", "path to localtests directory")
	flag.StringVar(&testName, "test", "", "run a single test by name (default: run all tests)")
	flag.BoolVar(&testNoop, "test-noop", false, "run a single noop migration, eg: --alter='ENGINE=InnoDB'")
	flag.StringVar(&cnf.StorageEngine, "storage-engine", envStringVarOrDefault("TEST_STORAGE_ENGINE", "innodb"), "mysql storage engine")
	flag.StringVar(&cnf.GhostBinary, "binary", "gh-ost", "path to gh-ost binary")
	flag.StringVar(&cnf.MysqlBinary, "mysql-binary", "mysql", "path to mysql binary")
	flag.BoolVar(&printVersion, "version", false, "print version and exit")
	flag.Parse()

	// print version
	if printVersion {
		fmt.Println(AppVersion)
		os.Exit(0)
	}

	// connect to replica
	replica, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/?interpolateParams=true",
		cnf.Username,
		cnf.Password,
		cnf.Host,
		cnf.Port,
	))
	if err != nil {
		log.Fatal(err)
	}
	defer replica.Close()

	// connect to primary
	primary, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/?interpolateParams=true",
		cnf.Username,
		cnf.Password,
		"primary", // TODO: fix me
		cnf.Port,
	))
	if err != nil {
		log.Fatal(err)
	}
	defer primary.Close()

	// start tester
	tester := localtests.NewTester(cnf, primary, replica)
	if err = tester.WaitForMySQLAvailable(); err != nil {
		log.Fatalf("Failed to setup MySQL database servers: %+v", err)
	}

	// find tests
	var tests []localtests.Test
	if testNoop {
		tests = []localtests.Test{
			{
				Name: "noop",
				ExtraArgs: []string{
					fmt.Sprintf("--alter='ENGINE=%s'", cnf.StorageEngine),
				},
			},
		}
	} else {
		tests, err = tester.ReadTests(testName)
		if err != nil {
			log.Fatalf("Failed to read tests: %+v", err)
		}
	}

	// run tests
	for _, test := range tests {
		log.Println("------------------------------------------------------------------------------------------------------------")
		log.Printf("Loading test %q at %s/%s", test.Name, cnf.TestsDir, test.Name)
		if err = tester.RunTest(test); err != nil {
			log.Fatalf("Failed to run test %s: %+v", test.Name, err)
		}
	}
}
