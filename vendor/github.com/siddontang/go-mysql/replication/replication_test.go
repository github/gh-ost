package replication

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	. "github.com/pingcap/check"
	uuid "github.com/satori/go.uuid"
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
)

// Use docker mysql to test, mysql is 3306, mariadb is 3316
var testHost = flag.String("host", "127.0.0.1", "MySQL master host")

var testOutputLogs = flag.Bool("out", false, "output binlog event")

func TestBinLogSyncer(t *testing.T) {
	TestingT(t)
}

type testSyncerSuite struct {
	b *BinlogSyncer
	c *client.Conn

	wg sync.WaitGroup

	flavor string
}

var _ = Suite(&testSyncerSuite{})

func (t *testSyncerSuite) SetUpSuite(c *C) {

}

func (t *testSyncerSuite) TearDownSuite(c *C) {
}

func (t *testSyncerSuite) SetUpTest(c *C) {
}

func (t *testSyncerSuite) TearDownTest(c *C) {
	if t.b != nil {
		t.b.Close()
		t.b = nil
	}

	if t.c != nil {
		t.c.Close()
		t.c = nil
	}
}

func (t *testSyncerSuite) testExecute(c *C, query string) {
	_, err := t.c.Execute(query)
	c.Assert(err, IsNil)
}

func (t *testSyncerSuite) testSync(c *C, s *BinlogStreamer) {
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()

		if s == nil {
			return
		}

		eventCount := 0
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			e, err := s.GetEvent(ctx)
			cancel()

			if err == context.DeadlineExceeded {
				eventCount += 1
				return
			}

			c.Assert(err, IsNil)

			if *testOutputLogs {
				e.Dump(os.Stdout)
				os.Stdout.Sync()
			}
		}
	}()

	//use mixed format
	t.testExecute(c, "SET SESSION binlog_format = 'MIXED'")

	str := `DROP TABLE IF EXISTS test_replication`
	t.testExecute(c, str)

	str = `CREATE TABLE test_replication (
			id BIGINT(64) UNSIGNED  NOT NULL AUTO_INCREMENT,
			str VARCHAR(256),
			f FLOAT,
			d DOUBLE,
			de DECIMAL(10,2),
			i INT,
			bi BIGINT,
			e enum ("e1", "e2"),
			b BIT(8),
			y YEAR,
			da DATE,
			ts TIMESTAMP,
			dt DATETIME,
			tm TIME,
			t TEXT,
			bb BLOB,
			se SET('a', 'b', 'c'),
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8`

	t.testExecute(c, str)

	//use row format
	t.testExecute(c, "SET SESSION binlog_format = 'ROW'")

	t.testExecute(c, `INSERT INTO test_replication (str, f, i, e, b, y, da, ts, dt, tm, de, t, bb, se)
		VALUES ("3", -3.14, 10, "e1", 0b0011, 1985,
		"2012-05-07", "2012-05-07 14:01:01", "2012-05-07 14:01:01",
		"14:01:01", -45363.64, "abc", "12345", "a,b")`)

	id := 100

	if t.flavor == mysql.MySQLFlavor {
		t.testExecute(c, "SET SESSION binlog_row_image = 'MINIMAL'")

		t.testExecute(c, fmt.Sprintf(`INSERT INTO test_replication (id, str, f, i, bb, de) VALUES (%d, "4", -3.14, 100, "abc", -45635.64)`, id))
		t.testExecute(c, fmt.Sprintf(`UPDATE test_replication SET f = -12.14, de = 555.34 WHERE id = %d`, id))
		t.testExecute(c, fmt.Sprintf(`DELETE FROM test_replication WHERE id = %d`, id))
	}

	// check whether we can create the table including the json field
	str = `DROP TABLE IF EXISTS test_json`
	t.testExecute(c, str)

	str = `CREATE TABLE test_json (
			id BIGINT(64) UNSIGNED  NOT NULL AUTO_INCREMENT,
			c1 JSON,
			c2 DECIMAL(10, 0),
			PRIMARY KEY (id)
			) ENGINE=InnoDB`

	if _, err := t.c.Execute(str); err == nil {
		t.testExecute(c, `INSERT INTO test_json (c2) VALUES (1)`)
		t.testExecute(c, `INSERT INTO test_json (c1, c2) VALUES ('{"key1": "value1", "key2": "value2"}', 1)`)
	}

	t.testExecute(c, "DROP TABLE IF EXISTS test_json_v2")

	str = `CREATE TABLE test_json_v2 (
			id INT, 
			c JSON, 
			PRIMARY KEY (id)
			) ENGINE=InnoDB`

	if _, err := t.c.Execute(str); err == nil {
		tbls := []string{
			// Refer: https://github.com/shyiko/mysql-binlog-connector-java/blob/c8e81c879710dc19941d952f9031b0a98f8b7c02/src/test/java/com/github/shyiko/mysql/binlog/event/deserialization/json/JsonBinaryValueIntegrationTest.java#L84
			// License: https://github.com/shyiko/mysql-binlog-connector-java#license
			`INSERT INTO test_json_v2 VALUES (0, NULL)`,
			`INSERT INTO test_json_v2 VALUES (1, '{\"a\": 2}')`,
			`INSERT INTO test_json_v2 VALUES (2, '[1,2]')`,
			`INSERT INTO test_json_v2 VALUES (3, '{\"a\":\"b\", \"c\":\"d\",\"ab\":\"abc\", \"bc\": [\"x\", \"y\"]}')`,
			`INSERT INTO test_json_v2 VALUES (4, '[\"here\", [\"I\", \"am\"], \"!!!\"]')`,
			`INSERT INTO test_json_v2 VALUES (5, '\"scalar string\"')`,
			`INSERT INTO test_json_v2 VALUES (6, 'true')`,
			`INSERT INTO test_json_v2 VALUES (7, 'false')`,
			`INSERT INTO test_json_v2 VALUES (8, 'null')`,
			`INSERT INTO test_json_v2 VALUES (9, '-1')`,
			`INSERT INTO test_json_v2 VALUES (10, CAST(CAST(1 AS UNSIGNED) AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (11, '32767')`,
			`INSERT INTO test_json_v2 VALUES (12, '32768')`,
			`INSERT INTO test_json_v2 VALUES (13, '-32768')`,
			`INSERT INTO test_json_v2 VALUES (14, '-32769')`,
			`INSERT INTO test_json_v2 VALUES (15, '2147483647')`,
			`INSERT INTO test_json_v2 VALUES (16, '2147483648')`,
			`INSERT INTO test_json_v2 VALUES (17, '-2147483648')`,
			`INSERT INTO test_json_v2 VALUES (18, '-2147483649')`,
			`INSERT INTO test_json_v2 VALUES (19, '18446744073709551615')`,
			`INSERT INTO test_json_v2 VALUES (20, '18446744073709551616')`,
			`INSERT INTO test_json_v2 VALUES (21, '3.14')`,
			`INSERT INTO test_json_v2 VALUES (22, '{}')`,
			`INSERT INTO test_json_v2 VALUES (23, '[]')`,
			`INSERT INTO test_json_v2 VALUES (24, CAST(CAST('2015-01-15 23:24:25' AS DATETIME) AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (25, CAST(CAST('23:24:25' AS TIME) AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (125, CAST(CAST('23:24:25.12' AS TIME(3)) AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (225, CAST(CAST('23:24:25.0237' AS TIME(3)) AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (26, CAST(CAST('2015-01-15' AS DATE) AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (27, CAST(TIMESTAMP'2015-01-15 23:24:25' AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (127, CAST(TIMESTAMP'2015-01-15 23:24:25.12' AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (227, CAST(TIMESTAMP'2015-01-15 23:24:25.0237' AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (327, CAST(UNIX_TIMESTAMP('2015-01-15 23:24:25') AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (28, CAST(ST_GeomFromText('POINT(1 1)') AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (29, CAST('[]' AS CHAR CHARACTER SET 'ascii'))`,
			// TODO: 30 and 31 are BIT type from JSON_TYPE, may support later.
			`INSERT INTO test_json_v2 VALUES (30, CAST(x'cafe' AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (31, CAST(x'cafebabe' AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (100, CONCAT('{\"', REPEAT('a', 64 * 1024 - 1), '\":123}'))`,
		}

		for _, query := range tbls {
			t.testExecute(c, query)
		}

		// If MySQL supports JSON, it must supports GEOMETRY.
		t.testExecute(c, "DROP TABLE IF EXISTS test_geo")

		str = `CREATE TABLE test_geo (g GEOMETRY)`
		_, err = t.c.Execute(str)
		c.Assert(err, IsNil)

		tbls = []string{
			`INSERT INTO test_geo VALUES (POINT(1, 1))`,
			`INSERT INTO test_geo VALUES (LINESTRING(POINT(0,0), POINT(1,1), POINT(2,2)))`,
			// TODO: add more geometry tests
		}

		for _, query := range tbls {
			t.testExecute(c, query)
		}
	}

	str = `DROP TABLE IF EXISTS test_parse_time`
	t.testExecute(c, str)

	// Must allow zero time.
	t.testExecute(c, `SET sql_mode=''`)
	str = `CREATE TABLE test_parse_time (
			a1 DATETIME, 
			a2 DATETIME(3), 
			a3 DATETIME(6), 
			b1 TIMESTAMP, 
			b2 TIMESTAMP(3) , 
			b3 TIMESTAMP(6))`
	t.testExecute(c, str)

	t.testExecute(c, `INSERT INTO test_parse_time VALUES
		("2014-09-08 17:51:04.123456", "2014-09-08 17:51:04.123456", "2014-09-08 17:51:04.123456", 
		"2014-09-08 17:51:04.123456","2014-09-08 17:51:04.123456","2014-09-08 17:51:04.123456"),
		("0000-00-00 00:00:00.000000", "0000-00-00 00:00:00.000000", "0000-00-00 00:00:00.000000",
		"0000-00-00 00:00:00.000000", "0000-00-00 00:00:00.000000", "0000-00-00 00:00:00.000000"),
		("2014-09-08 17:51:04.000456", "2014-09-08 17:51:04.000456", "2014-09-08 17:51:04.000456", 
		"2014-09-08 17:51:04.000456","2014-09-08 17:51:04.000456","2014-09-08 17:51:04.000456")`)

	t.wg.Wait()
}

func (t *testSyncerSuite) setupTest(c *C, flavor string) {
	var port uint16 = 3306
	switch flavor {
	case mysql.MariaDBFlavor:
		port = 3316
	}

	t.flavor = flavor

	var err error
	if t.c != nil {
		t.c.Close()
	}

	t.c, err = client.Connect(fmt.Sprintf("%s:%d", *testHost, port), "root", "", "")
	if err != nil {
		c.Skip(err.Error())
	}

	// _, err = t.c.Execute("CREATE DATABASE IF NOT EXISTS test")
	// c.Assert(err, IsNil)

	_, err = t.c.Execute("USE test")
	c.Assert(err, IsNil)

	if t.b != nil {
		t.b.Close()
	}

	cfg := BinlogSyncerConfig{
		ServerID: 100,
		Flavor:   flavor,
		Host:     *testHost,
		Port:     port,
		User:     "root",
		Password: "",
	}

	t.b = NewBinlogSyncer(cfg)
}

func (t *testSyncerSuite) testPositionSync(c *C) {
	//get current master binlog file and position
	r, err := t.c.Execute("SHOW MASTER STATUS")
	c.Assert(err, IsNil)
	binFile, _ := r.GetString(0, 0)
	binPos, _ := r.GetInt(0, 1)

	s, err := t.b.StartSync(mysql.Position{binFile, uint32(binPos)})
	c.Assert(err, IsNil)

	// Test re-sync.
	time.Sleep(100 * time.Millisecond)
	t.b.c.SetReadDeadline(time.Now().Add(time.Millisecond))
	time.Sleep(100 * time.Millisecond)

	t.testSync(c, s)
}

func (t *testSyncerSuite) TestMysqlPositionSync(c *C) {
	t.setupTest(c, mysql.MySQLFlavor)
	t.testPositionSync(c)
}

func (t *testSyncerSuite) TestMysqlGTIDSync(c *C) {
	t.setupTest(c, mysql.MySQLFlavor)

	r, err := t.c.Execute("SELECT @@gtid_mode")
	c.Assert(err, IsNil)
	modeOn, _ := r.GetString(0, 0)
	if modeOn != "ON" {
		c.Skip("GTID mode is not ON")
	}

	r, err = t.c.Execute("SHOW GLOBAL VARIABLES LIKE 'SERVER_UUID'")
	c.Assert(err, IsNil)

	var masterUuid uuid.UUID
	if s, _ := r.GetString(0, 1); len(s) > 0 && s != "NONE" {
		masterUuid, err = uuid.FromString(s)
		c.Assert(err, IsNil)
	}

	set, _ := mysql.ParseMysqlGTIDSet(fmt.Sprintf("%s:%d-%d", masterUuid.String(), 1, 2))

	s, err := t.b.StartSyncGTID(set)
	c.Assert(err, IsNil)

	t.testSync(c, s)
}

func (t *testSyncerSuite) TestMariadbPositionSync(c *C) {
	t.setupTest(c, mysql.MariaDBFlavor)

	t.testPositionSync(c)
}

func (t *testSyncerSuite) TestMariadbGTIDSync(c *C) {
	t.setupTest(c, mysql.MariaDBFlavor)

	// get current master gtid binlog pos
	r, err := t.c.Execute("SELECT @@gtid_binlog_pos")
	c.Assert(err, IsNil)

	str, _ := r.GetString(0, 0)
	set, _ := mysql.ParseMariadbGTIDSet(str)

	s, err := t.b.StartSyncGTID(set)
	c.Assert(err, IsNil)

	t.testSync(c, s)
}

func (t *testSyncerSuite) TestMysqlSemiPositionSync(c *C) {
	t.setupTest(c, mysql.MySQLFlavor)

	t.b.cfg.SemiSyncEnabled = true

	t.testPositionSync(c)
}

func (t *testSyncerSuite) TestMysqlBinlogCodec(c *C) {
	t.setupTest(c, mysql.MySQLFlavor)

	t.testExecute(c, "RESET MASTER")

	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()

	go func() {
		defer wg.Done()

		t.testSync(c, nil)

		t.testExecute(c, "FLUSH LOGS")

		t.testSync(c, nil)
	}()

	os.RemoveAll("./var")

	err := t.b.StartBackup("./var", mysql.Position{"", uint32(0)}, 2*time.Second)
	c.Assert(err, IsNil)

	p := NewBinlogParser()

	f := func(e *BinlogEvent) error {
		if *testOutputLogs {
			e.Dump(os.Stdout)
			os.Stdout.Sync()
		}
		return nil
	}

	err = p.ParseFile("./var/mysql.000001", 0, f)
	c.Assert(err, IsNil)

	err = p.ParseFile("./var/mysql.000002", 0, f)
	c.Assert(err, IsNil)
}
