package canal

import (
	"bytes"
	"flag"
	"fmt"
	"testing"
	"time"

	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/siddontang/go-mysql/mysql"
)

var testHost = flag.String("host", "127.0.0.1", "MySQL host")

func Test(t *testing.T) {
	TestingT(t)
}

type canalTestSuite struct {
	c *Canal
}

var _ = Suite(&canalTestSuite{})

func (s *canalTestSuite) SetUpSuite(c *C) {
	cfg := NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:3306", *testHost)
	cfg.User = "root"
	cfg.HeartbeatPeriod = 200 * time.Millisecond
	cfg.ReadTimeout = 300 * time.Millisecond
	cfg.Dump.ExecutionPath = "mysqldump"
	cfg.Dump.TableDB = "test"
	cfg.Dump.Tables = []string{"canal_test"}

	var err error
	s.c, err = NewCanal(cfg)
	c.Assert(err, IsNil)
	s.execute(c, "DROP TABLE IF EXISTS test.canal_test")
	sql := `
        CREATE TABLE IF NOT EXISTS test.canal_test (
            id int AUTO_INCREMENT,
            name varchar(100),
            PRIMARY KEY(id)
            )ENGINE=innodb;
    `

	s.execute(c, sql)

	s.execute(c, "DELETE FROM test.canal_test")
	s.execute(c, "INSERT INTO test.canal_test (name) VALUES (?), (?), (?)", "a", "b", "c")

	s.execute(c, "SET GLOBAL binlog_format = 'ROW'")

	s.c.SetEventHandler(&testEventHandler{})
	err = s.c.Start()
	c.Assert(err, IsNil)
}

func (s *canalTestSuite) TearDownSuite(c *C) {
	// To test the heartbeat and read timeout,so need to sleep 1 seconds without data transmission
	log.Infof("Start testing the heartbeat and read timeout")
	time.Sleep(time.Second)

	if s.c != nil {
		s.c.Close()
		s.c = nil
	}
}

func (s *canalTestSuite) execute(c *C, query string, args ...interface{}) *mysql.Result {
	r, err := s.c.Execute(query, args...)
	c.Assert(err, IsNil)
	return r
}

type testEventHandler struct {
	DummyEventHandler
}

func (h *testEventHandler) Do(e *RowsEvent) error {
	log.Infof("%s %v\n", e.Action, e.Rows)
	return nil
}

func (h *testEventHandler) String() string {
	return "testEventHandler"
}

func (s *canalTestSuite) TestCanal(c *C) {
	<-s.c.WaitDumpDone()

	for i := 1; i < 10; i++ {
		s.execute(c, "INSERT INTO test.canal_test (name) VALUES (?)", fmt.Sprintf("%d", i))
	}
	s.execute(c, "ALTER TABLE test.canal_test ADD `age` INT(5) NOT NULL AFTER `name`")
	s.execute(c, "INSERT INTO test.canal_test (name,age) VALUES (?,?)", "d", "18")

	err := s.c.CatchMasterPos(10 * time.Second)
	c.Assert(err, IsNil)
}

func TestAlterTableExp(t *testing.T) {
	cases := []string{
		"ALTER TABLE `mydb`.`mytable` ADD `field2` DATE  NULL  AFTER `field1`;",
		"ALTER TABLE `mytable` ADD `field2` DATE  NULL  AFTER `field1`;",
		"ALTER TABLE mydb.mytable ADD `field2` DATE  NULL  AFTER `field1`;",
		"ALTER TABLE mytable ADD `field2` DATE  NULL  AFTER `field1`;",
		"ALTER TABLE mydb.mytable ADD field2 DATE  NULL  AFTER `field1`;",
	}

	table := []byte("mytable")
	db := []byte("mydb")
	for _, s := range cases {
		m := expAlterTable.FindSubmatch([]byte(s))
		if m == nil || !bytes.Equal(m[2], table) || (len(m[1]) > 0 && !bytes.Equal(m[1], db)) {
			t.Fatalf("TestAlterTableExp: case %s failed\n", s)
		}
	}
}

func TestRenameTableExp(t *testing.T) {
	cases := []string{
		"rename table `mydb`.`mytable` to `mydb`.`mytable1`",
		"rename table `mytable` to `mytable1`",
		"rename table mydb.mytable to mydb.mytable1",
		"rename table mytable to mytable1",

		"rename table `mydb`.`mytable1` to `mydb`.`mytable2`, `mydb`.`mytable` to `mydb`.`mytable1`",
		"rename table `mytable1` to `mytable2`, `mytable` to `mytable1`",
		"rename table mydb.mytable1 to mydb.mytable2, mydb.mytable to mydb.mytable1",
		"rename table mytable1 to mytable2, mytable to mytable1",
	}
	table := []byte("mytable1")
	db := []byte("mydb")
	for _, s := range cases {
		m := expRenameTable.FindSubmatch([]byte(s))
		if m == nil || !bytes.Equal(m[2], table) || (len(m[1]) > 0 && !bytes.Equal(m[1], db)) {
			t.Fatalf("TestRenameTableExp: case %s failed\n", s)
		}
	}
}
