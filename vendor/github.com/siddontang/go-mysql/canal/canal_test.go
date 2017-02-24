package canal

import (
	"flag"
	"fmt"
	"os"
	"testing"

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
	cfg.Dump.ExecutionPath = "mysqldump"
	cfg.Dump.TableDB = "test"
	cfg.Dump.Tables = []string{"canal_test"}

	os.RemoveAll(cfg.DataDir)

	var err error
	s.c, err = NewCanal(cfg)
	c.Assert(err, IsNil)

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

	s.c.RegRowsEventHandler(&testRowsEventHandler{})
	err = s.c.Start()
	c.Assert(err, IsNil)
}

func (s *canalTestSuite) TearDownSuite(c *C) {
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

type testRowsEventHandler struct {
}

func (h *testRowsEventHandler) Do(e *RowsEvent) error {
	log.Infof("%s %v\n", e.Action, e.Rows)
	return nil
}

func (h *testRowsEventHandler) String() string {
	return "testRowsEventHandler"
}

func (s *canalTestSuite) TestCanal(c *C) {
	<-s.c.WaitDumpDone()

	for i := 1; i < 10; i++ {
		s.execute(c, "INSERT INTO test.canal_test (name) VALUES (?)", fmt.Sprintf("%d", i))
	}

	err := s.c.CatchMasterPos(100)
	c.Assert(err, IsNil)
}
