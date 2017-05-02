package failover

import (
	"flag"
	"fmt"
	"testing"

	. "github.com/pingcap/check"
)

// We will use go-mysql docker to test
// go-mysql docker will build mysql 1-3 instances
var host = flag.String("host", "127.0.0.1", "go-mysql docker container address")
var enable_failover_test = flag.Bool("test-failover", false, "enable test failover")

func Test(t *testing.T) {
	TestingT(t)
}

type failoverTestSuite struct {
	s []*Server
}

var _ = Suite(&failoverTestSuite{})

func (s *failoverTestSuite) SetUpSuite(c *C) {
	if !*enable_failover_test {
		c.Skip("skip test failover")
	}

	ports := []int{3306, 3307, 3308, 3316, 3317, 3318}

	s.s = make([]*Server, len(ports))

	for i := 0; i < len(ports); i++ {
		s.s[i] = NewServer(fmt.Sprintf("%s:%d", *host, ports[i]), User{"root", ""}, User{"root", ""})
	}

	var err error
	for i := 0; i < len(ports); i++ {
		err = s.s[i].StopSlave()
		c.Assert(err, IsNil)

		err = s.s[i].ResetSlaveALL()
		c.Assert(err, IsNil)

		_, err = s.s[i].Execute(`SET GLOBAL BINLOG_FORMAT = "ROW"`)
		c.Assert(err, IsNil)

		_, err = s.s[i].Execute("DROP TABLE IF EXISTS test.go_mysql_test")
		c.Assert(err, IsNil)

		_, err = s.s[i].Execute("CREATE TABLE IF NOT EXISTS test.go_mysql_test (id INT AUTO_INCREMENT, name VARCHAR(256), PRIMARY KEY(id)) engine=innodb")
		c.Assert(err, IsNil)

		err = s.s[i].ResetMaster()
		c.Assert(err, IsNil)
	}
}

func (s *failoverTestSuite) TearDownSuite(c *C) {
}

func (s *failoverTestSuite) TestMysqlFailover(c *C) {
	h := new(MysqlGTIDHandler)

	m := s.s[0]
	s1 := s.s[1]
	s2 := s.s[2]

	s.testFailover(c, h, m, s1, s2)
}

func (s *failoverTestSuite) TestMariadbFailover(c *C) {
	h := new(MariadbGTIDHandler)

	for i := 3; i <= 5; i++ {
		_, err := s.s[i].Execute("SET GLOBAL gtid_slave_pos = ''")
		c.Assert(err, IsNil)
	}

	m := s.s[3]
	s1 := s.s[4]
	s2 := s.s[5]

	s.testFailover(c, h, m, s1, s2)
}

func (s *failoverTestSuite) testFailover(c *C, h Handler, m *Server, s1 *Server, s2 *Server) {
	var err error
	err = h.ChangeMasterTo(s1, m)
	c.Assert(err, IsNil)

	err = h.ChangeMasterTo(s2, m)
	c.Assert(err, IsNil)

	id := s.checkInsert(c, m, "a")

	err = h.WaitCatchMaster(s1, m)
	c.Assert(err, IsNil)

	err = h.WaitCatchMaster(s2, m)
	c.Assert(err, IsNil)

	s.checkSelect(c, s1, id, "a")
	s.checkSelect(c, s2, id, "a")

	err = s2.StopSlaveIOThread()
	c.Assert(err, IsNil)

	id = s.checkInsert(c, m, "b")
	id = s.checkInsert(c, m, "c")

	err = h.WaitCatchMaster(s1, m)
	c.Assert(err, IsNil)

	s.checkSelect(c, s1, id, "c")

	best, err := h.FindBestSlaves([]*Server{s1, s2})
	c.Assert(err, IsNil)
	c.Assert(best, DeepEquals, []*Server{s1})

	// promote s1 to master
	err = h.Promote(s1)
	c.Assert(err, IsNil)

	// change s2 to master s1
	err = h.ChangeMasterTo(s2, s1)
	c.Assert(err, IsNil)

	err = h.WaitCatchMaster(s2, s1)
	c.Assert(err, IsNil)

	s.checkSelect(c, s2, id, "c")

	// change m to master s1
	err = h.ChangeMasterTo(m, s1)
	c.Assert(err, IsNil)

	m, s1 = s1, m
	id = s.checkInsert(c, m, "d")

	err = h.WaitCatchMaster(s1, m)
	c.Assert(err, IsNil)

	err = h.WaitCatchMaster(s2, m)
	c.Assert(err, IsNil)

	best, err = h.FindBestSlaves([]*Server{s1, s2})
	c.Assert(err, IsNil)
	c.Assert(best, DeepEquals, []*Server{s1, s2})

	err = s2.StopSlaveIOThread()
	c.Assert(err, IsNil)

	id = s.checkInsert(c, m, "e")
	err = h.WaitCatchMaster(s1, m)

	best, err = h.FindBestSlaves([]*Server{s1, s2})
	c.Assert(err, IsNil)
	c.Assert(best, DeepEquals, []*Server{s1})
}

func (s *failoverTestSuite) checkSelect(c *C, m *Server, id uint64, name string) {
	rr, err := m.Execute("SELECT name FROM test.go_mysql_test WHERE id = ?", id)
	c.Assert(err, IsNil)
	str, _ := rr.GetString(0, 0)
	c.Assert(str, Equals, name)
}

func (s *failoverTestSuite) checkInsert(c *C, m *Server, name string) uint64 {
	r, err := m.Execute("INSERT INTO test.go_mysql_test (name) VALUES (?)", name)
	c.Assert(err, IsNil)

	return r.InsertId
}
