package schema

import (
	"flag"
	"fmt"
	"testing"

	. "github.com/pingcap/check"
	"github.com/siddontang/go-mysql/client"
)

// use docker mysql for test
var host = flag.String("host", "127.0.0.1", "MySQL host")

func Test(t *testing.T) {
	TestingT(t)
}

type schemaTestSuite struct {
	conn *client.Conn
}

var _ = Suite(&schemaTestSuite{})

func (s *schemaTestSuite) SetUpSuite(c *C) {
	var err error
	s.conn, err = client.Connect(fmt.Sprintf("%s:%d", *host, 3306), "root", "", "test")
	c.Assert(err, IsNil)
}

func (s *schemaTestSuite) TearDownSuite(c *C) {
	if s.conn != nil {
		s.conn.Close()
	}
}

func (s *schemaTestSuite) TestSchema(c *C) {
	_, err := s.conn.Execute(`DROP TABLE IF EXISTS schema_test`)
	c.Assert(err, IsNil)

	str := `
        CREATE TABLE IF NOT EXISTS schema_test (
            id INT,
            id1 INT,
            id2 INT,
            name VARCHAR(256),
            e ENUM("a", "b", "c"),
            se SET('a', 'b', 'c'),
            f FLOAT,
            d DECIMAL(2, 1),
            PRIMARY KEY(id2, id),
            UNIQUE (id1),
            INDEX name_idx (name)
        ) ENGINE = INNODB;
    `

	_, err = s.conn.Execute(str)
	c.Assert(err, IsNil)

	ta, err := NewTable(s.conn, "test", "schema_test")
	c.Assert(err, IsNil)

	c.Assert(ta.Columns, HasLen, 8)
	c.Assert(ta.Indexes, HasLen, 3)
	c.Assert(ta.PKColumns, DeepEquals, []int{2, 0})
	c.Assert(ta.Indexes[0].Columns, HasLen, 2)
	c.Assert(ta.Indexes[0].Name, Equals, "PRIMARY")
	c.Assert(ta.Indexes[2].Name, Equals, "name_idx")
	c.Assert(ta.Columns[4].EnumValues, DeepEquals, []string{"a", "b", "c"})
	c.Assert(ta.Columns[5].SetValues, DeepEquals, []string{"a", "b", "c"})
	c.Assert(ta.Columns[7].Type, Equals, TYPE_FLOAT)
}

func (s *schemaTestSuite) TestQuoteSchema(c *C) {
	str := "CREATE TABLE IF NOT EXISTS `a-b_test` (`a.b` INT) ENGINE = INNODB"

	_, err := s.conn.Execute(str)
	c.Assert(err, IsNil)

	ta, err := NewTable(s.conn, "test", "a-b_test")
	c.Assert(err, IsNil)

	c.Assert(ta.Columns[0].Name, Equals, "a.b")
}
