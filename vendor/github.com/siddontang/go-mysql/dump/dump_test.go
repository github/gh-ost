package dump

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	. "github.com/pingcap/check"
	"github.com/siddontang/go-mysql/client"
)

// use docker mysql for test
var host = flag.String("host", "127.0.0.1", "MySQL host")
var port = flag.Int("port", 3306, "MySQL host")

var execution = flag.String("exec", "mysqldump", "mysqldump execution path")

func Test(t *testing.T) {
	TestingT(t)
}

type schemaTestSuite struct {
	conn *client.Conn
	d    *Dumper
}

var _ = Suite(&schemaTestSuite{})

func (s *schemaTestSuite) SetUpSuite(c *C) {
	var err error
	s.conn, err = client.Connect(fmt.Sprintf("%s:%d", *host, *port), "root", "", "")
	c.Assert(err, IsNil)

	s.d, err = NewDumper(*execution, fmt.Sprintf("%s:%d", *host, *port), "root", "")
	c.Assert(err, IsNil)
	c.Assert(s.d, NotNil)

	s.d.SetCharset("utf8")
	s.d.SetErrOut(os.Stderr)

	_, err = s.conn.Execute("CREATE DATABASE IF NOT EXISTS test1")
	c.Assert(err, IsNil)

	_, err = s.conn.Execute("CREATE DATABASE IF NOT EXISTS test2")
	c.Assert(err, IsNil)

	str := `CREATE TABLE IF NOT EXISTS test%d.t%d (
			id int AUTO_INCREMENT,
			name varchar(256),
			PRIMARY KEY(id)
			) ENGINE=INNODB`
	_, err = s.conn.Execute(fmt.Sprintf(str, 1, 1))
	c.Assert(err, IsNil)

	_, err = s.conn.Execute(fmt.Sprintf(str, 2, 1))
	c.Assert(err, IsNil)

	_, err = s.conn.Execute(fmt.Sprintf(str, 1, 2))
	c.Assert(err, IsNil)

	_, err = s.conn.Execute(fmt.Sprintf(str, 2, 2))
	c.Assert(err, IsNil)

	str = `INSERT INTO test%d.t%d (name) VALUES ("a"), ("b"), ("\\"), ("''")`

	_, err = s.conn.Execute(fmt.Sprintf(str, 1, 1))
	c.Assert(err, IsNil)

	_, err = s.conn.Execute(fmt.Sprintf(str, 2, 1))
	c.Assert(err, IsNil)

	_, err = s.conn.Execute(fmt.Sprintf(str, 1, 2))
	c.Assert(err, IsNil)

	_, err = s.conn.Execute(fmt.Sprintf(str, 2, 2))
	c.Assert(err, IsNil)
}

func (s *schemaTestSuite) TearDownSuite(c *C) {
	if s.conn != nil {
		_, err := s.conn.Execute("DROP DATABASE IF EXISTS test1")
		c.Assert(err, IsNil)

		_, err = s.conn.Execute("DROP DATABASE IF EXISTS test2")
		c.Assert(err, IsNil)

		s.conn.Close()
	}
}

func (s *schemaTestSuite) TestDump(c *C) {
	// Using mysql 5.7 can't work, error:
	// 	mysqldump: Error 1412: Table definition has changed,
	// 	please retry transaction when dumping table `test_replication` at row: 0
	// err := s.d.Dump(ioutil.Discard)
	// c.Assert(err, IsNil)

	s.d.AddDatabases("test1", "test2")

	s.d.AddIgnoreTables("test1", "t2")

	err := s.d.Dump(ioutil.Discard)
	c.Assert(err, IsNil)

	s.d.AddTables("test1", "t1")

	err = s.d.Dump(ioutil.Discard)
	c.Assert(err, IsNil)
}

type testParseHandler struct {
}

func (h *testParseHandler) BinLog(name string, pos uint64) error {
	return nil
}

func (h *testParseHandler) Data(schema string, table string, values []string) error {
	return nil
}

func (s *parserTestSuite) TestParseFindTable(c *C) {
	tbl := []struct {
		sql   string
		table string
	}{
		{"INSERT INTO `note` VALUES ('title', 'here is sql: INSERT INTO `table` VALUES (\\'some value\\')');", "note"},
		{"INSERT INTO `note` VALUES ('1', '2', '3');", "note"},
		{"INSERT INTO `a.b` VALUES ('1');", "a.b"},
	}

	for _, t := range tbl {
		res := valuesExp.FindAllStringSubmatch(t.sql, -1)[0][1]
		c.Assert(res, Equals, t.table)
	}
}

type parserTestSuite struct {
}

var _ = Suite(&parserTestSuite{})

func (s *parserTestSuite) TestUnescape(c *C) {
	tbl := []struct {
		escaped  string
		expected string
	}{
		{`\\n`, `\n`},
		{`\\t`, `\t`},
		{`\\"`, `\"`},
		{`\\'`, `\'`},
		{`\\0`, `\0`},
		{`\\b`, `\b`},
		{`\\Z`, `\Z`},
		{`\\r`, `\r`},
		{`abc`, `abc`},
		{`abc\`, `abc`},
		{`ab\c`, `abc`},
		{`\abc`, `abc`},
	}

	for _, t := range tbl {
		unesacped := unescapeString(t.escaped)
		c.Assert(unesacped, Equals, t.expected)
	}
}

func (s *schemaTestSuite) TestParse(c *C) {
	var buf bytes.Buffer

	s.d.Reset()

	s.d.AddDatabases("test1", "test2")

	err := s.d.Dump(&buf)
	c.Assert(err, IsNil)

	err = Parse(&buf, new(testParseHandler), true)
	c.Assert(err, IsNil)
}

func (s *parserTestSuite) TestParseValue(c *C) {
	str := `'abc\\',''`
	values, err := parseValues(str)
	c.Assert(err, IsNil)
	c.Assert(values, DeepEquals, []string{`'abc\'`, `''`})

	str = `123,'\Z#÷QÎx£. Æ‘ÇoPâÅ_\r—\\','','qn'`
	values, err = parseValues(str)
	c.Assert(err, IsNil)
	c.Assert(values, HasLen, 4)

	str = `123,'\Z#÷QÎx£. Æ‘ÇoPâÅ_\r—\\','','qn\'`
	values, err = parseValues(str)
	c.Assert(err, NotNil)
}
