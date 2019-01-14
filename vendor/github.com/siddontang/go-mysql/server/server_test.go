package server

import (
	"crypto/tls"
	"database/sql"
	"flag"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/test_util/test_keys"
)

var testAddr = flag.String("addr", "127.0.0.1:4000", "MySQL proxy server address")
var testUser = flag.String("user", "root", "MySQL user")
var testPassword = flag.String("pass", "123456", "MySQL password")
var testDB = flag.String("db", "test", "MySQL test database")

var tlsConf = NewServerTLSConfig(test_keys.CaPem, test_keys.CertPem, test_keys.KeyPem, tls.VerifyClientCertIfGiven)

func prepareServerConf() []*Server {
	// add default server without TLS
	var servers = []*Server{
		// with default TLS
		NewDefaultServer(),
		// for key exchange, CLIENT_SSL must be enabled for the server and if the connection is not secured with TLS
		// server permits MYSQL_NATIVE_PASSWORD only
		NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_NATIVE_PASSWORD, test_keys.PubPem, tlsConf),
		NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_NATIVE_PASSWORD, test_keys.PubPem, tlsConf),
		// server permits SHA256_PASSWORD only
		NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_SHA256_PASSWORD, test_keys.PubPem, tlsConf),
		// server permits CACHING_SHA2_PASSWORD only
		NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_CACHING_SHA2_PASSWORD, test_keys.PubPem, tlsConf),

		// test auth switch: server permits SHA256_PASSWORD only but sent different method MYSQL_NATIVE_PASSWORD in handshake response
		NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_NATIVE_PASSWORD, test_keys.PubPem, tlsConf),
		// test auth switch: server permits CACHING_SHA2_PASSWORD only but sent different method MYSQL_NATIVE_PASSWORD in handshake response
		NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_NATIVE_PASSWORD, test_keys.PubPem, tlsConf),
		// test auth switch: server permits CACHING_SHA2_PASSWORD only but sent different method SHA256_PASSWORD in handshake response
		NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_SHA256_PASSWORD, test_keys.PubPem, tlsConf),
		// test auth switch: server permits MYSQL_NATIVE_PASSWORD only but sent different method SHA256_PASSWORD in handshake response
		NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_SHA256_PASSWORD, test_keys.PubPem, tlsConf),
		// test auth switch: server permits SHA256_PASSWORD only but sent different method CACHING_SHA2_PASSWORD in handshake response
		NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_CACHING_SHA2_PASSWORD, test_keys.PubPem, tlsConf),
		// test auth switch: server permits MYSQL_NATIVE_PASSWORD only but sent different method CACHING_SHA2_PASSWORD in handshake response
		NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_CACHING_SHA2_PASSWORD, test_keys.PubPem, tlsConf),
	}
	return servers
}

func Test(t *testing.T) {
	log.SetLevel(log.LevelDebug)

	// general tests
	inMemProvider := NewInMemoryProvider()
	inMemProvider.AddUser(*testUser, *testPassword)

	servers := prepareServerConf()
	//no TLS
	for _, svr := range servers {
		Suite(&serverTestSuite{
			server:       svr,
			credProvider: inMemProvider,
			tlsPara:      "false",
		})
	}

	// TLS if server supports
	for _, svr := range servers {
		if svr.tlsConfig != nil {
			Suite(&serverTestSuite{
				server:       svr,
				credProvider: inMemProvider,
				tlsPara:      "skip-verify",
			})
		}
	}

	TestingT(t)
}

type serverTestSuite struct {
	server       *Server
	credProvider CredentialProvider

	tlsPara string

	db *sql.DB

	l net.Listener
}

func (s *serverTestSuite) SetUpSuite(c *C) {
	var err error

	s.l, err = net.Listen("tcp", *testAddr)
	c.Assert(err, IsNil)

	go s.onAccept(c)

	time.Sleep(20 * time.Millisecond)

	s.db, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s?tls=%s", *testUser, *testPassword, *testAddr, *testDB, s.tlsPara))
	c.Assert(err, IsNil)

	s.db.SetMaxIdleConns(4)
}

func (s *serverTestSuite) TearDownSuite(c *C) {
	if s.db != nil {
		s.db.Close()
	}

	if s.l != nil {
		s.l.Close()
	}
}

func (s *serverTestSuite) onAccept(c *C) {
	for {
		conn, err := s.l.Accept()
		if err != nil {
			return
		}

		go s.onConn(conn, c)
	}
}

func (s *serverTestSuite) onConn(conn net.Conn, c *C) {
	//co, err := NewConn(conn, *testUser, *testPassword, &testHandler{s})
	co, err := NewCustomizedConn(conn, s.server, s.credProvider, &testHandler{s})
	c.Assert(err, IsNil)
	// set SSL if defined
	for {
		err = co.HandleCommand()
		if err != nil {
			return
		}
	}
}

func (s *serverTestSuite) TestSelect(c *C) {
	var a int64
	var b string

	err := s.db.QueryRow("SELECT a, b FROM tbl WHERE id=1").Scan(&a, &b)
	c.Assert(err, IsNil)
	c.Assert(a, Equals, int64(1))
	c.Assert(b, Equals, "hello world")
}

func (s *serverTestSuite) TestExec(c *C) {
	r, err := s.db.Exec("INSERT INTO tbl (a, b) values (1, \"hello world\")")
	c.Assert(err, IsNil)
	i, _ := r.LastInsertId()
	c.Assert(i, Equals, int64(1))

	r, err = s.db.Exec("REPLACE INTO tbl (a, b) values (1, \"hello world\")")
	c.Assert(err, IsNil)
	i, _ = r.RowsAffected()
	c.Assert(i, Equals, int64(1))

	r, err = s.db.Exec("UPDATE tbl SET b = \"abc\" where a = 1")
	c.Assert(err, IsNil)
	i, _ = r.RowsAffected()
	c.Assert(i, Equals, int64(1))

	r, err = s.db.Exec("DELETE FROM tbl where a = 1")
	c.Assert(err, IsNil)
	i, _ = r.RowsAffected()
	c.Assert(i, Equals, int64(1))
}

func (s *serverTestSuite) TestStmtSelect(c *C) {
	var a int64
	var b string

	err := s.db.QueryRow("SELECT a, b FROM tbl WHERE id=?", 1).Scan(&a, &b)
	c.Assert(err, IsNil)
	c.Assert(a, Equals, int64(1))
	c.Assert(b, Equals, "hello world")
}

func (s *serverTestSuite) TestStmtExec(c *C) {
	r, err := s.db.Exec("INSERT INTO tbl (a, b) values (?, ?)", 1, "hello world")
	c.Assert(err, IsNil)
	i, _ := r.LastInsertId()
	c.Assert(i, Equals, int64(1))

	r, err = s.db.Exec("REPLACE INTO tbl (a, b) values (?, ?)", 1, "hello world")
	c.Assert(err, IsNil)
	i, _ = r.RowsAffected()
	c.Assert(i, Equals, int64(1))

	r, err = s.db.Exec("UPDATE tbl SET b = \"abc\" where a = ?", 1)
	c.Assert(err, IsNil)
	i, _ = r.RowsAffected()
	c.Assert(i, Equals, int64(1))

	r, err = s.db.Exec("DELETE FROM tbl where a = ?", 1)
	c.Assert(err, IsNil)
	i, _ = r.RowsAffected()
	c.Assert(i, Equals, int64(1))
}

type testHandler struct {
	s *serverTestSuite
}

func (h *testHandler) UseDB(dbName string) error {
	return nil
}

func (h *testHandler) handleQuery(query string, binary bool) (*mysql.Result, error) {
	ss := strings.Split(query, " ")
	switch strings.ToLower(ss[0]) {
	case "select":
		var r *mysql.Resultset
		var err error
		//for handle go mysql driver select @@max_allowed_packet
		if strings.Contains(strings.ToLower(query), "max_allowed_packet") {
			r, err = mysql.BuildSimpleResultset([]string{"@@max_allowed_packet"}, [][]interface{}{
				{mysql.MaxPayloadLen},
			}, binary)
		} else {
			r, err = mysql.BuildSimpleResultset([]string{"a", "b"}, [][]interface{}{
				{1, "hello world"},
			}, binary)
		}

		if err != nil {
			return nil, errors.Trace(err)
		} else {
			return &mysql.Result{0, 0, 0, r}, nil
		}
	case "insert":
		return &mysql.Result{0, 1, 0, nil}, nil
	case "delete":
		return &mysql.Result{0, 0, 1, nil}, nil
	case "update":
		return &mysql.Result{0, 0, 1, nil}, nil
	case "replace":
		return &mysql.Result{0, 0, 1, nil}, nil
	default:
		return nil, fmt.Errorf("invalid query %s", query)
	}

	return nil, nil
}

func (h *testHandler) HandleQuery(query string) (*mysql.Result, error) {
	return h.handleQuery(query, false)
}

func (h *testHandler) HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
	return nil, nil
}
func (h *testHandler) HandleStmtPrepare(sql string) (params int, columns int, ctx interface{}, err error) {
	ss := strings.Split(sql, " ")
	switch strings.ToLower(ss[0]) {
	case "select":
		params = 1
		columns = 2
	case "insert":
		params = 2
		columns = 0
	case "replace":
		params = 2
		columns = 0
	case "update":
		params = 1
		columns = 0
	case "delete":
		params = 1
		columns = 0
	default:
		err = fmt.Errorf("invalid prepare %s", sql)
	}
	return params, columns, nil, err
}

func (h *testHandler) HandleStmtClose(context interface{}) error {
	return nil
}

func (h *testHandler) HandleStmtExecute(ctx interface{}, query string, args []interface{}) (*mysql.Result, error) {
	return h.handleQuery(query, true)
}

func (h *testHandler) HandleOtherCommand(cmd byte, data []byte) error {
	return mysql.NewError(mysql.ER_UNKNOWN_ERROR, fmt.Sprintf("command %d is not supported now", cmd))
}