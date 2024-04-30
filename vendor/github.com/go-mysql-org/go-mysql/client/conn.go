package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"runtime"
	"strings"
	"time"

	"github.com/pingcap/errors"

	. "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/packet"
	"github.com/go-mysql-org/go-mysql/utils"
)

type Conn struct {
	*packet.Conn

	user      string
	password  string
	db        string
	tlsConfig *tls.Config
	proto     string

	serverVersion string
	// server capabilities
	capability uint32
	// client-set capabilities only
	ccaps uint32

	attributes map[string]string

	status uint16

	charset string

	salt           []byte
	authPluginName string

	connectionID uint32
}

// This function will be called for every row in resultset from ExecuteSelectStreaming.
type SelectPerRowCallback func(row []FieldValue) error

// This function will be called once per result from ExecuteSelectStreaming
type SelectPerResultCallback func(result *Result) error

// This function will be called once per result from ExecuteMultiple
type ExecPerResultCallback func(result *Result, err error)

func getNetProto(addr string) string {
	proto := "tcp"
	if strings.Contains(addr, "/") {
		proto = "unix"
	}
	return proto
}

// Connect to a MySQL server, addr can be ip:port, or a unix socket domain like /var/sock.
// Accepts a series of configuration functions as a variadic argument.
func Connect(addr string, user string, password string, dbName string, options ...func(*Conn)) (*Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	dialer := &net.Dialer{}

	return ConnectWithDialer(ctx, "", addr, user, password, dbName, dialer.DialContext, options...)
}

// Dialer connects to the address on the named network using the provided context.
type Dialer func(ctx context.Context, network, address string) (net.Conn, error)

// Connect to a MySQL server using the given Dialer.
func ConnectWithDialer(ctx context.Context, network string, addr string, user string, password string, dbName string, dialer Dialer, options ...func(*Conn)) (*Conn, error) {
	c := new(Conn)

	c.attributes = map[string]string{
		"_client_name": "go-mysql",
		// "_client_version": "0.1",
		"_os":              runtime.GOOS,
		"_platform":        runtime.GOARCH,
		"_runtime_version": runtime.Version(),
	}

	if network == "" {
		network = getNetProto(addr)
	}

	var err error
	conn, err := dialer(ctx, network, addr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	c.user = user
	c.password = password
	c.db = dbName
	c.proto = network
	c.Conn = packet.NewConn(conn)

	// use default charset here, utf-8
	c.charset = DEFAULT_CHARSET

	// Apply configuration functions.
	for i := range options {
		options[i](c)
	}

	if c.tlsConfig != nil {
		seq := c.Conn.Sequence
		c.Conn = packet.NewTLSConn(conn)
		c.Conn.Sequence = seq
	}

	if err = c.handshake(); err != nil {
		return nil, errors.Trace(err)
	}

	if c.ccaps&CLIENT_COMPRESS > 0 {
		c.Conn.Compression = MYSQL_COMPRESS_ZLIB
	} else if c.ccaps&CLIENT_ZSTD_COMPRESSION_ALGORITHM > 0 {
		c.Conn.Compression = MYSQL_COMPRESS_ZSTD
	}

	return c, nil
}

func (c *Conn) handshake() error {
	var err error
	if err = c.readInitialHandshake(); err != nil {
		c.Close()
		return errors.Trace(fmt.Errorf("readInitialHandshake: %w", err))
	}

	if err := c.writeAuthHandshake(); err != nil {
		c.Close()

		return errors.Trace(fmt.Errorf("writeAuthHandshake: %w", err))
	}

	if err := c.handleAuthResult(); err != nil {
		c.Close()
		return errors.Trace(fmt.Errorf("handleAuthResult: %w", err))
	}

	return nil
}

func (c *Conn) Close() error {
	return c.Conn.Close()
}

func (c *Conn) Quit() error {
	if err := c.writeCommand(COM_QUIT); err != nil {
		return err
	}
	return c.Close()
}

func (c *Conn) Ping() error {
	if err := c.writeCommand(COM_PING); err != nil {
		return errors.Trace(err)
	}

	if _, err := c.readOK(); err != nil {
		return errors.Trace(err)
	}

	return nil
}

// SetCapability enables the use of a specific capability
func (c *Conn) SetCapability(cap uint32) {
	c.ccaps |= cap
}

// UnsetCapability disables the use of a specific capability
func (c *Conn) UnsetCapability(cap uint32) {
	c.ccaps &= ^cap
}

// UseSSL: use default SSL
// pass to options when connect
func (c *Conn) UseSSL(insecureSkipVerify bool) {
	c.tlsConfig = &tls.Config{InsecureSkipVerify: insecureSkipVerify}
}

// SetTLSConfig: use user-specified TLS config
// pass to options when connect
func (c *Conn) SetTLSConfig(config *tls.Config) {
	c.tlsConfig = config
}

func (c *Conn) UseDB(dbName string) error {
	if c.db == dbName {
		return nil
	}

	if err := c.writeCommandStr(COM_INIT_DB, dbName); err != nil {
		return errors.Trace(err)
	}

	if _, err := c.readOK(); err != nil {
		return errors.Trace(err)
	}

	c.db = dbName
	return nil
}

func (c *Conn) GetDB() string {
	return c.db
}

func (c *Conn) GetServerVersion() string {
	return c.serverVersion
}

func (c *Conn) CompareServerVersion(v string) (int, error) {
	return CompareServerVersions(c.serverVersion, v)
}

func (c *Conn) Execute(command string, args ...interface{}) (*Result, error) {
	if len(args) == 0 {
		return c.exec(command)
	} else {
		if s, err := c.Prepare(command); err != nil {
			return nil, errors.Trace(err)
		} else {
			var r *Result
			r, err = s.Execute(args...)
			s.Close()
			return r, err
		}
	}
}

// ExecuteMultiple will call perResultCallback for every result of the multiple queries
// that are executed.
//
// When ExecuteMultiple is used, the connection should have the SERVER_MORE_RESULTS_EXISTS
// flag set to signal the server multiple queries are executed. Handling the responses
// is up to the implementation of perResultCallback.
//
// Example:
//
// queries := "SELECT 1; SELECT NOW();"
// conn.ExecuteMultiple(queries, func(result *mysql.Result, err error) {
// // Use the result as you want
// })
func (c *Conn) ExecuteMultiple(query string, perResultCallback ExecPerResultCallback) (*Result, error) {
	if err := c.writeCommandStr(COM_QUERY, query); err != nil {
		return nil, errors.Trace(err)
	}

	var err error
	var result *Result

	bs := utils.ByteSliceGet(16)
	defer utils.ByteSlicePut(bs)

	for {
		bs.B, err = c.ReadPacketReuseMem(bs.B[:0])
		if err != nil {
			return nil, errors.Trace(err)
		}

		switch bs.B[0] {
		case OK_HEADER:
			result, err = c.handleOKPacket(bs.B)
		case ERR_HEADER:
			err = c.handleErrorPacket(bytes.Repeat(bs.B, 1))
			result = nil
		case LocalInFile_HEADER:
			err = ErrMalformPacket
			result = nil
		default:
			result, err = c.readResultset(bs.B, false)
		}
		// call user-defined callback
		perResultCallback(result, err)

		// if there was an error of this was the last result, stop looping
		if err != nil || result.Status&SERVER_MORE_RESULTS_EXISTS == 0 {
			break
		}
	}

	// return an empty result(set) signaling we're done streaming a multiple
	// streaming session
	// if this would end up in WriteValue, it would just be ignored as all
	// responses should have been handled in perResultCallback
	return &Result{Resultset: &Resultset{
		Streaming:     StreamingMultiple,
		StreamingDone: true,
	}}, nil
}

// ExecuteSelectStreaming will call perRowCallback for every row in resultset
// WITHOUT saving any row data to Result.{Values/RawPkg/RowDatas} fields.
// When given, perResultCallback will be called once per result
//
// ExecuteSelectStreaming should be used only for SELECT queries with a large response resultset for memory preserving.
//
// Example:
//
// var result mysql.Result
// conn.ExecuteSelectStreaming(`SELECT ... LIMIT 100500`, &result, func(row []mysql.FieldValue) error {
// // Use the row as you want.
// // You must not save FieldValue.AsString() value after this callback is done. Copy it if you need.
// return nil
// }, nil)
func (c *Conn) ExecuteSelectStreaming(command string, result *Result, perRowCallback SelectPerRowCallback, perResultCallback SelectPerResultCallback) error {
	if err := c.writeCommandStr(COM_QUERY, command); err != nil {
		return errors.Trace(err)
	}

	return c.readResultStreaming(false, result, perRowCallback, perResultCallback)
}

func (c *Conn) Begin() error {
	_, err := c.exec("BEGIN")
	return errors.Trace(err)
}

func (c *Conn) Commit() error {
	_, err := c.exec("COMMIT")
	return errors.Trace(err)
}

func (c *Conn) Rollback() error {
	_, err := c.exec("ROLLBACK")
	return errors.Trace(err)
}

func (c *Conn) SetAttributes(attributes map[string]string) {
	for k, v := range attributes {
		c.attributes[k] = v
	}
}

func (c *Conn) SetCharset(charset string) error {
	if c.charset == charset {
		return nil
	}

	if _, err := c.exec(fmt.Sprintf("SET NAMES %s", charset)); err != nil {
		return errors.Trace(err)
	} else {
		c.charset = charset
		return nil
	}
}

func (c *Conn) FieldList(table string, wildcard string) ([]*Field, error) {
	if err := c.writeCommandStrStr(COM_FIELD_LIST, table, wildcard); err != nil {
		return nil, errors.Trace(err)
	}

	fs := make([]*Field, 0, 4)
	var f *Field
	for {
		data, err := c.ReadPacket()
		if err != nil {
			return nil, errors.Trace(err)
		}

		// ERR Packet
		if data[0] == ERR_HEADER {
			return nil, c.handleErrorPacket(data)
		}

		// EOF Packet
		if c.isEOFPacket(data) {
			return fs, nil
		}

		if f, err = FieldData(data).Parse(); err != nil {
			return nil, errors.Trace(err)
		}
		fs = append(fs, f)
	}
}

func (c *Conn) SetAutoCommit() error {
	if !c.IsAutoCommit() {
		if _, err := c.exec("SET AUTOCOMMIT = 1"); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (c *Conn) IsAutoCommit() bool {
	return c.status&SERVER_STATUS_AUTOCOMMIT > 0
}

func (c *Conn) IsInTransaction() bool {
	return c.status&SERVER_STATUS_IN_TRANS > 0
}

func (c *Conn) GetCharset() string {
	return c.charset
}

func (c *Conn) GetConnectionID() uint32 {
	return c.connectionID
}

func (c *Conn) HandleOKPacket(data []byte) *Result {
	r, _ := c.handleOKPacket(data)
	return r
}

func (c *Conn) HandleErrorPacket(data []byte) error {
	return c.handleErrorPacket(data)
}

func (c *Conn) ReadOKPacket() (*Result, error) {
	return c.readOK()
}

func (c *Conn) exec(query string) (*Result, error) {
	if err := c.writeCommandStr(COM_QUERY, query); err != nil {
		return nil, errors.Trace(err)
	}

	return c.readResult(false)
}

func (c *Conn) CapabilityString() string {
	var caps []string
	capability := c.capability
	for i := 0; capability != 0; i++ {
		field := uint32(1 << i)
		if capability&field == 0 {
			continue
		}
		capability ^= field

		switch field {
		case CLIENT_LONG_PASSWORD:
			caps = append(caps, "CLIENT_LONG_PASSWORD")
		case CLIENT_FOUND_ROWS:
			caps = append(caps, "CLIENT_FOUND_ROWS")
		case CLIENT_LONG_FLAG:
			caps = append(caps, "CLIENT_LONG_FLAG")
		case CLIENT_CONNECT_WITH_DB:
			caps = append(caps, "CLIENT_CONNECT_WITH_DB")
		case CLIENT_NO_SCHEMA:
			caps = append(caps, "CLIENT_NO_SCHEMA")
		case CLIENT_COMPRESS:
			caps = append(caps, "CLIENT_COMPRESS")
		case CLIENT_ODBC:
			caps = append(caps, "CLIENT_ODBC")
		case CLIENT_LOCAL_FILES:
			caps = append(caps, "CLIENT_LOCAL_FILES")
		case CLIENT_IGNORE_SPACE:
			caps = append(caps, "CLIENT_IGNORE_SPACE")
		case CLIENT_PROTOCOL_41:
			caps = append(caps, "CLIENT_PROTOCOL_41")
		case CLIENT_INTERACTIVE:
			caps = append(caps, "CLIENT_INTERACTIVE")
		case CLIENT_SSL:
			caps = append(caps, "CLIENT_SSL")
		case CLIENT_IGNORE_SIGPIPE:
			caps = append(caps, "CLIENT_IGNORE_SIGPIPE")
		case CLIENT_TRANSACTIONS:
			caps = append(caps, "CLIENT_TRANSACTIONS")
		case CLIENT_RESERVED:
			caps = append(caps, "CLIENT_RESERVED")
		case CLIENT_SECURE_CONNECTION:
			caps = append(caps, "CLIENT_SECURE_CONNECTION")
		case CLIENT_MULTI_STATEMENTS:
			caps = append(caps, "CLIENT_MULTI_STATEMENTS")
		case CLIENT_MULTI_RESULTS:
			caps = append(caps, "CLIENT_MULTI_RESULTS")
		case CLIENT_PS_MULTI_RESULTS:
			caps = append(caps, "CLIENT_PS_MULTI_RESULTS")
		case CLIENT_PLUGIN_AUTH:
			caps = append(caps, "CLIENT_PLUGIN_AUTH")
		case CLIENT_CONNECT_ATTRS:
			caps = append(caps, "CLIENT_CONNECT_ATTRS")
		case CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA:
			caps = append(caps, "CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA")
		case CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS:
			caps = append(caps, "CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS")
		case CLIENT_SESSION_TRACK:
			caps = append(caps, "CLIENT_SESSION_TRACK")
		case CLIENT_DEPRECATE_EOF:
			caps = append(caps, "CLIENT_DEPRECATE_EOF")
		case CLIENT_OPTIONAL_RESULTSET_METADATA:
			caps = append(caps, "CLIENT_OPTIONAL_RESULTSET_METADATA")
		case CLIENT_ZSTD_COMPRESSION_ALGORITHM:
			caps = append(caps, "CLIENT_ZSTD_COMPRESSION_ALGORITHM")
		case CLIENT_QUERY_ATTRIBUTES:
			caps = append(caps, "CLIENT_QUERY_ATTRIBUTES")
		case MULTI_FACTOR_AUTHENTICATION:
			caps = append(caps, "MULTI_FACTOR_AUTHENTICATION")
		case CLIENT_CAPABILITY_EXTENSION:
			caps = append(caps, "CLIENT_CAPABILITY_EXTENSION")
		case CLIENT_SSL_VERIFY_SERVER_CERT:
			caps = append(caps, "CLIENT_SSL_VERIFY_SERVER_CERT")
		case CLIENT_REMEMBER_OPTIONS:
			caps = append(caps, "CLIENT_REMEMBER_OPTIONS")
		default:
			caps = append(caps, fmt.Sprintf("(%d)", field))
		}
	}

	return strings.Join(caps, "|")
}

func (c *Conn) StatusString() string {
	var stats []string
	status := c.status
	for i := 0; status != 0; i++ {
		field := uint16(1 << i)
		if status&field == 0 {
			continue
		}
		status ^= field

		switch field {
		case SERVER_STATUS_IN_TRANS:
			stats = append(stats, "SERVER_STATUS_IN_TRANS")
		case SERVER_STATUS_AUTOCOMMIT:
			stats = append(stats, "SERVER_STATUS_AUTOCOMMIT")
		case SERVER_MORE_RESULTS_EXISTS:
			stats = append(stats, "SERVER_MORE_RESULTS_EXISTS")
		case SERVER_STATUS_NO_GOOD_INDEX_USED:
			stats = append(stats, "SERVER_STATUS_NO_GOOD_INDEX_USED")
		case SERVER_STATUS_NO_INDEX_USED:
			stats = append(stats, "SERVER_STATUS_NO_INDEX_USED")
		case SERVER_STATUS_CURSOR_EXISTS:
			stats = append(stats, "SERVER_STATUS_CURSOR_EXISTS")
		case SERVER_STATUS_LAST_ROW_SEND:
			stats = append(stats, "SERVER_STATUS_LAST_ROW_SEND")
		case SERVER_STATUS_DB_DROPPED:
			stats = append(stats, "SERVER_STATUS_DB_DROPPED")
		case SERVER_STATUS_NO_BACKSLASH_ESCAPED:
			stats = append(stats, "SERVER_STATUS_NO_BACKSLASH_ESCAPED")
		case SERVER_STATUS_METADATA_CHANGED:
			stats = append(stats, "SERVER_STATUS_METADATA_CHANGED")
		case SERVER_QUERY_WAS_SLOW:
			stats = append(stats, "SERVER_QUERY_WAS_SLOW")
		case SERVER_PS_OUT_PARAMS:
			stats = append(stats, "SERVER_PS_OUT_PARAMS")
		default:
			stats = append(stats, fmt.Sprintf("(%d)", field))
		}
	}

	return strings.Join(stats, "|")
}
