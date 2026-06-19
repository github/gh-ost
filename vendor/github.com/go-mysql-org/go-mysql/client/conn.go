package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"maps"
	"math/bits"
	"net"
	"runtime"
	"runtime/debug"
	"slices"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/charset"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/packet"
	"github.com/go-mysql-org/go-mysql/utils"
)

const defaultBufferSize = 65536 // 64kb

type Option func(*Conn) error

type Conn struct {
	*packet.Conn

	user      string
	password  string
	db        string
	tlsConfig *tls.Config
	proto     string

	// Connection read and write timeouts to set on the connection
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	// The buffer size to use in the packet connection
	BufferSize int

	serverVersion string
	// server capabilities
	capability uint32
	// client-set capabilities only
	ccaps uint32
	// Capability flags explicitly disabled by the client via UnsetCapability()
	// These flags are removed from the final advertised capability set during handshake.
	clientExplicitOffCaps uint32

	attributes map[string]string

	status uint16

	charset string
	// sets the collation to be set on the auth handshake, this does not issue a 'set names' command
	collation string

	salt           []byte
	authPluginName string

	connectionID uint32

	queryAttributes []mysql.QueryAttribute

	// Include the file + line as query attribute. The number set which frame in the stack should be used.
	includeLine int
}

// This function will be called for every row in resultset from ExecuteSelectStreaming.
type SelectPerRowCallback func(row []mysql.FieldValue) error

// This function will be called once per result from ExecuteSelectStreaming
type SelectPerResultCallback func(result *mysql.Result) error

// This function will be called once per result from ExecuteMultiple
type ExecPerResultCallback func(result *mysql.Result, err error)

func getNetProto(addr string) string {
	proto := "tcp"
	if strings.Contains(addr, "/") {
		proto = "unix"
	}
	return proto
}

// Connect to a MySQL server, addr can be ip:port, or a unix socket domain like /var/sock.
// Accepts a series of configuration functions as a variadic argument.
func Connect(addr, user, password, dbName string, options ...Option) (*Conn, error) {
	return ConnectWithTimeout(addr, user, password, dbName, time.Second*10, options...)
}

// ConnectWithTimeout to a MySQL address using a timeout.
func ConnectWithTimeout(addr, user, password, dbName string, timeout time.Duration, options ...Option) (*Conn, error) {
	return ConnectWithContext(context.Background(), addr, user, password, dbName, time.Second*10, options...)
}

// ConnectWithContext to a MySQL addr using the provided context.
func ConnectWithContext(ctx context.Context, addr, user, password, dbName string, timeout time.Duration, options ...Option) (*Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	return ConnectWithDialer(ctx, "", addr, user, password, dbName, dialer.DialContext, options...)
}

// Dialer connects to the address on the named network using the provided context.
type Dialer func(ctx context.Context, network, address string) (net.Conn, error)

// ConnectWithDialer to a MySQL server using the given Dialer.
func ConnectWithDialer(ctx context.Context, network, addr, user, password, dbName string, dialer Dialer, options ...Option) (*Conn, error) {
	c := new(Conn)

	c.includeLine = -1
	c.BufferSize = defaultBufferSize
	c.attributes = map[string]string{
		"_client_name":     "go-mysql",
		"_os":              runtime.GOOS,
		"_platform":        runtime.GOARCH,
		"_runtime_version": runtime.Version(),
	}
	if buildInfo, ok := debug.ReadBuildInfo(); ok {
		for _, bi := range buildInfo.Deps {
			if bi.Path == "github.com/go-mysql-org/go-mysql" {
				c.attributes["_client_version"] = bi.Version
				break
			}
		}
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

	// use default charset here, utf-8
	c.charset = mysql.DEFAULT_CHARSET

	// Apply configuration functions.
	for _, option := range options {
		if err := option(c); err != nil {
			// must close the connection in the event the provided configuration is not valid
			_ = conn.Close()
			return nil, err
		}
	}

	c.Conn = packet.NewConnWithTimeout(conn, c.ReadTimeout, c.WriteTimeout, c.BufferSize)
	if c.tlsConfig != nil {
		seq := c.Sequence
		c.Conn = packet.NewTLSConnWithTimeout(conn, c.ReadTimeout, c.WriteTimeout)
		c.Sequence = seq
	}

	if err = c.handshake(); err != nil {
		// in the event of an error c.handshake() will close the connection
		return nil, errors.Trace(err)
	}

	if c.ccaps&mysql.CLIENT_COMPRESS > 0 {
		c.Compression = mysql.MYSQL_COMPRESS_ZLIB
	} else if c.ccaps&mysql.CLIENT_ZSTD_COMPRESSION_ALGORITHM > 0 {
		c.Compression = mysql.MYSQL_COMPRESS_ZSTD
	}

	// if a collation was set with a ID of > 255, then we need to call SET NAMES ...
	// since the auth handshake response only support collations with 1-byte ids
	if len(c.collation) != 0 {
		collation, err := charset.GetCollationByName(c.collation)
		if err != nil {
			c.Close()
			return nil, errors.Trace(fmt.Errorf("invalid collation name %s", c.collation))
		}

		if collation.ID > 255 {
			if _, err := c.exec(fmt.Sprintf("SET NAMES %s COLLATE %s", c.charset, c.collation)); err != nil {
				c.Close()
				return nil, errors.Trace(err)
			}
		}
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

// Close directly closes the connection. Use Quit() to first send COM_QUIT to the server and then close the connection.
func (c *Conn) Close() error {
	return c.Conn.Close()
}

// Quit sends COM_QUIT to the server and then closes the connection. Use Close() to directly close the connection.
func (c *Conn) Quit() error {
	if err := c.writeCommand(mysql.COM_QUIT); err != nil {
		return err
	}
	return c.Close()
}

func (c *Conn) Ping() error {
	if err := c.writeCommand(mysql.COM_PING); err != nil {
		return errors.Trace(err)
	}

	if _, err := c.readOK(); err != nil {
		return errors.Trace(err)
	}

	return nil
}

// SetCapability marks the specified flag as explicitly enabled by the client.
func (c *Conn) SetCapability(capability uint32) error {
	if !slices.Contains(optionalCapabilities, capability) {
		return errors.New("unsupported or unknown capability")
	}
	c.ccaps |= capability
	c.clientExplicitOffCaps &^= capability
	return nil
}

// UnsetCapability marks the specified flag as explicitly disabled by the client.
// This disables the flag even if the server supports it.
func (c *Conn) UnsetCapability(capability uint32) {
	c.ccaps &^= capability
	c.clientExplicitOffCaps |= capability
}

// HasCapability returns true if the connection has the specific capability
func (c *Conn) HasCapability(capability uint32) bool {
	return c.ccaps&capability != 0
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
	_, err := c.UseDBWithResult(dbName)
	return err
}

func (c *Conn) UseDBWithResult(dbName string) (*mysql.Result, error) {
	if err := c.writeCommandStr(mysql.COM_INIT_DB, dbName); err != nil {
		return nil, errors.Trace(err)
	}

	var r *mysql.Result
	var err error
	if r, err = c.readOK(); err != nil {
		return r, errors.Trace(err)
	}

	c.db = dbName
	return r, nil
}

func (c *Conn) GetDB() string {
	return c.db
}

// GetServerVersion returns the version of the server as reported by the server
// in the initial server greeting.
func (c *Conn) GetServerVersion() string {
	return c.serverVersion
}

// CompareServerVersion is comparing version v against the version
// of the server and returns 0 if they are equal, and 1 if the server version
// is higher and -1 if the server version is lower.
func (c *Conn) CompareServerVersion(v string) (int, error) {
	return mysql.CompareServerVersions(c.serverVersion, v)
}

func (c *Conn) Execute(command string, args ...any) (*mysql.Result, error) {
	if len(args) == 0 {
		return c.exec(command)
	}
	s, err := c.Prepare(command)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var r *mysql.Result
	r, err = s.Execute(args...)
	s.Close()
	return r, err
}

// ExecuteMultiple will call perResultCallback for every result of the multiple queries
// that are executed.
//
// When ExecuteMultiple is used, the connection should have the SERVER_MORE_RESULTS_EXISTS
// flag set to signal the server multiple queries are executed. Handling the responses
// is up to the implementation of perResultCallback.
func (c *Conn) ExecuteMultiple(query string, perResultCallback ExecPerResultCallback) (*mysql.Result, error) {
	if err := c.execSend(query); err != nil {
		return nil, errors.Trace(err)
	}

	var err error
	var result *mysql.Result

	bs := utils.ByteSliceGet(16)
	defer utils.ByteSlicePut(bs)

	for {
		bs.B, err = c.ReadPacketReuseMem(bs.B[:0])
		if err != nil {
			return nil, errors.Trace(err)
		}

		switch bs.B[0] {
		case mysql.OK_HEADER:
			result, err = c.handleOKPacket(bs.B)
		case mysql.ERR_HEADER:
			err = c.handleErrorPacket(bytes.Repeat(bs.B, 1))
			result = nil
		case mysql.LocalInFile_HEADER:
			err = mysql.ErrMalformPacket
			result = nil
		default:
			result, err = c.readResultset(bs.B, false)
		}
		// call user-defined callback
		perResultCallback(result, err)

		// if there was an error of this was the last result, stop looping
		if err != nil || result.Status&mysql.SERVER_MORE_RESULTS_EXISTS == 0 {
			break
		}
	}

	// return an empty result(set) signaling we're done streaming a multiple
	// streaming session
	// if this would end up in WriteValue, it would just be ignored as all
	// responses should have been handled in perResultCallback
	rs := mysql.NewResultset(0)
	rs.Streaming = mysql.StreamingMultiple
	rs.StreamingDone = true
	return mysql.NewResult(rs), nil
}

// ExecuteSelectStreaming will call perRowCallback for every row in resultset
// WITHOUT saving any row data to Result.{Values/RawPkg/RowDatas} fields.
// When given, perResultCallback will be called once per result
//
// ExecuteSelectStreaming should be used only for SELECT queries with a large response resultset for memory preserving.
func (c *Conn) ExecuteSelectStreaming(command string, result *mysql.Result, perRowCallback SelectPerRowCallback, perResultCallback SelectPerResultCallback) error {
	if err := c.execSend(command); err != nil {
		return errors.Trace(err)
	}

	return c.readResultStreaming(false, result, perRowCallback, perResultCallback)
}

func (c *Conn) Begin() error {
	_, err := c.exec("BEGIN")
	return errors.Trace(err)
}

func (c *Conn) BeginTx(readOnly bool, txIsolation string) error {
	if txIsolation != "" {
		if _, err := c.exec("SET TRANSACTION ISOLATION LEVEL " + txIsolation); err != nil {
			return errors.Trace(err)
		}
	}
	var err error
	if readOnly {
		_, err = c.exec("START TRANSACTION READ ONLY")
	} else {
		_, err = c.exec("START TRANSACTION")
	}
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

// SetAttributes sets connection attributes
func (c *Conn) SetAttributes(attributes map[string]string) {
	maps.Copy(c.attributes, attributes)
}

func (c *Conn) SetCharset(charset string) error {
	if c.charset == charset {
		return nil
	}

	if _, err := c.exec(fmt.Sprintf("SET NAMES %s", charset)); err != nil {
		return errors.Trace(err)
	}
	c.charset = charset
	return nil
}

func (c *Conn) SetCollation(collation string) error {
	if len(c.serverVersion) != 0 {
		return errors.Trace(errors.Errorf("cannot set collation after connection is established"))
	}

	c.collation = collation
	return nil
}

func (c *Conn) GetCollation() string {
	return c.collation
}

// FieldList uses COM_FIELD_LIST to get a list of fields from a table
func (c *Conn) FieldList(table string, wildcard string) ([]*mysql.Field, error) {
	if err := c.writeCommandStrStr(mysql.COM_FIELD_LIST, table, wildcard); err != nil {
		return nil, errors.Trace(err)
	}

	fs := make([]*mysql.Field, 0, 4)
	var f *mysql.Field
	for {
		data, err := c.ReadPacket()
		if err != nil {
			return nil, errors.Trace(err)
		}

		// ERR Packet
		if data[0] == mysql.ERR_HEADER {
			return nil, c.handleErrorPacket(data)
		}

		// EOF Packet
		if c.isEOFPacket(data) {
			return fs, nil
		}

		if f, err = mysql.FieldData(data).Parse(); err != nil {
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

// IsAutoCommit returns true if SERVER_STATUS_AUTOCOMMIT is set
func (c *Conn) IsAutoCommit() bool {
	return c.status&mysql.SERVER_STATUS_AUTOCOMMIT > 0
}

// IsInTransaction returns true if SERVER_STATUS_IN_TRANS is set
func (c *Conn) IsInTransaction() bool {
	return c.status&mysql.SERVER_STATUS_IN_TRANS > 0
}

func (c *Conn) GetCharset() string {
	return c.charset
}

func (c *Conn) GetConnectionID() uint32 {
	return c.connectionID
}

func (c *Conn) HandleOKPacket(data []byte) *mysql.Result {
	r, _ := c.handleOKPacket(data)
	return r
}

func (c *Conn) HandleErrorPacket(data []byte) error {
	return c.handleErrorPacket(data)
}

func (c *Conn) ReadOKPacket() (*mysql.Result, error) {
	return c.readOK()
}

// Send COM_QUERY and read the result
func (c *Conn) exec(query string) (*mysql.Result, error) {
	err := c.execSend(query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return c.readResult(false)
}

// Sends COM_QUERY
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query.html
func (c *Conn) execSend(query string) error {
	var buf bytes.Buffer
	defer clear(c.queryAttributes)

	if c.capability&mysql.CLIENT_QUERY_ATTRIBUTES > 0 {
		if c.includeLine >= 0 {
			_, file, line, ok := runtime.Caller(c.includeLine)
			if ok {
				lineAttr := mysql.QueryAttribute{
					Name:  "_line",
					Value: fmt.Sprintf("%s:%d", file, line),
				}
				c.queryAttributes = append(c.queryAttributes, lineAttr)
			}
		}

		numParams := len(c.queryAttributes)
		buf.Write(mysql.PutLengthEncodedInt(uint64(numParams)))
		buf.WriteByte(0x1) // parameter_set_count, unused
		if numParams > 0 {
			// null_bitmap, length: (num_params+7)/8
			for i := 0; i < (numParams+7)/8; i++ {
				buf.WriteByte(0x0)
			}
			buf.WriteByte(0x1) // new_params_bind_flag, unused
			for _, qa := range c.queryAttributes {
				buf.Write(qa.TypeAndFlag())
				buf.Write(mysql.PutLengthEncodedString([]byte(qa.Name)))
			}
			for _, qa := range c.queryAttributes {
				buf.Write(qa.ValueBytes())
			}
		}
	}

	_, err := buf.Write(utils.StringToByteSlice(query))
	if err != nil {
		return err
	}

	if err := c.writeCommandBuf(mysql.COM_QUERY, buf.Bytes()); err != nil {
		return errors.Trace(err)
	}

	return nil
}

// CapabilityString is returning a string with the names of capability flags
// separated by "|". Examples of capability names are CLIENT_DEPRECATE_EOF and CLIENT_PROTOCOL_41.
// These are defined as constants in the mysql package.
func (c *Conn) CapabilityString() string {
	capability := c.capability
	caps := make([]string, 0, bits.OnesCount32(capability))
	for capability != 0 {
		field := uint32(1 << bits.TrailingZeros32(capability))
		capability ^= field

		if capname, ok := mysql.CapNames[field]; ok {
			caps = append(caps, capname)
		} else {
			caps = append(caps, fmt.Sprintf("(%d)", field))
		}
	}

	return strings.Join(caps, "|")
}

// StatusString returns a "|" separated list of status fields. Example status values are SERVER_QUERY_WAS_SLOW and SERVER_STATUS_AUTOCOMMIT.
// These are defined as constants in the mysql package.
func (c *Conn) StatusString() string {
	status := c.status
	stats := make([]string, 0, bits.OnesCount16(status))
	for status != 0 {
		field := uint16(1 << bits.TrailingZeros16(status))
		status ^= field

		switch field {
		case mysql.SERVER_STATUS_IN_TRANS:
			stats = append(stats, "SERVER_STATUS_IN_TRANS")
		case mysql.SERVER_STATUS_AUTOCOMMIT:
			stats = append(stats, "SERVER_STATUS_AUTOCOMMIT")
		case mysql.SERVER_MORE_RESULTS_EXISTS:
			stats = append(stats, "SERVER_MORE_RESULTS_EXISTS")
		case mysql.SERVER_STATUS_NO_GOOD_INDEX_USED:
			stats = append(stats, "SERVER_STATUS_NO_GOOD_INDEX_USED")
		case mysql.SERVER_STATUS_NO_INDEX_USED:
			stats = append(stats, "SERVER_STATUS_NO_INDEX_USED")
		case mysql.SERVER_STATUS_CURSOR_EXISTS:
			stats = append(stats, "SERVER_STATUS_CURSOR_EXISTS")
		case mysql.SERVER_STATUS_LAST_ROW_SEND:
			stats = append(stats, "SERVER_STATUS_LAST_ROW_SEND")
		case mysql.SERVER_STATUS_DB_DROPPED:
			stats = append(stats, "SERVER_STATUS_DB_DROPPED")
		case mysql.SERVER_STATUS_NO_BACKSLASH_ESCAPED:
			stats = append(stats, "SERVER_STATUS_NO_BACKSLASH_ESCAPED")
		case mysql.SERVER_STATUS_METADATA_CHANGED:
			stats = append(stats, "SERVER_STATUS_METADATA_CHANGED")
		case mysql.SERVER_QUERY_WAS_SLOW:
			stats = append(stats, "SERVER_QUERY_WAS_SLOW")
		case mysql.SERVER_PS_OUT_PARAMS:
			stats = append(stats, "SERVER_PS_OUT_PARAMS")
		default:
			stats = append(stats, fmt.Sprintf("(%d)", field))
		}
	}

	return strings.Join(stats, "|")
}

// SetQueryAttributes sets the query attributes to be send along with the next query
func (c *Conn) SetQueryAttributes(attrs ...mysql.QueryAttribute) error {
	c.queryAttributes = attrs
	return nil
}

// IncludeLine can be passed as option when connecting to include the file name and line number
// of the caller as query attribute `_line` when sending queries.
// The argument is used the dept in the stack. The top level is go-mysql and then there are the
// levels of the application.
func (c *Conn) IncludeLine(frame int) {
	c.includeLine = frame
}
