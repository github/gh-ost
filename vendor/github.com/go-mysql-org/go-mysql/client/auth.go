package client

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"fmt"

	. "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/packet"
	"github.com/pingcap/errors"
)

const defaultAuthPluginName = AUTH_NATIVE_PASSWORD

// defines the supported auth plugins
var supportedAuthPlugins = []string{AUTH_NATIVE_PASSWORD, AUTH_SHA256_PASSWORD, AUTH_CACHING_SHA2_PASSWORD}

// helper function to determine what auth methods are allowed by this client
func authPluginAllowed(pluginName string) bool {
	for _, p := range supportedAuthPlugins {
		if pluginName == p {
			return true
		}
	}
	return false
}

// See:
//   - https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_v10.html
//   - https://github.com/alibaba/canal/blob/0ec46991499a22870dde4ae736b2586cbcbfea94/driver/src/main/java/com/alibaba/otter/canal/parse/driver/mysql/packets/server/HandshakeInitializationPacket.java#L89
//   - https://github.com/vapor/mysql-nio/blob/main/Sources/MySQLNIO/Protocol/MySQLProtocol%2BHandshakeV10.swift
//   - https://github.com/github/vitess-gh/blob/70ae1a2b3a116ff6411b0f40852d6e71382f6e07/go/mysql/client.go
func (c *Conn) readInitialHandshake() error {
	data, err := c.ReadPacket()
	if err != nil {
		return errors.Trace(err)
	}

	if data[0] == ERR_HEADER {
		return errors.Annotate(c.handleErrorPacket(data), "read initial handshake error")
	}

	if data[0] < MinProtocolVersion {
		return errors.Errorf("invalid protocol version %d, must >= 10", data[0])
	}
	pos := 1

	// skip mysql version
	// mysql version end with 0x00
	version := data[pos : bytes.IndexByte(data[pos:], 0x00)+1]
	c.serverVersion = string(version)
	pos += len(version) + 1 /*trailing zero byte*/

	// connection id length is 4
	c.connectionID = binary.LittleEndian.Uint32(data[pos : pos+4])
	pos += 4

	// first 8 bytes of the plugin provided data (scramble)
	c.salt = append(c.salt[:0], data[pos:pos+8]...)
	pos += 8

	if data[pos] != 0 { // 	0x00 byte, terminating the first part of a scramble
		return errors.Errorf("expect 0x00 after scramble, got %q", rune(data[pos]))
	}
	pos++

	// The lower 2 bytes of the Capabilities Flags
	c.capability = uint32(binary.LittleEndian.Uint16(data[pos : pos+2]))
	// check protocol
	if c.capability&CLIENT_PROTOCOL_41 == 0 {
		return errors.New("the MySQL server can not support protocol 41 and above required by the client")
	}
	if c.capability&CLIENT_SSL == 0 && c.tlsConfig != nil {
		return errors.New("the MySQL Server does not support TLS required by the client")
	}
	pos += 2

	if len(data) > pos {
		// default server a_protocol_character_set, only the lower 8-bits
		// c.charset = data[pos]
		pos += 1

		c.status = binary.LittleEndian.Uint16(data[pos : pos+2])
		pos += 2

		// The upper 2 bytes of the Capabilities Flags
		c.capability = uint32(binary.LittleEndian.Uint16(data[pos:pos+2]))<<16 | c.capability
		pos += 2

		// length of the combined auth_plugin_data (scramble), if auth_plugin_data_len is > 0
		authPluginDataLen := data[pos]
		if (c.capability&CLIENT_PLUGIN_AUTH == 0) && (authPluginDataLen > 0) {
			return errors.Errorf("invalid auth plugin data filler %d", authPluginDataLen)
		}
		pos++

		// skip reserved (all [00] ?)
		pos += 10

		if c.capability&CLIENT_SECURE_CONNECTION != 0 {
			// Rest of the plugin provided data (scramble)

			// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_v10.html
			// $len=MAX(13, length of auth-plugin-data - 8)
			//
			// https://github.com/mysql/mysql-server/blob/1bfe02bdad6604d54913c62614bde57a055c8332/sql/auth/sql_authentication.cc#L1641-L1642
			// the first packet *must* have at least 20 bytes of a scramble.
			// if a plugin provided less, we pad it to 20 with zeros
			rest := int(authPluginDataLen) - 8
			if max := 12 + 1; rest < max {
				rest = max
			}

			authPluginDataPart2 := data[pos : pos+rest]
			pos += rest

			c.salt = append(c.salt, authPluginDataPart2...)
		}

		if c.capability&CLIENT_PLUGIN_AUTH != 0 {
			c.authPluginName = string(data[pos : pos+bytes.IndexByte(data[pos:], 0x00)])
			pos += len(c.authPluginName)

			if data[pos] != 0 {
				return errors.Errorf("expect 0x00 after authPluginName, got %q", rune(data[pos]))
			}
			// pos++ // ineffectual
		}
	}

	// if server gives no default auth plugin name, use a client default
	if c.authPluginName == "" {
		c.authPluginName = defaultAuthPluginName
	}

	return nil
}

// generate auth response data according to auth plugin
//
// NOTE: the returned boolean value indicates whether to add a \NUL to the end of data.
// it is quite tricky because MySQL server expects different formats of responses in different auth situations.
// here the \NUL needs to be added when sending back the empty password or cleartext password in 'sha256_password'
// authentication.
func (c *Conn) genAuthResponse(authData []byte) ([]byte, bool, error) {
	// password hashing
	switch c.authPluginName {
	case AUTH_NATIVE_PASSWORD:
		return CalcPassword(authData[:20], []byte(c.password)), false, nil
	case AUTH_CACHING_SHA2_PASSWORD:
		return CalcCachingSha2Password(authData, c.password), false, nil
	case AUTH_CLEAR_PASSWORD:
		return []byte(c.password), true, nil
	case AUTH_SHA256_PASSWORD:
		if len(c.password) == 0 {
			return nil, true, nil
		}
		if c.tlsConfig != nil || c.proto == "unix" {
			// write cleartext auth packet
			// see: https://dev.mysql.com/doc/refman/8.0/en/sha256-pluggable-authentication.html
			return []byte(c.password), true, nil
		} else {
			// request public key from server
			// see: https://dev.mysql.com/doc/internals/en/public-key-retrieval.html
			return []byte{1}, false, nil
		}
	default:
		// not reachable
		return nil, false, fmt.Errorf("auth plugin '%s' is not supported", c.authPluginName)
	}
}

// generate connection attributes data
func (c *Conn) genAttributes() []byte {
	if len(c.attributes) == 0 {
		return nil
	}

	attrData := make([]byte, 0)
	for k, v := range c.attributes {
		attrData = append(attrData, PutLengthEncodedString([]byte(k))...)
		attrData = append(attrData, PutLengthEncodedString([]byte(v))...)
	}
	return append(PutLengthEncodedInt(uint64(len(attrData))), attrData...)
}

// See: http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse
func (c *Conn) writeAuthHandshake() error {
	if !authPluginAllowed(c.authPluginName) {
		return fmt.Errorf("unknow auth plugin name '%s'", c.authPluginName)
	}

	// Set default client capabilities that reflect the abilities of this library
	capability := CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION |
		CLIENT_LONG_PASSWORD | CLIENT_TRANSACTIONS | CLIENT_PLUGIN_AUTH
	// Adjust client capability flags based on server support
	capability |= c.capability & CLIENT_LONG_FLAG
	// Adjust client capability flags on specific client requests
	// Only flags that would make any sense setting and aren't handled elsewhere
	// in the library are supported here
	capability |= c.ccaps&CLIENT_FOUND_ROWS | c.ccaps&CLIENT_IGNORE_SPACE |
		c.ccaps&CLIENT_MULTI_STATEMENTS | c.ccaps&CLIENT_MULTI_RESULTS |
		c.ccaps&CLIENT_PS_MULTI_RESULTS | c.ccaps&CLIENT_CONNECT_ATTRS |
		c.ccaps&CLIENT_COMPRESS | c.ccaps&CLIENT_ZSTD_COMPRESSION_ALGORITHM |
		c.ccaps&CLIENT_LOCAL_FILES

	// To enable TLS / SSL
	if c.tlsConfig != nil {
		capability |= CLIENT_SSL
	}

	auth, addNull, err := c.genAuthResponse(c.salt)
	if err != nil {
		return err
	}

	// encode length of the auth plugin data
	// here we use the Length-Encoded-Integer(LEI) as the data length may not fit into one byte
	// see: https://dev.mysql.com/doc/internals/en/integer.html#length-encoded-integer
	var authRespLEIBuf [9]byte
	authRespLEI := AppendLengthEncodedInteger(authRespLEIBuf[:0], uint64(len(auth)))
	if len(authRespLEI) > 1 {
		// if the length can not be written in 1 byte, it must be written as a
		// length encoded integer
		capability |= CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
	}

	// packet length
	// capability 4
	// max-packet size 4
	// charset 1
	// reserved all[0] 23
	// username
	// auth
	// mysql_native_password + null-terminated
	length := 4 + 4 + 1 + 23 + len(c.user) + 1 + len(authRespLEI) + len(auth) + 21 + 1
	if addNull {
		length++
	}
	// db name
	if len(c.db) > 0 {
		capability |= CLIENT_CONNECT_WITH_DB
		length += len(c.db) + 1
	}
	// connection attributes
	attrData := c.genAttributes()
	if len(attrData) > 0 {
		capability |= CLIENT_CONNECT_ATTRS
		length += len(attrData)
	}
	if c.ccaps&CLIENT_ZSTD_COMPRESSION_ALGORITHM > 0 {
		length++
	}

	data := make([]byte, length+4)

	// capability [32 bit]
	data[4] = byte(capability)
	data[5] = byte(capability >> 8)
	data[6] = byte(capability >> 16)
	data[7] = byte(capability >> 24)

	// MaxPacketSize [32 bit] (none)
	data[8] = 0x00
	data[9] = 0x00
	data[10] = 0x00
	data[11] = 0x00

	// Charset [1 byte]
	// use default collation id 33 here, is utf-8
	data[12] = DEFAULT_COLLATION_ID

	// SSL Connection Request Packet
	// http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::SSLRequest
	if c.tlsConfig != nil {
		// Send TLS / SSL request packet
		if err := c.WritePacket(data[:(4+4+1+23)+4]); err != nil {
			return err
		}

		// Switch to TLS
		tlsConn := tls.Client(c.Conn.Conn, c.tlsConfig)
		if err := tlsConn.Handshake(); err != nil {
			return err
		}

		currentSequence := c.Sequence
		c.Conn = packet.NewConn(tlsConn)
		c.Sequence = currentSequence
	}

	// Filler [23 bytes] (all 0x00)
	pos := 13
	for ; pos < 13+23; pos++ {
		data[pos] = 0
	}

	// User [null terminated string]
	if len(c.user) > 0 {
		pos += copy(data[pos:], c.user)
	}
	data[pos] = 0x00
	pos++

	// auth [length encoded integer]
	pos += copy(data[pos:], authRespLEI)
	pos += copy(data[pos:], auth)
	if addNull {
		data[pos] = 0x00
		pos++
	}

	// db [null terminated string]
	if len(c.db) > 0 {
		pos += copy(data[pos:], c.db)
		data[pos] = 0x00
		pos++
	}

	// Assume native client during response
	pos += copy(data[pos:], c.authPluginName)
	data[pos] = 0x00
	pos++

	// connection attributes
	if len(attrData) > 0 {
		pos += copy(data[pos:], attrData)
	}

	if c.ccaps&CLIENT_ZSTD_COMPRESSION_ALGORITHM > 0 {
		// zstd_compression_level
		data[pos] = 0x03
	}

	return c.WritePacket(data)
}
