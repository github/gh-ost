package client

import (
	"bytes"
	"crypto/rsa"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"fmt"

	"github.com/pingcap/errors"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/utils"
)

func (c *Conn) isEOFPacket(data []byte) bool {
	// 0xffffff due to https://dev.mysql.com/worklog/task/?id=7766
	// "Server will never send OK packet longer than 16777216 bytes thus limiting
	// size of OK packet to be 16777215 bytes"
	return data[0] == mysql.EOF_HEADER && len(data) <= 0xffffff
}

func (c *Conn) handleOKPacket(data []byte) (*mysql.Result, error) {
	var n int
	pos := 1

	r := mysql.NewResultReserveResultset(0)

	r.AffectedRows, _, n = mysql.LengthEncodedInt(data[pos:])
	pos += n
	r.InsertId, _, n = mysql.LengthEncodedInt(data[pos:])
	pos += n

	if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		r.Status = binary.LittleEndian.Uint16(data[pos:])
		c.status = r.Status
		pos += 2

		//todo:strict_mode, check warnings as error
		r.Warnings = binary.LittleEndian.Uint16(data[pos:])
		pos += 2
	} else if c.capability&mysql.CLIENT_TRANSACTIONS > 0 {
		r.Status = binary.LittleEndian.Uint16(data[pos:])
		c.status = r.Status
		pos += 2
	}

	if (c.capability&mysql.CLIENT_SESSION_TRACK > 0) &&
		(c.status&mysql.SERVER_SESSION_STATE_CHANGED > 0) {
		var err error

		// Example status message:
		// "Records: 3  Duplicates: 0  Warnings: 0"
		statusMessageLength := int(data[pos])
		pos++
		if statusMessageLength > 0 {
			r.StatusMessage = utils.ByteSliceToString(data[pos : pos+statusMessageLength])
			pos += statusMessageLength
		}

		sessionTrackingChangeLength := int(data[pos])
		pos++
		dataLength := len(data[pos:])
		if dataLength != sessionTrackingChangeLength {
			return nil, fmt.Errorf("incorrect data length for session tracking data: expected %d but got %d",
				sessionTrackingChangeLength, dataLength)
		}
		r.SessionTracking, err = decodeSessionTracking(data[pos:])
		if err != nil {
			return nil, err
		}
	}

	// skip info
	return r, nil
}

func decodeSessionTracking(data []byte) (s *mysql.SessionTrackingInfo, err error) {
	s = &mysql.SessionTrackingInfo{}
	pos := 0
	for pos < len(data) {
		sessionTrackingChangeType := data[pos]
		pos++ // session tracking type
		pos++ // length of session tracking data, unused

		switch sessionTrackingChangeType {
		case mysql.SESSION_TRACK_SYSTEM_VARIABLES:
			if s.Variables == nil {
				s.Variables = make(map[string]string, 1)
			}
			varNameLength := data[pos]
			pos++
			varName := utils.ByteSliceToString(data[pos : pos+int(varNameLength)])
			pos += int(varNameLength)
			varValueLength := data[pos]
			pos++
			s.Variables[varName] = utils.ByteSliceToString(data[pos : pos+int(varValueLength)])
			pos += int(varValueLength)
		case mysql.SESSION_TRACK_SCHEMA:
			schemaInfoLength := data[pos]
			pos++
			s.Schema = utils.ByteSliceToString(data[pos : pos+int(schemaInfoLength)])
			pos += int(schemaInfoLength)
		case mysql.SESSION_TRACK_STATE_CHANGE:
			s.State = string(data[pos])
			pos++
		case mysql.SESSION_TRACK_GTIDS:
			gtidFormat := data[pos]
			if gtidFormat != 0 {
				return nil, fmt.Errorf("unexpected GTID format %d", gtidFormat)
			}
			pos++
			gtidLength := data[pos]
			pos++
			s.GTID = utils.ByteSliceToString(data[pos : pos+int(gtidLength)])
			pos += int(gtidLength)
		case mysql.SESSION_TRACK_TRANSACTION_CHARACTERISTICS:
			characteristicsLength := data[pos]
			pos++
			if characteristicsLength > 0 {
				s.Characteristics = utils.ByteSliceToString(data[pos : pos+int(characteristicsLength)])
				pos += int(characteristicsLength)
			}
		case mysql.SESSION_TRACK_TRANSACTION_STATE:
			transactionStateLength := data[pos]
			pos++
			s.TransactionState = utils.ByteSliceToString(data[pos : pos+int(transactionStateLength)])
			pos += int(transactionStateLength)
		default:
			return nil, fmt.Errorf("got unknown change type %v", sessionTrackingChangeType)
		}
	}

	return s, nil
}

func (c *Conn) handleErrorPacket(data []byte) error {
	e := new(mysql.MyError)

	pos := 1

	e.Code = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		// skip '#'
		pos++
		e.State = utils.ByteSliceToString(data[pos : pos+5])
		pos += 5
	}

	e.Message = utils.ByteSliceToString(data[pos:])

	return e
}

func (c *Conn) handleAuthResult() error {
	data, switchToPlugin, err := c.readAuthResult()
	if err != nil {
		return fmt.Errorf("readAuthResult: %w", err)
	}
	// handle auth switch, only support 'sha256_password', and 'caching_sha2_password'
	if switchToPlugin != "" {
		// fmt.Printf("now switching auth plugin to '%s'\n", switchToPlugin)
		if data == nil {
			data = c.salt
		} else {
			copy(c.salt, data)
		}
		c.authPluginName = switchToPlugin
		auth, addNull, err := c.genAuthResponse(data)
		if err != nil {
			return err
		}

		if err = c.WriteAuthSwitchPacket(auth, addNull); err != nil {
			return err
		}

		// Read Result Packet
		data, switchToPlugin, err = c.readAuthResult()
		if err != nil {
			return err
		}

		// Do not allow to change the auth plugin more than once
		if switchToPlugin != "" {
			return errors.Errorf("can not switch auth plugin more than once")
		}
	}

	// handle caching_sha2_password
	switch c.authPluginName {
	case mysql.AUTH_CACHING_SHA2_PASSWORD:
		if data == nil {
			return nil // auth already succeeded
		}
		switch data[0] {
		case mysql.CACHE_SHA2_FAST_AUTH:
			_, err = c.readOK()
			return err
		case mysql.CACHE_SHA2_FULL_AUTH:
			// need full authentication
			if c.tlsConfig != nil || c.proto == "unix" {
				if err = c.WriteClearAuthPacket(c.password); err != nil {
					return err
				}
			} else {
				if err = c.WritePublicKeyAuthPacket(c.password, c.salt); err != nil {
					return err
				}
			}
			_, err = c.readOK()
			return err
		default:
			return errors.Errorf("invalid packet %x", data[0])
		}
	case mysql.AUTH_SHA256_PASSWORD:
		if len(data) == 0 {
			return nil // auth already succeeded
		}
		block, _ := pem.Decode(data)
		pub, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return err
		}
		// send encrypted password
		err = c.WriteEncryptedPassword(c.password, c.salt, pub.(*rsa.PublicKey))
		if err != nil {
			return err
		}
		_, err = c.readOK()
		return err
	}
	return nil
}

func (c *Conn) readAuthResult() ([]byte, string, error) {
	data, err := c.ReadPacket()
	if err != nil {
		return nil, "", fmt.Errorf("ReadPacket: %w", err)
	}

	// see: https://insidemysql.com/preparing-your-community-connector-for-mysql-8-part-2-sha256/
	// packet indicator
	switch data[0] {
	case mysql.OK_HEADER:
		_, err := c.handleOKPacket(data)
		return nil, "", err

	case mysql.MORE_DATE_HEADER:
		return data[1:], "", err

	case mysql.EOF_HEADER:
		// server wants to switch auth
		if len(data) < 1 {
			// https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::OldAuthSwitchRequest
			return nil, mysql.AUTH_MYSQL_OLD_PASSWORD, nil
		}
		pluginEndIndex := bytes.IndexByte(data, 0x00)
		if pluginEndIndex < 0 {
			return nil, "", errors.New("invalid packet")
		}
		plugin := string(data[1:pluginEndIndex])
		authData := data[pluginEndIndex+1:]
		return authData, plugin, nil

	default: // Error otherwise
		return nil, "", c.handleErrorPacket(data)
	}
}

func (c *Conn) readOK() (*mysql.Result, error) {
	data, err := c.ReadPacket()
	if err != nil {
		return nil, errors.Trace(err)
	}

	switch data[0] {
	case mysql.OK_HEADER:
		return c.handleOKPacket(data)
	case mysql.ERR_HEADER:
		return nil, c.handleErrorPacket(data)
	default:
		return nil, errors.New("invalid ok packet")
	}
}

func (c *Conn) readResult(binary bool) (*mysql.Result, error) {
	bs := utils.ByteSliceGet(16)
	defer utils.ByteSlicePut(bs)
	var err error
	bs.B, err = c.ReadPacketReuseMem(bs.B[:0])
	if err != nil {
		return nil, errors.Trace(err)
	}

	switch bs.B[0] {
	case mysql.OK_HEADER:
		return c.handleOKPacket(bs.B)
	case mysql.ERR_HEADER:
		return nil, c.handleErrorPacket(bytes.Repeat(bs.B, 1))
	case mysql.LocalInFile_HEADER:
		return nil, mysql.ErrMalformPacket
	default:
		return c.readResultset(bs.B, binary)
	}
}

func (c *Conn) readResultStreaming(binary bool, result *mysql.Result, perRowCb SelectPerRowCallback, perResCb SelectPerResultCallback) error {
	bs := utils.ByteSliceGet(16)
	defer utils.ByteSlicePut(bs)
	var err error
	bs.B, err = c.ReadPacketReuseMem(bs.B[:0])
	if err != nil {
		return errors.Trace(err)
	}

	switch bs.B[0] {
	case mysql.OK_HEADER:
		// https://dev.mysql.com/doc/internals/en/com-query-response.html
		// 14.6.4.1 COM_QUERY Response
		// If the number of columns in the resultset is 0, this is a OK_Packet.

		okResult, err := c.handleOKPacket(bs.B)
		if err != nil {
			return errors.Trace(err)
		}

		result.Status = okResult.Status
		result.AffectedRows = okResult.AffectedRows
		result.InsertId = okResult.InsertId
		result.Warnings = okResult.Warnings
		if result.Resultset == nil {
			result.Resultset = mysql.NewResultset(0)
		} else {
			result.Reset(0)
		}
		return nil
	case mysql.ERR_HEADER:
		return c.handleErrorPacket(bytes.Repeat(bs.B, 1))
	case mysql.LocalInFile_HEADER:
		return mysql.ErrMalformPacket
	default:
		return c.readResultsetStreaming(bs.B, binary, result, perRowCb, perResCb)
	}
}

func (c *Conn) readResultset(data []byte, binary bool) (*mysql.Result, error) {
	// column count
	count, _, n := mysql.LengthEncodedInt(data)

	if n-len(data) != 0 {
		return nil, mysql.ErrMalformPacket
	}

	result := mysql.NewResultReserveResultset(int(count))

	if err := c.readResultColumns(result); err != nil {
		return nil, errors.Trace(err)
	}

	if err := c.readResultRows(result, binary); err != nil {
		return nil, errors.Trace(err)
	}

	return result, nil
}

func (c *Conn) readResultsetStreaming(data []byte, binary bool, result *mysql.Result, perRowCb SelectPerRowCallback, perResCb SelectPerResultCallback) error {
	columnCount, _, n := mysql.LengthEncodedInt(data)

	if n-len(data) != 0 {
		return mysql.ErrMalformPacket
	}

	if result.Resultset == nil {
		result.Resultset = mysql.NewResultset(int(columnCount))
	} else {
		// Reuse memory if can
		result.Reset(int(columnCount))
	}

	// this is a streaming resultset
	result.Streaming = mysql.StreamingSelect

	if err := c.readResultColumns(result); err != nil {
		return errors.Trace(err)
	}

	if perResCb != nil {
		if err := perResCb(result); err != nil {
			return err
		}
	}

	if err := c.readResultRowsStreaming(result, binary, perRowCb); err != nil {
		return errors.Trace(err)
	}

	// this resultset is done streaming
	result.StreamingDone = true

	return nil
}

func (c *Conn) readResultColumns(result *mysql.Result) (err error) {
	var data []byte

	for i := range result.Fields {
		rawPkgLen := len(result.RawPkg)
		result.RawPkg, err = c.ReadPacketReuseMem(result.RawPkg)
		if err != nil {
			return err
		}
		data = result.RawPkg[rawPkgLen:]

		if result.Fields[i] == nil {
			result.Fields[i] = &mysql.Field{}
		}
		err = result.Fields[i].Parse(data)
		if err != nil {
			return err
		}

		result.FieldNames[utils.ByteSliceToString(result.Fields[i].Name)] = i
	}

	if c.capability&mysql.CLIENT_DEPRECATE_EOF == 0 {
		// EOF Packet
		rawPkgLen := len(result.RawPkg)
		result.RawPkg, err = c.ReadPacketReuseMem(result.RawPkg)
		if err != nil {
			return err
		}
		data = result.RawPkg[rawPkgLen:]

		if c.isEOFPacket(data) {
			if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
				result.Warnings = binary.LittleEndian.Uint16(data[1:])
				// todo add strict_mode, warning will be treat as error
				result.Status = binary.LittleEndian.Uint16(data[3:])
				c.status = result.Status
			}
			return nil
		}
		return mysql.ErrMalformPacket
	}
	return nil
}

func (c *Conn) readResultRows(result *mysql.Result, isBinary bool) (err error) {
	var data []byte

	for {
		rawPkgLen := len(result.RawPkg)
		result.RawPkg, err = c.ReadPacketReuseMem(result.RawPkg)
		if err != nil {
			return err
		}
		data = result.RawPkg[rawPkgLen:]

		if c.isEOFPacket(data) {
			if c.capability&mysql.CLIENT_DEPRECATE_EOF != 0 {
				// Treat like OK
				affectedRows, _, n := mysql.LengthEncodedInt(data[1:])
				insertID, _, m := mysql.LengthEncodedInt(data[1+n:])
				result.Status = binary.LittleEndian.Uint16(data[1+n+m:])
				result.AffectedRows = affectedRows
				result.InsertId = insertID
				c.status = result.Status
			} else if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
				result.Warnings = binary.LittleEndian.Uint16(data[1:])
				// todo add strict_mode, warning will be treat as error
				result.Status = binary.LittleEndian.Uint16(data[3:])
				c.status = result.Status
			}
			break
		}

		if data[0] == mysql.ERR_HEADER {
			return c.handleErrorPacket(data)
		}

		result.RowDatas = append(result.RowDatas, data)
	}

	if cap(result.Values) < len(result.RowDatas) {
		result.Values = make([][]mysql.FieldValue, len(result.RowDatas))
	} else {
		result.Values = result.Values[:len(result.RowDatas)]
	}

	for i := range result.Values {
		result.Values[i], err = result.RowDatas[i].Parse(result.Fields, isBinary, result.Values[i])
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (c *Conn) readResultRowsStreaming(result *mysql.Result, isBinary bool, perRowCb SelectPerRowCallback) (err error) {
	var (
		data []byte
		row  []mysql.FieldValue
	)

	for {
		data, err = c.ReadPacketReuseMem(data[:0])
		if err != nil {
			return err
		}

		if c.isEOFPacket(data) {
			if c.capability&mysql.CLIENT_DEPRECATE_EOF != 0 {
				// Treat like OK
				affectedRows, _, n := mysql.LengthEncodedInt(data[1:])
				insertID, _, m := mysql.LengthEncodedInt(data[1+n:])
				result.Status = binary.LittleEndian.Uint16(data[1+n+m:])
				result.AffectedRows = affectedRows
				result.InsertId = insertID
				c.status = result.Status
			} else if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
				result.Warnings = binary.LittleEndian.Uint16(data[1:])
				// todo add strict_mode, warning will be treat as error
				result.Status = binary.LittleEndian.Uint16(data[3:])
				c.status = result.Status
			}

			break
		}

		if data[0] == mysql.ERR_HEADER {
			return c.handleErrorPacket(data)
		}

		// Parse this row
		row, err = mysql.RowData(data).Parse(result.Fields, isBinary, row)
		if err != nil {
			return errors.Trace(err)
		}

		// Send the row to "userland" code
		err = perRowCb(row)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}
