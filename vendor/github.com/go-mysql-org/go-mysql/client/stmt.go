package client

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"runtime"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/stmt"
	"github.com/go-mysql-org/go-mysql/utils"
	"github.com/pingcap/errors"
)

type Stmt struct {
	conn     *Conn
	warnings int

	// PreparedStmt contains common fields shared with server.Stmt for proxy passthrough
	stmt.PreparedStmt
}

func (s *Stmt) ParamNum() int {
	return s.Params
}

func (s *Stmt) ColumnNum() int {
	return s.Columns
}

func (s *Stmt) WarningsNum() int {
	return s.warnings
}

func (s *Stmt) Execute(args ...any) (*mysql.Result, error) {
	if err := s.write(args...); err != nil {
		return nil, errors.Trace(err)
	}

	return s.conn.readResult(true)
}

func (s *Stmt) ExecuteSelectStreaming(result *mysql.Result, perRowCb SelectPerRowCallback, perResCb SelectPerResultCallback, args ...any) error {
	if err := s.write(args...); err != nil {
		return errors.Trace(err)
	}

	return s.conn.readResultStreaming(true, result, perRowCb, perResCb)
}

func (s *Stmt) Close() error {
	if err := s.conn.writeCommandUint32(mysql.COM_STMT_CLOSE, s.ID); err != nil {
		return errors.Trace(err)
	}

	return nil
}

// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html
func (s *Stmt) write(args ...any) error {
	defer clear(s.conn.queryAttributes)
	paramsNum := s.Params

	if len(args) != paramsNum {
		return fmt.Errorf("argument mismatch, need %d but got %d", s.Params, len(args))
	}

	if (s.conn.capability&mysql.CLIENT_QUERY_ATTRIBUTES > 0) && (s.conn.includeLine >= 0) {
		_, file, line, ok := runtime.Caller(s.conn.includeLine)
		if ok {
			lineAttr := mysql.QueryAttribute{
				Name:  "_line",
				Value: fmt.Sprintf("%s:%d", file, line),
			}
			s.conn.queryAttributes = append(s.conn.queryAttributes, lineAttr)
		}
	}

	qaLen := len(s.conn.queryAttributes)
	paramTypes := make([][]byte, paramsNum+qaLen)
	paramFlags := make([][]byte, paramsNum+qaLen)
	paramValues := make([][]byte, paramsNum+qaLen)
	paramNames := make([][]byte, paramsNum+qaLen)

	// NULL-bitmap, length: (num-params+7)
	nullBitmap := make([]byte, (paramsNum+qaLen+7)>>3)

	length := 1 + 4 + 1 + 4 + ((paramsNum + 7) >> 3) + 1 + (paramsNum << 1)

	var newParamBoundFlag byte

	for i := range args {
		if args[i] == nil {
			nullBitmap[i/8] |= 1 << (uint(i) % 8)
			paramTypes[i] = []byte{mysql.MYSQL_TYPE_NULL}
			paramNames[i] = []byte{0} // length encoded, no name
			paramFlags[i] = []byte{0}
			continue
		}

		newParamBoundFlag = 1

		switch v := args[i].(type) {
		case int8:
			paramTypes[i] = []byte{mysql.MYSQL_TYPE_TINY}
			paramValues[i] = []byte{byte(v)}
		case int16:
			paramTypes[i] = []byte{mysql.MYSQL_TYPE_SHORT}
			paramValues[i] = mysql.Uint16ToBytes(uint16(v))
		case int32:
			paramTypes[i] = []byte{mysql.MYSQL_TYPE_LONG}
			paramValues[i] = mysql.Uint32ToBytes(uint32(v))
		case int:
			paramTypes[i] = []byte{mysql.MYSQL_TYPE_LONGLONG}
			paramValues[i] = mysql.Uint64ToBytes(uint64(v))
		case int64:
			paramTypes[i] = []byte{mysql.MYSQL_TYPE_LONGLONG}
			paramValues[i] = mysql.Uint64ToBytes(uint64(v))
		case uint8:
			paramTypes[i] = []byte{mysql.MYSQL_TYPE_TINY}
			paramFlags[i] = []byte{mysql.PARAM_UNSIGNED}
			paramValues[i] = []byte{v}
		case uint16:
			paramTypes[i] = []byte{mysql.MYSQL_TYPE_SHORT}
			paramFlags[i] = []byte{mysql.PARAM_UNSIGNED}
			paramValues[i] = mysql.Uint16ToBytes(v)
		case uint32:
			paramTypes[i] = []byte{mysql.MYSQL_TYPE_LONG}
			paramFlags[i] = []byte{mysql.PARAM_UNSIGNED}
			paramValues[i] = mysql.Uint32ToBytes(v)
		case uint:
			paramTypes[i] = []byte{mysql.MYSQL_TYPE_LONGLONG}
			paramFlags[i] = []byte{mysql.PARAM_UNSIGNED}
			paramValues[i] = mysql.Uint64ToBytes(uint64(v))
		case uint64:
			paramTypes[i] = []byte{mysql.MYSQL_TYPE_LONGLONG}
			paramFlags[i] = []byte{mysql.PARAM_UNSIGNED}
			paramValues[i] = mysql.Uint64ToBytes(v)
		case bool:
			paramTypes[i] = []byte{mysql.MYSQL_TYPE_TINY}
			if v {
				paramValues[i] = []byte{1}
			} else {
				paramValues[i] = []byte{0}
			}
		case float32:
			paramTypes[i] = []byte{mysql.MYSQL_TYPE_FLOAT}
			paramValues[i] = mysql.Uint32ToBytes(math.Float32bits(v))
		case float64:
			paramTypes[i] = []byte{mysql.MYSQL_TYPE_DOUBLE}
			paramValues[i] = mysql.Uint64ToBytes(math.Float64bits(v))
		case string:
			paramTypes[i] = []byte{mysql.MYSQL_TYPE_STRING}
			paramValues[i] = append(mysql.PutLengthEncodedInt(uint64(len(v))), v...)
		case []byte:
			paramTypes[i] = []byte{mysql.MYSQL_TYPE_STRING}
			paramValues[i] = append(mysql.PutLengthEncodedInt(uint64(len(v))), v...)
		case mysql.TypedBytes:
			paramTypes[i] = []byte{v.Type}
			paramValues[i] = append(mysql.PutLengthEncodedInt(uint64(len(v.Bytes))), v.Bytes...)
		case json.RawMessage:
			paramTypes[i] = []byte{mysql.MYSQL_TYPE_STRING}
			paramValues[i] = append(mysql.PutLengthEncodedInt(uint64(len(v))), v...)
		default:
			return fmt.Errorf("invalid argument type %T", args[i])
		}
		paramNames[i] = []byte{0} // length encoded, no name
		if paramFlags[i] == nil {
			paramFlags[i] = []byte{0}
		}

		length += len(paramValues[i])
	}
	for i, qa := range s.conn.queryAttributes {
		tf := qa.TypeAndFlag()
		paramTypes[(i + paramsNum)] = []byte{tf[0]}
		paramFlags[i+paramsNum] = []byte{tf[1]}
		paramValues[i+paramsNum] = qa.ValueBytes()
		paramNames[i+paramsNum] = mysql.PutLengthEncodedString([]byte(qa.Name))
	}

	data := utils.BytesBufferGet()
	defer func() {
		utils.BytesBufferPut(data)
	}()
	if data.Len() < length+4 {
		data.Grow(4 + length)
	}

	data.Write([]byte{0, 0, 0, 0})
	data.WriteByte(mysql.COM_STMT_EXECUTE)
	data.Write([]byte{byte(s.ID), byte(s.ID >> 8), byte(s.ID >> 16), byte(s.ID >> 24)})

	flags := mysql.CURSOR_TYPE_NO_CURSOR
	if paramsNum > 0 {
		flags |= mysql.PARAMETER_COUNT_AVAILABLE
	}
	data.WriteByte(flags)

	// iteration-count, always 1
	data.Write([]byte{1, 0, 0, 0})

	if paramsNum > 0 || (s.conn.capability&mysql.CLIENT_QUERY_ATTRIBUTES > 0 && (flags&mysql.PARAMETER_COUNT_AVAILABLE > 0)) {
		if s.conn.capability&mysql.CLIENT_QUERY_ATTRIBUTES > 0 {
			paramsNum += len(s.conn.queryAttributes)
			data.Write(mysql.PutLengthEncodedInt(uint64(paramsNum)))
		}
		if paramsNum > 0 {
			data.Write(nullBitmap)

			// new-params-bound-flag
			data.WriteByte(newParamBoundFlag)

			if newParamBoundFlag == 1 {
				for i := 0; i < paramsNum; i++ {
					data.Write(paramTypes[i])
					data.Write(paramFlags[i])

					if s.conn.capability&mysql.CLIENT_QUERY_ATTRIBUTES > 0 {
						data.Write(paramNames[i])
					}
				}

				// value of each parameter
				for _, v := range paramValues {
					data.Write(v)
				}
			}
		}
	}

	s.conn.ResetSequence()

	return s.conn.WritePacket(data.Bytes())
}

func (c *Conn) Prepare(query string) (*Stmt, error) {
	if err := c.writeCommandStr(mysql.COM_STMT_PREPARE, query); err != nil {
		return nil, errors.Trace(err)
	}

	data, err := c.ReadPacket()
	if err != nil {
		return nil, errors.Trace(err)
	}

	if data[0] == mysql.ERR_HEADER {
		return nil, c.handleErrorPacket(data)
	} else if data[0] != mysql.OK_HEADER {
		return nil, mysql.ErrMalformPacket
	}

	s := new(Stmt)
	s.conn = c

	pos := 1

	// for statement id
	s.ID = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	// number columns
	s.Columns = int(binary.LittleEndian.Uint16(data[pos:]))
	pos += 2

	// number params
	s.Params = int(binary.LittleEndian.Uint16(data[pos:]))
	pos += 2

	// reserved
	pos++

	if len(data) >= 12 {
		// warnings
		s.warnings = int(binary.LittleEndian.Uint16(data[pos:]))
		// pos += 2
	}

	if s.Params > 0 {
		s.RawParamFields = make([][]byte, s.Params)
		for i := range s.Params {
			data, err := s.conn.ReadPacket()
			if err != nil {
				return nil, errors.Trace(err)
			}
			s.RawParamFields[i] = data
		}
		if s.conn.capability&mysql.CLIENT_DEPRECATE_EOF == 0 {
			if packet, err := s.conn.ReadPacket(); err != nil {
				return nil, errors.Trace(err)
			} else if !c.isEOFPacket(packet) {
				return nil, mysql.ErrMalformPacket
			}
		}
	}

	if s.Columns > 0 {
		s.RawColumnFields = make([][]byte, s.Columns)
		for i := range s.Columns {
			data, err := s.conn.ReadPacket()
			if err != nil {
				return nil, errors.Trace(err)
			}
			s.RawColumnFields[i] = data
		}
		if s.conn.capability&mysql.CLIENT_DEPRECATE_EOF == 0 {
			if packet, err := s.conn.ReadPacket(); err != nil {
				return nil, errors.Trace(err)
			} else if !c.isEOFPacket(packet) {
				return nil, mysql.ErrMalformPacket
			}
		}
	}

	return s, nil
}
