package server

import (
	"fmt"

	. "github.com/siddontang/go-mysql/mysql"
)

func (c *Conn) writeOK(r *Result) error {
	if r == nil {
		r = &Result{}
	}

	r.Status |= c.status

	data := make([]byte, 4, 32)

	data = append(data, OK_HEADER)

	data = append(data, PutLengthEncodedInt(r.AffectedRows)...)
	data = append(data, PutLengthEncodedInt(r.InsertId)...)

	if c.capability&CLIENT_PROTOCOL_41 > 0 {
		data = append(data, byte(r.Status), byte(r.Status>>8))
		data = append(data, 0, 0)
	}

	return c.WritePacket(data)
}

func (c *Conn) writeError(e error) error {
	var m *MyError
	var ok bool
	if m, ok = e.(*MyError); !ok {
		m = NewError(ER_UNKNOWN_ERROR, e.Error())
	}

	data := make([]byte, 4, 16+len(m.Message))

	data = append(data, ERR_HEADER)
	data = append(data, byte(m.Code), byte(m.Code>>8))

	if c.capability&CLIENT_PROTOCOL_41 > 0 {
		data = append(data, '#')
		data = append(data, m.State...)
	}

	data = append(data, m.Message...)

	return c.WritePacket(data)
}

func (c *Conn) writeEOF() error {
	data := make([]byte, 4, 9)

	data = append(data, EOF_HEADER)
	if c.capability&CLIENT_PROTOCOL_41 > 0 {
		data = append(data, 0, 0)
		data = append(data, byte(c.status), byte(c.status>>8))
	}

	return c.WritePacket(data)
}

func (c *Conn) writeResultset(r *Resultset) error {
	columnLen := PutLengthEncodedInt(uint64(len(r.Fields)))

	data := make([]byte, 4, 1024)

	data = append(data, columnLen...)
	if err := c.WritePacket(data); err != nil {
		return err
	}

	for _, v := range r.Fields {
		data = data[0:4]
		data = append(data, v.Dump()...)
		if err := c.WritePacket(data); err != nil {
			return err
		}
	}

	if err := c.writeEOF(); err != nil {
		return err
	}

	for _, v := range r.RowDatas {
		data = data[0:4]
		data = append(data, v...)
		if err := c.WritePacket(data); err != nil {
			return err
		}
	}

	if err := c.writeEOF(); err != nil {
		return err
	}

	return nil
}

func (c *Conn) writeFieldList(fs []*Field) error {
	data := make([]byte, 4, 1024)

	for _, v := range fs {
		data = data[0:4]
		data = append(data, v.Dump()...)
		if err := c.WritePacket(data); err != nil {
			return err
		}
	}

	if err := c.writeEOF(); err != nil {
		return err
	}
	return nil
}

type noResponse struct{}

func (c *Conn) writeValue(value interface{}) error {
	switch v := value.(type) {
	case noResponse:
		return nil
	case error:
		return c.writeError(v)
	case nil:
		return c.writeOK(nil)
	case *Result:
		if v != nil && v.Resultset != nil {
			return c.writeResultset(v.Resultset)
		} else {
			return c.writeOK(v)
		}
	case []*Field:
		return c.writeFieldList(v)
	case *Stmt:
		return c.writePrepare(v)
	default:
		return fmt.Errorf("invalid response type %T", value)
	}
}
