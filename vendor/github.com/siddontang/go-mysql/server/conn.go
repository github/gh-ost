package server

import (
	"net"
	"sync/atomic"

	. "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/packet"
	"github.com/siddontang/go/sync2"
)

/*
   Conn acts like a MySQL server connection, you can use MySQL client to communicate with it.
*/
type Conn struct {
	*packet.Conn

	capability uint32

	connectionID uint32

	status uint16

	user string

	salt []byte

	h Handler

	stmts  map[uint32]*Stmt
	stmtID uint32

	closed sync2.AtomicBool
}

var baseConnID uint32 = 10000

func NewConn(conn net.Conn, user string, password string, h Handler) (*Conn, error) {
	c := new(Conn)

	c.h = h

	c.user = user
	c.Conn = packet.NewConn(conn)

	c.connectionID = atomic.AddUint32(&baseConnID, 1)

	c.stmts = make(map[uint32]*Stmt)

	c.salt, _ = RandomBuf(20)

	c.closed.Set(false)

	if err := c.handshake(password); err != nil {
		c.Close()
		return nil, err
	}

	return c, nil
}

func (c *Conn) handshake(password string) error {
	if err := c.writeInitialHandshake(); err != nil {
		return err
	}

	if err := c.readHandshakeResponse(password); err != nil {
		c.writeError(err)

		return err
	}

	if err := c.writeOK(nil); err != nil {
		return err
	}

	c.ResetSequence()

	return nil
}

func (c *Conn) Close() {
	c.closed.Set(true)
	c.Conn.Close()
}

func (c *Conn) Closed() bool {
	return c.closed.Get()
}

func (c *Conn) GetUser() string {
	return c.user
}

func (c *Conn) ConnectionID() uint32 {
	return c.connectionID
}

func (c *Conn) IsAutoCommit() bool {
	return c.status&SERVER_STATUS_AUTOCOMMIT > 0
}

func (c *Conn) IsInTransaction() bool {
	return c.status&SERVER_STATUS_IN_TRANS > 0
}

func (c *Conn) SetInTransaction() {
	c.status |= SERVER_STATUS_IN_TRANS
}

func (c *Conn) ClearInTransaction() {
	c.status &= ^SERVER_STATUS_IN_TRANS
}
