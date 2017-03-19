package failover

import (
	"fmt"

	"github.com/siddontang/go-mysql/client"
	. "github.com/siddontang/go-mysql/mysql"
)

type User struct {
	Name     string
	Password string
}

type Server struct {
	Addr string

	User     User
	ReplUser User

	conn *client.Conn
}

func NewServer(addr string, user User, replUser User) *Server {
	s := new(Server)

	s.Addr = addr

	s.User = user
	s.ReplUser = replUser

	return s
}

func (s *Server) Close() {
	if s.conn != nil {
		s.conn.Close()
	}
}

func (s *Server) Execute(cmd string, args ...interface{}) (r *Result, err error) {
	retryNum := 3
	for i := 0; i < retryNum; i++ {
		if s.conn == nil {
			s.conn, err = client.Connect(s.Addr, s.User.Name, s.User.Password, "")
			if err != nil {
				return nil, err
			}
		}

		r, err = s.conn.Execute(cmd, args...)
		if err != nil && ErrorEqual(err, ErrBadConn) {
			return
		} else if ErrorEqual(err, ErrBadConn) {
			s.conn = nil
			continue
		} else {
			return
		}
	}
	return
}

func (s *Server) StartSlave() error {
	_, err := s.Execute("START SLAVE")
	return err
}

func (s *Server) StopSlave() error {
	_, err := s.Execute("STOP SLAVE")
	return err
}

func (s *Server) StopSlaveIOThread() error {
	_, err := s.Execute("STOP SLAVE IO_THREAD")
	return err
}

func (s *Server) SlaveStatus() (*Resultset, error) {
	r, err := s.Execute("SHOW SLAVE STATUS")
	if err != nil {
		return nil, err
	} else {
		return r.Resultset, nil
	}
}

func (s *Server) MasterStatus() (*Resultset, error) {
	r, err := s.Execute("SHOW MASTER STATUS")
	if err != nil {
		return nil, err
	} else {
		return r.Resultset, nil
	}
}

func (s *Server) ResetSlave() error {
	_, err := s.Execute("RESET SLAVE")
	return err
}

func (s *Server) ResetSlaveALL() error {
	_, err := s.Execute("RESET SLAVE ALL")
	return err
}

func (s *Server) ResetMaster() error {
	_, err := s.Execute("RESET MASTER")
	return err
}

func (s *Server) MysqlGTIDMode() (string, error) {
	r, err := s.Execute("SELECT @@gtid_mode")
	if err != nil {
		return GTIDModeOff, err
	}
	on, _ := r.GetString(0, 0)
	if on != GTIDModeOn {
		return GTIDModeOff, nil
	} else {
		return GTIDModeOn, nil
	}
}

func (s *Server) SetReadonly(b bool) error {
	var err error
	if b {
		_, err = s.Execute("SET GLOBAL read_only = ON")
	} else {
		_, err = s.Execute("SET GLOBAL read_only = OFF")
	}
	return err
}

func (s *Server) LockTables() error {
	_, err := s.Execute("FLUSH TABLES WITH READ LOCK")
	return err
}

func (s *Server) UnlockTables() error {
	_, err := s.Execute("UNLOCK TABLES")
	return err
}

// Get current binlog filename and position read from master
func (s *Server) FetchSlaveReadPos() (Position, error) {
	r, err := s.SlaveStatus()
	if err != nil {
		return Position{}, err
	}

	fname, _ := r.GetStringByName(0, "Master_Log_File")
	pos, _ := r.GetIntByName(0, "Read_Master_Log_Pos")

	return Position{fname, uint32(pos)}, nil
}

// Get current executed binlog filename and position from master
func (s *Server) FetchSlaveExecutePos() (Position, error) {
	r, err := s.SlaveStatus()
	if err != nil {
		return Position{}, err
	}

	fname, _ := r.GetStringByName(0, "Relay_Master_Log_File")
	pos, _ := r.GetIntByName(0, "Exec_Master_Log_Pos")

	return Position{fname, uint32(pos)}, nil
}

func (s *Server) MasterPosWait(pos Position, timeout int) error {
	_, err := s.Execute(fmt.Sprintf("SELECT MASTER_POS_WAIT('%s', %d, %d)", pos.Name, pos.Pos, timeout))
	return err
}
