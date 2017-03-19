package failover

import (
	"fmt"
	"net"

	"github.com/juju/errors"
	. "github.com/siddontang/go-mysql/mysql"
)

// Limiatation
// + Multi source replication is not supported
// + Slave can not handle write transactions, so maybe readonly or strict_mode = 1 is better
type MariadbGTIDHandler struct {
	Handler
}

func (h *MariadbGTIDHandler) Promote(s *Server) error {
	if err := h.WaitRelayLogDone(s); err != nil {
		return errors.Trace(err)
	}

	if err := s.StopSlave(); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (h *MariadbGTIDHandler) FindBestSlaves(slaves []*Server) ([]*Server, error) {
	bestSlaves := []*Server{}

	ps := make([]uint64, len(slaves))

	lastIndex := -1
	var seq uint64

	for i, slave := range slaves {
		rr, err := slave.Execute("SELECT @@gtid_current_pos")

		if err != nil {
			return nil, errors.Trace(err)
		}

		str, _ := rr.GetString(0, 0)
		if len(str) == 0 {
			seq = 0
		} else {
			g, err := ParseMariadbGTIDSet(str)
			if err != nil {
				return nil, errors.Trace(err)
			}

			seq = g.(MariadbGTID).SequenceNumber
		}

		ps[i] = seq

		if lastIndex == -1 {
			lastIndex = i
			bestSlaves = []*Server{slave}
		} else {
			if ps[lastIndex] < seq {
				lastIndex = i
				bestSlaves = []*Server{slave}
			} else if ps[lastIndex] == seq {
				// these two slaves have same data,
				bestSlaves = append(bestSlaves, slave)
			}
		}
	}

	return bestSlaves, nil
}

const changeMasterToWithCurrentPos = `CHANGE MASTER TO 
    MASTER_HOST = "%s", MASTER_PORT = %s, 
    MASTER_USER = "%s", MASTER_PASSWORD = "%s", 
    MASTER_USE_GTID = current_pos`

func (h *MariadbGTIDHandler) ChangeMasterTo(s *Server, m *Server) error {
	if err := h.WaitRelayLogDone(s); err != nil {
		return errors.Trace(err)
	}

	if err := s.StopSlave(); err != nil {
		return errors.Trace(err)
	}

	if err := s.ResetSlave(); err != nil {
		return errors.Trace(err)
	}

	host, port, _ := net.SplitHostPort(m.Addr)

	if _, err := s.Execute(fmt.Sprintf(changeMasterToWithCurrentPos,
		host, port, m.ReplUser.Name, m.ReplUser.Password)); err != nil {
		return errors.Trace(err)
	}

	if err := s.StartSlave(); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (h *MariadbGTIDHandler) WaitRelayLogDone(s *Server) error {
	if err := s.StopSlaveIOThread(); err != nil {
		return errors.Trace(err)
	}

	r, err := s.SlaveStatus()
	if err != nil {
		return errors.Trace(err)
	}

	fname, _ := r.GetStringByName(0, "Master_Log_File")
	pos, _ := r.GetIntByName(0, "Read_Master_Log_Pos")

	return s.MasterPosWait(Position{fname, uint32(pos)}, 0)
}

func (h *MariadbGTIDHandler) WaitCatchMaster(s *Server, m *Server) error {
	r, err := m.Execute("SELECT @@gtid_binlog_pos")
	if err != nil {
		return errors.Trace(err)
	}

	pos, _ := r.GetString(0, 0)

	return h.waitUntilAfterGTID(s, pos)
}

func (h *MariadbGTIDHandler) CheckGTIDMode(slaves []*Server) error {
	return nil
}

func (h *MariadbGTIDHandler) waitUntilAfterGTID(s *Server, pos string) error {
	_, err := s.Execute(fmt.Sprintf("SELECT MASTER_GTID_WAIT('%s')", pos))
	return err
}
