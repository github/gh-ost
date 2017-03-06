package failover

import (
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/mysql"
)

// Failover will do below things after the master down
//  1. Elect a slave which has the most up-to-date data with old master
//  2. Promote the slave to new master
//  3. Change other slaves to the new master
//
// Limitation:
//  1, All slaves must have the same master before, Failover will check using master server id or uuid
//  2, If the failover error, the whole topology may be wrong, we must handle this error manually
//  3, Slaves must have same replication mode, all use GTID or not
//
func Failover(flavor string, slaves []*Server) ([]*Server, error) {
	var h Handler
	var err error

	switch flavor {
	case mysql.MySQLFlavor:
		h = new(MysqlGTIDHandler)
	case mysql.MariaDBFlavor:
		return nil, errors.Errorf("MariaDB failover is not supported now")
	default:
		return nil, errors.Errorf("invalid flavor %s", flavor)
	}

	// First check slaves use gtid or not
	if err := h.CheckGTIDMode(slaves); err != nil {
		return nil, errors.Trace(err)
	}

	// Stop all slave IO_THREAD and wait the relay log done
	for _, slave := range slaves {
		if err = h.WaitRelayLogDone(slave); err != nil {
			return nil, errors.Trace(err)
		}
	}

	var bestSlave *Server
	// Find best slave which has the most up-to-data data
	if bestSlaves, err := h.FindBestSlaves(slaves); err != nil {
		return nil, errors.Trace(err)
	} else {
		bestSlave = bestSlaves[0]
	}

	// Promote the best slave to master
	if err = h.Promote(bestSlave); err != nil {
		return nil, errors.Trace(err)
	}

	// Change master
	for i := 0; i < len(slaves); i++ {
		if bestSlave == slaves[i] {
			continue
		}

		if err = h.ChangeMasterTo(slaves[i], bestSlave); err != nil {
			return nil, errors.Trace(err)
		}
	}

	return slaves, nil
}
