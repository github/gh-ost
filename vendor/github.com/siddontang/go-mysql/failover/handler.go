package failover

type Handler interface {
	// Promote slave s to master
	Promote(s *Server) error

	// Change slave s to master m and replicate from it
	ChangeMasterTo(s *Server, m *Server) error

	// Ensure all relay log done, it will stop slave IO_THREAD
	// You must start slave again if you want to do replication continuatively
	WaitRelayLogDone(s *Server) error

	// Wait until slave s catch all data from master m at current time
	WaitCatchMaster(s *Server, m *Server) error

	// Find best slave which has the most up-to-date data from master
	FindBestSlaves(slaves []*Server) ([]*Server, error)

	// Check all slaves have gtid enabled
	CheckGTIDMode(slaves []*Server) error
}
