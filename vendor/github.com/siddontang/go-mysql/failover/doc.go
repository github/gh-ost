// Failover supports to promote a new master and let other slaves
// replicate from it automatically.
//
// Failover does not support monitoring whether a master is alive or not,
// and will think the master is down.
//
// This package is still in development and could not be used in production environment.
package failover
