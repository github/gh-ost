package statsd

import (
	"sync"
)

var (
	// containerID holds the container ID.
	containerID = ""

	initOnce sync.Once
)

// getContainerID returns the container ID configured at the client creation
// It can either be auto-discovered with origin detection or provided by the user.
// User-defined container ID is prioritized.
func getContainerID() string {
	return containerID
}
