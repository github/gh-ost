//go:build windows
// +build windows

package statsd

import (
	"fmt"
	"time"
)

// newUDSWriter is disabled on Windows, SOCK_DGRAM  are still unavailable but
// SOCK_STREAM should work once implemented in the agent (https://devblogs.microsoft.com/commandline/af_unix-comes-to-windows/)
func newUDSWriter(_ string, _ time.Duration, _ time.Duration, _ string) (Transport, error) {
	return nil, fmt.Errorf("Unix socket is not available on Windows")
}
