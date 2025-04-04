//go:build unix

package utils

import (
	"syscall"
	"time"
)

// Now is a faster method to get current time
func Now() time.Time {
	var tv syscall.Timeval
	if err := syscall.Gettimeofday(&tv); err != nil {
		// If it failed at syscall, use time package instead
		return time.Now()
	}

	return time.Unix(0, syscall.TimevalToNsec(tv))
}
