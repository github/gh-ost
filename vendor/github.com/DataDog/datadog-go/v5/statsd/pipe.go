//go:build !windows
// +build !windows

package statsd

import (
	"errors"
	"time"
)

func newWindowsPipeWriter(pipepath string, writeTimeout time.Duration) (Transport, error) {
	return nil, errors.New("Windows Named Pipes are only supported on Windows")
}
