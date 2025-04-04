package client

import (
	"time"
)

type (
	poolOptions struct {
		logFunc LogFunc

		minAlive int
		maxAlive int
		maxIdle  int

		addr     string
		user     string
		password string
		dbName   string

		connOptions []Option

		newPoolPingTimeout time.Duration
	}
)

type (
	PoolOption func(o *poolOptions)
)

// WithPoolLimits sets pool limits:
//   - minAlive specifies the minimum number of open connections that the pool will try to maintain.
//   - maxAlive specifies the maximum number of open connections (for internal reasons,
//     may be greater by 1 inside newConnectionProducer).
//   - maxIdle specifies the maximum number of idle connections (see DefaultIdleTimeout).
func WithPoolLimits(minAlive, maxAlive, maxIdle int) PoolOption {
	return func(o *poolOptions) {
		o.minAlive = minAlive
		o.maxAlive = maxAlive
		o.maxIdle = maxIdle
	}
}

func WithLogFunc(f LogFunc) PoolOption {
	return func(o *poolOptions) {
		o.logFunc = f
	}
}

func WithConnOptions(options ...Option) PoolOption {
	return func(o *poolOptions) {
		o.connOptions = append(o.connOptions, options...)
	}
}

// WithNewPoolPingTimeout enables connect & ping to DB during the pool initialization
func WithNewPoolPingTimeout(timeout time.Duration) PoolOption {
	return func(o *poolOptions) {
		o.newPoolPingTimeout = timeout
	}
}
