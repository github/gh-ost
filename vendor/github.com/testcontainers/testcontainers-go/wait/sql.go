package wait

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/docker/go-connections/nat"
)

var (
	_ Strategy        = (*waitForSql)(nil)
	_ StrategyTimeout = (*waitForSql)(nil)
)

const defaultForSqlQuery = "SELECT 1"

// ForSQL constructs a new waitForSql strategy for the given driver
func ForSQL(port nat.Port, driver string, url func(host string, port nat.Port) string) *waitForSql {
	return &waitForSql{
		Port:           port,
		URL:            url,
		Driver:         driver,
		startupTimeout: defaultStartupTimeout(),
		PollInterval:   defaultPollInterval(),
		query:          defaultForSqlQuery,
	}
}

type waitForSql struct {
	timeout *time.Duration

	URL            func(host string, port nat.Port) string
	Driver         string
	Port           nat.Port
	startupTimeout time.Duration
	PollInterval   time.Duration
	query          string
}

// WithStartupTimeout can be used to change the default startup timeout
func (w *waitForSql) WithStartupTimeout(timeout time.Duration) *waitForSql {
	w.timeout = &timeout
	return w
}

// WithPollInterval can be used to override the default polling interval of 100 milliseconds
func (w *waitForSql) WithPollInterval(pollInterval time.Duration) *waitForSql {
	w.PollInterval = pollInterval
	return w
}

// WithQuery can be used to override the default query used in the strategy.
func (w *waitForSql) WithQuery(query string) *waitForSql {
	w.query = query
	return w
}

func (w *waitForSql) Timeout() *time.Duration {
	return w.timeout
}

// WaitUntilReady repeatedly tries to run "SELECT 1" or user defined query on the given port using sql and driver.
//
// If it doesn't succeed until the timeout value which defaults to 60 seconds, it will return an error.
func (w *waitForSql) WaitUntilReady(ctx context.Context, target StrategyTarget) error {
	timeout := defaultStartupTimeout()
	if w.timeout != nil {
		timeout = *w.timeout
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	host, err := target.Host(ctx)
	if err != nil {
		return err
	}

	ticker := time.NewTicker(w.PollInterval)
	defer ticker.Stop()

	var port nat.Port
	port, err = target.MappedPort(ctx, w.Port)

	for port == "" {
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %w", ctx.Err(), err)
		case <-ticker.C:
			if err := checkTarget(ctx, target); err != nil {
				return err
			}
			port, err = target.MappedPort(ctx, w.Port)
		}
	}

	db, err := sql.Open(w.Driver, w.URL(host, port))
	if err != nil {
		return fmt.Errorf("sql.Open: %w", err)
	}
	defer db.Close()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := checkTarget(ctx, target); err != nil {
				return err
			}
			if _, err := db.ExecContext(ctx, w.query); err != nil {
				continue
			}
			return nil
		}
	}
}
