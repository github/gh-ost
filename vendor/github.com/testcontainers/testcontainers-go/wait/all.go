package wait

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// Implement interface
var (
	_ Strategy        = (*MultiStrategy)(nil)
	_ StrategyTimeout = (*MultiStrategy)(nil)
)

type MultiStrategy struct {
	// all Strategies should have a startupTimeout to avoid waiting infinitely
	timeout  *time.Duration
	deadline *time.Duration

	// additional properties
	Strategies []Strategy
}

// WithStartupTimeoutDefault sets the default timeout for all inner wait strategies
func (ms *MultiStrategy) WithStartupTimeoutDefault(timeout time.Duration) *MultiStrategy {
	ms.timeout = &timeout
	return ms
}

// WithStartupTimeout sets a time.Duration which limits all wait strategies
//
// Deprecated: use WithDeadline
func (ms *MultiStrategy) WithStartupTimeout(timeout time.Duration) Strategy {
	return ms.WithDeadline(timeout)
}

// WithDeadline sets a time.Duration which limits all wait strategies
func (ms *MultiStrategy) WithDeadline(deadline time.Duration) *MultiStrategy {
	ms.deadline = &deadline
	return ms
}

func ForAll(strategies ...Strategy) *MultiStrategy {
	return &MultiStrategy{
		Strategies: strategies,
	}
}

func (ms *MultiStrategy) Timeout() *time.Duration {
	return ms.timeout
}

// String returns a human-readable description of the wait strategy.
func (ms *MultiStrategy) String() string {
	if len(ms.Strategies) == 0 {
		return "all of: (none)"
	}

	var strategies []string
	for _, strategy := range ms.Strategies {
		if strategy == nil || reflect.ValueOf(strategy).IsNil() {
			continue
		}
		if s, ok := strategy.(fmt.Stringer); ok {
			strategies = append(strategies, s.String())
		} else {
			strategies = append(strategies, fmt.Sprintf("%T", strategy))
		}
	}

	// Always include "all of:" prefix to make it clear this is a MultiStrategy
	// even when there's only one strategy after filtering out nils
	return "all of: [" + strings.Join(strategies, ", ") + "]"
}

func (ms *MultiStrategy) WaitUntilReady(ctx context.Context, target StrategyTarget) error {
	var cancel context.CancelFunc
	if ms.deadline != nil {
		ctx, cancel = context.WithTimeout(ctx, *ms.deadline)
		defer cancel()
	}

	if len(ms.Strategies) == 0 {
		return errors.New("no wait strategy supplied")
	}

	for _, strategy := range ms.Strategies {
		if strategy == nil || reflect.ValueOf(strategy).IsNil() {
			// A module could be appending strategies after part of the container initialization,
			// and use wait.ForAll on a not initialized strategy.
			// In this case, we just skip the nil strategy.
			continue
		}

		strategyCtx := ctx

		// Set default Timeout when strategy implements StrategyTimeout
		if st, ok := strategy.(StrategyTimeout); ok {
			if ms.Timeout() != nil && st.Timeout() == nil {
				strategyCtx, cancel = context.WithTimeout(ctx, *ms.Timeout())
				defer cancel()
			}
		}

		err := strategy.WaitUntilReady(strategyCtx, target)
		if err != nil {
			return err
		}
	}

	return nil
}
