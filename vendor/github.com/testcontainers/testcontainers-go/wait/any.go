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
	_ Strategy        = (*AnyMultiStrategy)(nil)
	_ StrategyTimeout = (*AnyMultiStrategy)(nil)
)

type AnyMultiStrategy struct {
	// all Strategies should have a startupTimeout to avoid waiting infinitely
	timeout  *time.Duration
	deadline *time.Duration

	// additional properties
	Strategies []Strategy
}

// WithStartupTimeoutDefault sets the default timeout for all inner wait strategies.
func (ms *AnyMultiStrategy) WithStartupTimeoutDefault(timeout time.Duration) *AnyMultiStrategy {
	ms.timeout = &timeout
	return ms
}

// WithDeadline sets a time.Duration which limits all wait strategies.
func (ms *AnyMultiStrategy) WithDeadline(deadline time.Duration) *AnyMultiStrategy {
	ms.deadline = &deadline
	return ms
}

// ForAny returns a WaitStrategy that waits for any of the supplied conditions
// to become true (after which it cancels the remaining ones).
//
// Failures are not permitted: any strategy which fails will have its error
// immediately returned.
func ForAny(strategies ...Strategy) *AnyMultiStrategy {
	return &AnyMultiStrategy{
		Strategies: strategies,
	}
}

func (ms *AnyMultiStrategy) Timeout() *time.Duration {
	return ms.timeout
}

// String returns a human-readable description of the wait strategy.
func (ms *AnyMultiStrategy) String() string {
	if len(ms.Strategies) == 0 {
		return "any of: (none)"
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

	// Always include "any of:" prefix to make it clear this is a AnyMultiStrategy
	// even when there's only one strategy after filtering out nils.
	return "any of: [" + strings.Join(strategies, ", ") + "]"
}

func (ms *AnyMultiStrategy) WaitUntilReady(ctx context.Context, target StrategyTarget) error {
	if len(ms.Strategies) == 0 {
		return errors.New("no wait strategy supplied")
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // All remaining strategies will stop when this fires.

	if ms.deadline != nil {
		ctx, cancel = context.WithTimeout(ctx, *ms.deadline)
		defer cancel()
	}

	resCh := make(chan error, len(ms.Strategies))
	var valid int

	for _, strategy := range ms.Strategies {
		if strategy == nil || reflect.ValueOf(strategy).IsNil() {
			// A module could be appending strategies after part of the container initialization,
			// and use wait.ForAny on a not initialized strategy.
			// In this case, we just skip the nil strategy.
			continue
		}
		valid++

		strategyCtx := ctx
		// Set default Timeout when strategy implements StrategyTimeout
		if st, ok := strategy.(StrategyTimeout); ok {
			if ms.Timeout() != nil && st.Timeout() == nil {
				strategyCtx, cancel = context.WithTimeout(ctx, *ms.Timeout())
				defer cancel()
			}
		}
		go func() { resCh <- strategy.WaitUntilReady(strategyCtx, target) }()
	}

	if valid == 0 {
		return nil
	}

	for {
		select {
		case err := <-resCh:
			if err != nil {
				return err
			}
			return nil
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for strategies: %w", ctx.Err())
		}
	}
}
