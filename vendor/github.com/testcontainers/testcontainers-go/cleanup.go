package testcontainers

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"
)

// terminateOptions is a type that holds the options for terminating a container.
type terminateOptions struct {
	ctx     context.Context
	timeout *time.Duration
	volumes []string
}

// TerminateOption is a type that represents an option for terminating a container.
type TerminateOption func(*terminateOptions)

// StopContext returns a TerminateOption that sets the context.
// Default: context.Background().
func StopContext(ctx context.Context) TerminateOption {
	return func(c *terminateOptions) {
		c.ctx = ctx
	}
}

// StopTimeout returns a TerminateOption that sets the timeout.
// Default: See [Container.Stop].
func StopTimeout(timeout time.Duration) TerminateOption {
	return func(c *terminateOptions) {
		c.timeout = &timeout
	}
}

// RemoveVolumes returns a TerminateOption that sets additional volumes to remove.
// This is useful when the container creates named volumes that should be removed
// which are not removed by default.
// Default: nil.
func RemoveVolumes(volumes ...string) TerminateOption {
	return func(c *terminateOptions) {
		c.volumes = volumes
	}
}

// TerminateContainer calls [Container.Terminate] on the container if it is not nil.
//
// This should be called as a defer directly after [GenericContainer](...)
// or a modules Run(...) to ensure the container is terminated when the
// function ends.
func TerminateContainer(container Container, options ...TerminateOption) error {
	if isNil(container) {
		return nil
	}

	c := &terminateOptions{
		ctx: context.Background(),
	}

	for _, opt := range options {
		opt(c)
	}

	// TODO: Add a timeout when terminate supports it.
	err := container.Terminate(c.ctx)
	if !isCleanupSafe(err) {
		return fmt.Errorf("terminate: %w", err)
	}

	// Remove additional volumes if any.
	if len(c.volumes) == 0 {
		return nil
	}

	client, err := NewDockerClientWithOpts(c.ctx)
	if err != nil {
		return fmt.Errorf("docker client: %w", err)
	}

	defer client.Close()

	// Best effort to remove all volumes.
	var errs []error
	for _, volume := range c.volumes {
		if errRemove := client.VolumeRemove(c.ctx, volume, true); errRemove != nil {
			errs = append(errs, fmt.Errorf("volume remove %q: %w", volume, errRemove))
		}
	}

	return errors.Join(errs...)
}

// isNil returns true if val is nil or an nil instance false otherwise.
func isNil(val any) bool {
	if val == nil {
		return true
	}

	valueOf := reflect.ValueOf(val)
	switch valueOf.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Ptr, reflect.UnsafePointer, reflect.Interface, reflect.Slice:
		return valueOf.IsNil()
	default:
		return false
	}
}
