/*
   Copyright 2022 GitHub Inc.
         See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/mysql"
)

func newTestThrottler() *Throttler {
	migrationContext := base.NewMigrationContext()
	return NewThrottler(migrationContext, NewApplier(migrationContext), NewInspector(migrationContext), "test")
}

func TestThrottleReturnsWhenNotThrottled(t *testing.T) {
	thlr := newTestThrottler()
	// not throttled by default — should return immediately
	done := make(chan struct{})
	go func() {
		thlr.throttle(nil)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("throttle() did not return when not throttled")
	}
}

func TestThrottleBlocksWhileThrottled(t *testing.T) {
	thlr := newTestThrottler()
	thlr.migrationContext.SetThrottled(true, "test", base.NoThrottleReasonHint)

	done := make(chan struct{})
	go func() {
		thlr.throttle(nil)
		close(done)
	}()

	// should still be blocking after 300ms
	select {
	case <-done:
		t.Fatal("throttle() returned while still throttled")
	case <-time.After(300 * time.Millisecond):
	}

	// unthrottle — should unblock within the next 250ms sleep cycle
	thlr.migrationContext.SetThrottled(false, "", base.NoThrottleReasonHint)
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("throttle() did not return after unthrottling")
	}
}

func TestThrottleReturnsOnContextCancellation(t *testing.T) {
	thlr := newTestThrottler()
	thlr.migrationContext.SetThrottled(true, "test", base.NoThrottleReasonHint)

	done := make(chan struct{})
	go func() {
		thlr.throttle(nil)
		close(done)
	}()

	// should be blocked
	select {
	case <-done:
		t.Fatal("throttle() returned before context was cancelled")
	case <-time.After(300 * time.Millisecond):
	}

	thlr.migrationContext.CancelContext()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("throttle() did not return after context cancellation")
	}
}

func TestControlReplicaConnectionConfig(t *testing.T) {
	replicaKey := mysql.InstanceKey{Hostname: "replica-host", Port: 3307}

	t.Run("uses inspector connection config when not in move-tables mode", func(t *testing.T) {
		thlr := newTestThrottler()
		thlr.migrationContext.InspectorConnectionConfig.Key = mysql.InstanceKey{Hostname: "source-host", Port: 3306}
		thlr.migrationContext.InspectorConnectionConfig.User = "source-user"
		thlr.migrationContext.InspectorConnectionConfig.Password = "source-pass"

		connectionConfig := thlr.controlReplicaConnectionConfig(replicaKey)

		assert.False(t, thlr.migrationContext.IsMoveTablesMode())
		assert.Equal(t, replicaKey, connectionConfig.Key)
		assert.Equal(t, "source-user", connectionConfig.User)
		assert.Equal(t, "source-pass", connectionConfig.Password)
	})

	t.Run("uses move-tables connection config when in move-tables mode", func(t *testing.T) {
		thlr := newTestThrottler()
		thlr.migrationContext.MoveTables.TableNames = []string{"my_table"}
		thlr.migrationContext.MoveTables.ConnectionConfig = mysql.NewConnectionConfig()
		thlr.migrationContext.MoveTables.ConnectionConfig.Key = mysql.InstanceKey{Hostname: "target-host", Port: 3306}
		thlr.migrationContext.MoveTables.ConnectionConfig.User = "target-user"
		thlr.migrationContext.MoveTables.ConnectionConfig.Password = "target-pass"

		connectionConfig := thlr.controlReplicaConnectionConfig(replicaKey)

		assert.True(t, thlr.migrationContext.IsMoveTablesMode())
		assert.Equal(t, replicaKey, connectionConfig.Key)
		assert.Equal(t, "target-user", connectionConfig.User)
		assert.Equal(t, "target-pass", connectionConfig.Password)
	})
}

func TestThrottleCallsOnThrottledCallback(t *testing.T) {
	thlr := newTestThrottler()
	thlr.migrationContext.SetThrottled(true, "test", base.NoThrottleReasonHint)

	var callCount atomic.Int32
	done := make(chan struct{})
	go func() {
		thlr.throttle(func() { callCount.Add(1) })
		close(done)
	}()

	// wait long enough for at least two callback invocations
	time.Sleep(700 * time.Millisecond)
	assert.GreaterOrEqual(t, callCount.Load(), int32(2))

	thlr.migrationContext.CancelContext()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("throttle() did not return after context cancellation")
	}
}
