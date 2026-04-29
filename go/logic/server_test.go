package logic

import (
	"bufio"
	"bytes"
	"os"
	"path"
	"sync/atomic"
	"testing"
	"time"

	"github.com/github/gh-ost/go/base"
	"github.com/stretchr/testify/require"
)

func TestServerRunCPUProfile(t *testing.T) {
	t.Parallel()

	t.Run("failed already running", func(t *testing.T) {
		s := &Server{isCPUProfiling: 1}
		profile, err := s.runCPUProfile("15ms")
		require.Equal(t, err, ErrCPUProfilingInProgress)
		require.Nil(t, profile)
	})

	t.Run("failed bad duration", func(t *testing.T) {
		s := &Server{isCPUProfiling: 0}
		profile, err := s.runCPUProfile("should-fail")
		require.Error(t, err)
		require.Nil(t, profile)
	})

	t.Run("failed bad option", func(t *testing.T) {
		s := &Server{isCPUProfiling: 0}
		profile, err := s.runCPUProfile("10ms,badoption")
		require.Equal(t, err, ErrCPUProfilingBadOption)
		require.Nil(t, profile)
	})

	t.Run("success", func(t *testing.T) {
		s := &Server{
			isCPUProfiling:   0,
			migrationContext: base.NewMigrationContext(),
		}
		defaultCPUProfileDuration = time.Millisecond * 10
		profile, err := s.runCPUProfile("")
		require.NoError(t, err)
		require.NotNil(t, profile)
		require.Equal(t, int64(0), s.isCPUProfiling)
	})

	t.Run("success with block", func(t *testing.T) {
		s := &Server{
			isCPUProfiling:   0,
			migrationContext: base.NewMigrationContext(),
		}
		profile, err := s.runCPUProfile("10ms,block")
		require.NoError(t, err)
		require.NotNil(t, profile)
		require.Equal(t, int64(0), s.isCPUProfiling)
	})

	t.Run("success with block and gzip", func(t *testing.T) {
		s := &Server{
			isCPUProfiling:   0,
			migrationContext: base.NewMigrationContext(),
		}
		profile, err := s.runCPUProfile("10ms,block,gzip")
		require.NoError(t, err)
		require.NotNil(t, profile)
		require.Equal(t, int64(0), s.isCPUProfiling)
	})
}

func TestServerCreatePostponeCutOverFlagFile(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		s := &Server{
			migrationContext: base.NewMigrationContext(),
		}
		dir, err := os.MkdirTemp("", "gh-ost-test-")
		require.NoError(t, err)
		defer os.RemoveAll(dir)

		filePath := path.Join(dir, "postpone-cut-over.flag")

		err = s.createPostponeCutOverFlagFile(filePath)
		require.NoError(t, err)
		require.FileExists(t, filePath)
	})

	t.Run("file already exists", func(t *testing.T) {
		s := &Server{
			migrationContext: base.NewMigrationContext(),
		}
		dir, err := os.MkdirTemp("", "gh-ost-test-")
		require.NoError(t, err)

		filePath := path.Join(dir, "postpone-cut-over.flag")
		err = base.TouchFile(filePath)
		require.NoError(t, err)

		err = s.createPostponeCutOverFlagFile(filePath)
		require.NoError(t, err)
		require.FileExists(t, filePath)
	})
}

func newTestServer() *Server {
	ctx := base.NewMigrationContext()
	return &Server{
		migrationContext: ctx,
		hooksExecutor:    NewHooksExecutor(ctx),
	}
}

func applyCommand(t *testing.T, s *Server, command string) (PrintStatusRule, string) {
	t.Helper()
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	rule, err := s.applyServerCommand(command, writer)
	require.NoError(t, err)
	writer.Flush()
	return rule, buf.String()
}

func TestServerCopyConcurrencyCommand(t *testing.T) {
	t.Parallel()

	t.Run("query default", func(t *testing.T) {
		s := newTestServer()
		// NewMigrationContext defaults to 0; CLI flag sets it to 1
		_, output := applyCommand(t, s, "copy-concurrency=?")
		require.Equal(t, "0\n", output)
	})

	t.Run("set valid value", func(t *testing.T) {
		s := newTestServer()
		rule, _ := applyCommand(t, s, "copy-concurrency=8")
		require.EqualValues(t, ForcePrintStatusAndHintRule, rule)
		require.Equal(t, int64(8), atomic.LoadInt64(&s.migrationContext.CopyConcurrency))
	})

	t.Run("set to 1 (single-threaded)", func(t *testing.T) {
		s := newTestServer()
		atomic.StoreInt64(&s.migrationContext.CopyConcurrency, 4)
		rule, _ := applyCommand(t, s, "copy-concurrency=1")
		require.EqualValues(t, ForcePrintStatusAndHintRule, rule)
		require.Equal(t, int64(1), atomic.LoadInt64(&s.migrationContext.CopyConcurrency))
	})

	t.Run("set max value", func(t *testing.T) {
		s := newTestServer()
		rule, _ := applyCommand(t, s, "copy-concurrency=32")
		require.EqualValues(t, ForcePrintStatusAndHintRule, rule)
		require.Equal(t, int64(32), atomic.LoadInt64(&s.migrationContext.CopyConcurrency))
	})

	t.Run("reject zero", func(t *testing.T) {
		s := newTestServer()
		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)
		_, err := s.applyServerCommand("copy-concurrency=0", writer)
		require.Error(t, err)
		require.Contains(t, err.Error(), "between 1 and 32")
	})

	t.Run("reject too high", func(t *testing.T) {
		s := newTestServer()
		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)
		_, err := s.applyServerCommand("copy-concurrency=33", writer)
		require.Error(t, err)
		require.Contains(t, err.Error(), "between 1 and 32")
	})

	t.Run("reject non-numeric", func(t *testing.T) {
		s := newTestServer()
		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)
		_, err := s.applyServerCommand("copy-concurrency=abc", writer)
		require.Error(t, err)
	})
}

func TestServerCopyMaxLagMillisCommand(t *testing.T) {
	t.Parallel()

	t.Run("query default", func(t *testing.T) {
		s := newTestServer()
		_, output := applyCommand(t, s, "copy-max-lag-millis=?")
		require.Equal(t, "0\n", output) // NewMigrationContext defaults to 0
	})

	t.Run("set valid value", func(t *testing.T) {
		s := newTestServer()
		rule, _ := applyCommand(t, s, "copy-max-lag-millis=60000")
		require.EqualValues(t, ForcePrintStatusAndHintRule, rule)
		require.Equal(t, int64(60000), atomic.LoadInt64(&s.migrationContext.CopyMaxLagMillis))
	})

	t.Run("set to zero (disabled)", func(t *testing.T) {
		s := newTestServer()
		atomic.StoreInt64(&s.migrationContext.CopyMaxLagMillis, 60000)
		rule, _ := applyCommand(t, s, "copy-max-lag-millis=0")
		require.EqualValues(t, ForcePrintStatusAndHintRule, rule)
		require.Equal(t, int64(0), atomic.LoadInt64(&s.migrationContext.CopyMaxLagMillis))
	})

	t.Run("reject negative", func(t *testing.T) {
		s := newTestServer()
		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)
		_, err := s.applyServerCommand("copy-max-lag-millis=-1", writer)
		require.Error(t, err)
		require.Contains(t, err.Error(), ">= 0")
	})

	t.Run("reject non-numeric", func(t *testing.T) {
		s := newTestServer()
		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)
		_, err := s.applyServerCommand("copy-max-lag-millis=abc", writer)
		require.Error(t, err)
	})
}
