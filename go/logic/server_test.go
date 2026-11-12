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

func TestServerUnpostponeCommands(t *testing.T) {
	t.Parallel()

	newServer := func() *Server {
		migrationContext := base.NewMigrationContext()
		migrationContext.OriginalTableName = "users"
		return NewServer(migrationContext, CompositeHooks{}, nil)
	}
	run := func(t *testing.T, s *Server, command string) (string, PrintStatusRule, error) {
		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)
		rule, err := s.applyServerCommand(command, writer)
		require.NoError(t, writer.Flush())
		return buf.String(), rule, err
	}

	t.Run("unpostpone is rejected while not postponing", func(t *testing.T) {
		s := newServer()
		out, rule, err := run(t, s, "unpostpone")
		require.NoError(t, err)
		require.Equal(t, NoPrintStatusRule, rule)
		require.Contains(t, out, "only invoke this when gh-ost is actively postponing")
		require.Equal(t, int64(0), atomic.LoadInt64(&s.migrationContext.UserCommandedUnpostponeFlag))
	})

	t.Run("cancel-unpostpone withdraws a pending unpostpone", func(t *testing.T) {
		s := newServer()
		atomic.StoreInt64(&s.migrationContext.IsPostponingCutOver, 1)

		out, _, err := run(t, s, "unpostpone")
		require.NoError(t, err)
		require.Equal(t, "Unpostponed\n", out)
		require.Equal(t, int64(1), atomic.LoadInt64(&s.migrationContext.UserCommandedUnpostponeFlag))

		out, rule, err := run(t, s, "cancel-unpostpone")
		require.NoError(t, err)
		require.Equal(t, PrintStatusRule(ForcePrintStatusAndHintRule), rule)
		require.Contains(t, out, "Unpostpone cancelled")
		require.Equal(t, int64(0), atomic.LoadInt64(&s.migrationContext.UserCommandedUnpostponeFlag))
		require.Equal(t, int64(1), atomic.LoadInt64(&s.migrationContext.IsPostponingCutOver))

		// A second cancel has nothing to withdraw.
		out, rule, err = run(t, s, "cancel-unpostpone")
		require.NoError(t, err)
		require.Equal(t, NoPrintStatusRule, rule)
		require.Contains(t, out, "no pending unpostpone")
	})

	t.Run("cancel-unpostpone with table name", func(t *testing.T) {
		s := newServer()
		atomic.StoreInt64(&s.migrationContext.UserCommandedUnpostponeFlag, 1)

		_, _, err := run(t, s, "cancel-unpostpone=orders")
		require.Error(t, err)
		require.Equal(t, int64(1), atomic.LoadInt64(&s.migrationContext.UserCommandedUnpostponeFlag))

		out, _, err := run(t, s, "cancel-unpostpone=users")
		require.NoError(t, err)
		require.Contains(t, out, "Unpostpone cancelled")
		require.Equal(t, int64(0), atomic.LoadInt64(&s.migrationContext.UserCommandedUnpostponeFlag))
	})

	t.Run("help lists cancel-unpostpone", func(t *testing.T) {
		out, _, err := run(t, newServer(), "help")
		require.NoError(t, err)
		require.Contains(t, out, "cancel-unpostpone")
	})
}
