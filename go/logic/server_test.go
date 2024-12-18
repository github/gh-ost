package logic

import (
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
