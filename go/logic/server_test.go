package logic

import (
	"testing"
	"time"

	"github.com/github/gh-ost/go/base"
	"github.com/openark/golib/tests"
)

func TestServerRunCPUProfile(t *testing.T) {
	t.Parallel()

	t.Run("failed already running", func(t *testing.T) {
		s := &Server{isCPUProfiling: 1}
		profile, err := s.runCPUProfile("15ms")
		tests.S(t).ExpectEquals(err, ErrCPUProfilingInProgress)
		tests.S(t).ExpectEquals(profile, nil)
	})

	t.Run("failed bad duration", func(t *testing.T) {
		s := &Server{isCPUProfiling: 0}
		profile, err := s.runCPUProfile("should-fail")
		tests.S(t).ExpectNotNil(err)
		tests.S(t).ExpectEquals(profile, nil)
	})

	t.Run("failed bad option", func(t *testing.T) {
		s := &Server{isCPUProfiling: 0}
		profile, err := s.runCPUProfile("10ms,badoption")
		tests.S(t).ExpectEquals(err, ErrCPUProfilingBadOption)
		tests.S(t).ExpectEquals(profile, nil)
	})

	t.Run("success", func(t *testing.T) {
		s := &Server{
			isCPUProfiling:   0,
			migrationContext: base.NewMigrationContext(),
		}
		defaultCPUProfileDuration = time.Millisecond * 10
		profile, err := s.runCPUProfile("")
		tests.S(t).ExpectNil(err)
		tests.S(t).ExpectNotEquals(profile, nil)
		tests.S(t).ExpectEquals(s.isCPUProfiling, int64(0))
	})

	t.Run("success with block", func(t *testing.T) {
		s := &Server{
			isCPUProfiling:   0,
			migrationContext: base.NewMigrationContext(),
		}
		profile, err := s.runCPUProfile("10ms,block")
		tests.S(t).ExpectNil(err)
		tests.S(t).ExpectNotEquals(profile, nil)
		tests.S(t).ExpectEquals(s.isCPUProfiling, int64(0))
	})

	t.Run("success with block and gzip", func(t *testing.T) {
		s := &Server{
			isCPUProfiling:   0,
			migrationContext: base.NewMigrationContext(),
		}
		profile, err := s.runCPUProfile("10ms,block,gzip")
		tests.S(t).ExpectNil(err)
		tests.S(t).ExpectNotEquals(profile, nil)
		tests.S(t).ExpectEquals(s.isCPUProfiling, int64(0))
	})
}
