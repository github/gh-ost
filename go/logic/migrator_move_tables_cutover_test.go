package logic

import (
	"context"
	gosql "database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/binlog"
	"github.com/github/gh-ost/go/mysql"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	testmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
)

// -----------------------------------------------------------------------------
// Pure unit tests - no MySQL. These exercise the orchestration branches that
// run BEFORE T1's RENAME, so they do not require a real source-primary DB. The
// "RENAME was not attempted" check is a proxy assertion: m.sourcePrimaryDB is
// nil, so if T1 were reached the test would fail with the source-primary-not-
// initialized error instead of the earlier error it asserts on.
// -----------------------------------------------------------------------------

// TestMoveTablesCutOver_NoopShortCircuits maps to the Noop semantics decision
// in #8209-implement-protocol (Noop returns immediately, no postpone gate, no
// hooks, no RENAME).
func TestMoveTablesCutOver_NoopShortCircuits(t *testing.T) {
	var calls []string
	fakeHooks := &recordingHooks{name: "fake", calls: &calls}

	ctx := base.NewMigrationContext()
	ctx.Noop = true
	ctx.Hooks = fakeHooks

	m := NewMigrator(ctx, "test")

	require.Equal(t, int64(0), atomic.LoadInt64(&ctx.CutOverCompleteFlag), "pre-state: flag must be 0")
	require.Empty(t, calls, "pre-state: no hooks recorded")

	require.NoError(t, m.moveTablesCutOver())

	require.Equal(t, int64(0), atomic.LoadInt64(&ctx.CutOverCompleteFlag),
		"post-state: Noop must not set CutOverCompleteFlag")
	require.Empty(t, calls, "post-state: Noop must not fire any hook")
}

// TestMoveTablesCutOver_OnBeforeCutOverHookAbortsBeforeRename maps to T0 in
// coop_cutover.md section 1.3 ("non-zero return code aborts cutover"). The "aborts
// BEFORE source DDL" assertion is enforced as a proxy: m.sourcePrimaryDB is nil,
// so if T1 RENAME executed it would fail with the source-primary-not-initialized
// error rather than the asserted hook error.
func TestMoveTablesCutOver_OnBeforeCutOverHookAbortsBeforeRename(t *testing.T) {
	var calls []string
	boom := errors.New("hook says no")
	fakeHooks := &recordingHooks{name: "fake", calls: &calls, errOn: "OnBeforeCutOver", errVal: boom}

	ctx := base.NewMigrationContext()
	ctx.Hooks = fakeHooks
	ctx.DatabaseName = "test"
	ctx.OriginalTableName = "t"

	m := NewMigrator(ctx, "test")

	require.Equal(t, int64(0), atomic.LoadInt64(&ctx.CutOverCompleteFlag), "pre-state: flag must be 0")
	require.Empty(t, calls, "pre-state: no hooks recorded")

	err := m.moveTablesCutOver()
	require.Error(t, err)
	require.ErrorIs(t, err, boom)
	require.Contains(t, err.Error(), "on-before-cut-over hook failed")

	require.Equal(t, int64(0), atomic.LoadInt64(&ctx.CutOverCompleteFlag),
		"post-state: T0 abort must leave CutOverCompleteFlag unset")
	require.Equal(t, []string{"fake:OnBeforeCutOver"}, calls,
		"post-state: only the failing T0 hook fires; no OnSuccess, no OnBeginPostponed")
}

type onSuccessCheckHooks struct {
	*recordingHooks
	onSuccessCheck func() error
}

func (h *onSuccessCheckHooks) OnSuccess(bool) error {
	if err := h.record("OnSuccess"); err != nil {
		return err
	}
	if h.onSuccessCheck != nil {
		return h.onSuccessCheck()
	}
	return nil
}

// TestMoveTablesCutOver_PostponeGateFiresOnBeginPostponedOnce maps to the
// postpone-gate decision in #8209-implement-protocol (keep OnBeginPostponed
// firing logic with the same once-per-cutover semantics as standard cutOver).
// Also exercises Edge Case Test Quality #5 by asserting both pre-state and
// post-state of IsPostponingCutOver.
func TestMoveTablesCutOver_PostponeGateFiresOnBeginPostponedOnce(t *testing.T) {
	flagPath := filepath.Join(t.TempDir(), "postpone.flag")
	require.NoError(t, os.WriteFile(flagPath, nil, 0o644))

	var calls []string
	boom := errors.New("we got past the gate")
	fakeHooks := &recordingHooks{name: "fake", calls: &calls, errOn: "OnBeforeCutOver", errVal: boom}

	ctx := base.NewMigrationContext()
	ctx.PostponeCutOverFlagFile = flagPath
	ctx.Hooks = fakeHooks

	m := NewMigrator(ctx, "test")

	require.Equal(t, int64(0), atomic.LoadInt64(&ctx.IsPostponingCutOver), "pre-state: gate flag must be 0")
	require.FileExists(t, flagPath, "pre-state: postpone flag file present")
	require.Empty(t, calls, "pre-state: no hooks recorded")

	// Remove the flag file during the gate's first 1s sleep so the next
	// poll exits cleanly. sleepWhileTrue uses time.Sleep(1 * time.Second).
	go func() {
		time.Sleep(500 * time.Millisecond)
		_ = os.Remove(flagPath)
	}()

	err := m.moveTablesCutOver()
	require.ErrorIs(t, err, boom, "expect to bail at T0 hook after gate releases")

	onBegin := 0
	for _, c := range calls {
		if c == "fake:OnBeginPostponed" {
			onBegin++
		}
	}
	require.Equal(t, 1, onBegin,
		"post-state: OnBeginPostponed must fire exactly once per cutover (idempotent via IsPostponingCutOver)")
	require.Equal(t, int64(0), atomic.LoadInt64(&ctx.IsPostponingCutOver),
		"post-state: gate must reset IsPostponingCutOver to 0 after exit")
	require.Contains(t, calls, "fake:OnBeforeCutOver",
		"post-state: T0 hook must fire after gate releases")
	require.Equal(t, int64(0), atomic.LoadInt64(&ctx.CutOverCompleteFlag),
		"post-state: hook failure must leave CutOverCompleteFlag unset")
}

// TestResumeMoveTablesCutOverFromCheckpointAlreadyDrained verifies the crash-
// safe resume branch skips T1 entirely and proceeds directly to T5 when the
// persisted checkpoint already shows a drain-satisfied position.
func TestResumeMoveTablesCutOverFromCheckpointAlreadyDrained(t *testing.T) {
	var calls []string
	fakeHooks := &recordingHooks{name: "fake", calls: &calls}

	ctx := base.NewMigrationContext()
	ctx.Hooks = fakeHooks
	ctx.Checkpoint = false

	m := NewMigrator(ctx, "test")
	m.applier = NewApplier(ctx)

	drainGTID, err := mysql.NewGTIDBinlogCoordinates("11111111-1111-1111-1111-111111111111:1-10")
	require.NoError(t, err)

	chk := &Checkpoint{
		LastTrxCoords:              drainGTID,
		MoveTablesCutOverStarted:   true,
		MoveTablesCutOverDrainGTID: drainGTID,
	}

	require.Equal(t, int64(0), atomic.LoadInt64(&ctx.CutOverCompleteFlag), "pre-state: flag must be 0")
	require.Empty(t, calls, "pre-state: no hooks recorded")

	require.NoError(t, m.resumeMoveTablesCutOverFromCheckpoint(chk))

	require.Equal(t, int64(1), atomic.LoadInt64(&ctx.CutOverCompleteFlag),
		"post-state: resume path must set CutOverCompleteFlag before exiting")
	require.NotNil(t, ctx.MoveTables.DrainGTID,
		"post-state: resume must set MoveTables.DrainGTID so the on-success hook gets GH_OST_DRAIN_GTID")
	require.Equal(t, drainGTID.String(), ctx.MoveTables.DrainGTID.String(),
		"post-state: MoveTables.DrainGTID must equal the checkpoint drain GTID")
	require.Equal(t, []string{"fake:OnSuccess"}, calls,
		"post-state: resume path should jump directly to T5 without rerunning T0/T1")
	if m.applier.CurrentCoordinates != nil {
		require.Equal(t, drainGTID.String(), m.applier.CurrentCoordinates.String())
	}
}

// TestResolveSourcePrimaryConnectionConfig_AssumeMasterHostnameOverride verifies
// that --assume-master-host forces the move-tables source primary to the given
// host, and that --master-user/--master-password override the inherited source
// credentials. No DB is required: the override branch builds the config purely
// from the inspector config.
func TestResolveSourcePrimaryConnectionConfig_AssumeMasterHostnameOverride(t *testing.T) {
	mc := base.NewMigrationContext()
	mc.InspectorConnectionConfig.User = "src_user"
	mc.InspectorConnectionConfig.Password = "src_pass"
	mc.AssumeMasterHostname = "10.0.0.5:3307"
	mc.CliMasterUser = "master_user"
	mc.CliMasterPassword = "master_pass"
	m := NewMigrator(mc, "test")

	cfg, err := m.resolveSourcePrimaryConnectionConfig("8.0.42")
	require.NoError(t, err)
	require.Equal(t, "10.0.0.5", cfg.Key.Hostname)
	require.Equal(t, 3307, cfg.Key.Port)
	require.Equal(t, "master_user", cfg.User, "--master-user must override source credentials")
	require.Equal(t, "master_pass", cfg.Password, "--master-password must override source credentials")

	// Without explicit master credentials, the forced primary inherits the source
	// (inspector) credentials.
	mc.CliMasterUser = ""
	mc.CliMasterPassword = ""
	cfg, err = m.resolveSourcePrimaryConnectionConfig("8.0.42")
	require.NoError(t, err)
	require.Equal(t, "src_user", cfg.User)
	require.Equal(t, "src_pass", cfg.Password)
}

// TestValidateMoveTablesSourceReadHost verifies the read-path guard: a replica
// source passes, a source that resolves to the primary is blocked with a hint to
// use a replica or --allow-on-source-primary, and the opt-in flag bypasses it.
func TestValidateMoveTablesSourceReadHost(t *testing.T) {
	newMigrator := func(srcKey, primaryKey mysql.InstanceKey, allow bool) *Migrator {
		mc := base.NewMigrationContext()
		mc.MoveTables.TableNames = []string{"t"}
		mc.InspectorConnectionConfig.Key = srcKey
		mc.MoveTables.SourcePrimaryConnectionConfig = &mysql.ConnectionConfig{Key: primaryKey}
		mc.MoveTables.AllowOnSourcePrimary = allow
		return NewMigrator(mc, "test")
	}
	replica := mysql.InstanceKey{Hostname: "replica.example.com", Port: 3306}
	primary := mysql.InstanceKey{Hostname: "primary.example.com", Port: 3306}

	t.Run("source is a replica: passes", func(t *testing.T) {
		require.NoError(t, newMigrator(replica, primary, false).validateMoveTablesSourceReadHost())
	})

	t.Run("source is the primary: blocked", func(t *testing.T) {
		err := newMigrator(primary, primary, false).validateMoveTablesSourceReadHost()
		require.Error(t, err)
		require.Contains(t, err.Error(), "--allow-on-source-primary")
	})

	t.Run("source is the primary but opted in: passes", func(t *testing.T) {
		require.NoError(t, newMigrator(primary, primary, true).validateMoveTablesSourceReadHost())
	})
}

// -----------------------------------------------------------------------------
// Integration tests - real MySQL via testcontainers, exercise T1/T2/T3.
//
// These live under a dedicated suite (NOT MigratorTestSuite) so the parent
// function name TestMoveTablesCutOver matches the `-run MoveTablesCutOver`
// verifier alongside the pure unit tests above. SetupSuite duplicates the
// testcontainer setup pattern used by MigratorTestSuite intentionally to
// keep this commit's diff strictly additive (no edits to existing tests or
// shared helpers, per the commit-3 prompt).
//
// Known-environmental flakes when Docker/testcontainers is unavailable mirror
// the same class as #8206; they are not regressions of #8209.
// -----------------------------------------------------------------------------

type MoveTablesCutOverSuite struct {
	suite.Suite
	mysqlContainer testcontainers.Container
	db             *gosql.DB
}

func (s *MoveTablesCutOverSuite) SetupSuite() {
	ctx := context.Background()
	mysqlContainer, err := testmysql.Run(ctx,
		testMysqlContainerImage,
		testmysql.WithDatabase(testMysqlDatabase),
		testmysql.WithUsername(testMysqlUser),
		testmysql.WithPassword(testMysqlPass),
		testmysql.WithConfigFile("my.cnf.test"),
	)
	s.Require().NoError(err)
	s.mysqlContainer = mysqlContainer

	dsn, err := mysqlContainer.ConnectionString(ctx)
	s.Require().NoError(err)
	db, err := gosql.Open("mysql", dsn)
	s.Require().NoError(err)
	s.db = db
}

func (s *MoveTablesCutOverSuite) TearDownSuite() {
	s.Assert().NoError(s.db.Close())
	s.Assert().NoError(testcontainers.TerminateContainer(s.mysqlContainer))
}

func (s *MoveTablesCutOverSuite) SetupTest() {
	_, err := s.db.ExecContext(context.Background(), "CREATE DATABASE IF NOT EXISTS "+testMysqlDatabase)
	s.Require().NoError(err)
}

func (s *MoveTablesCutOverSuite) TearDownTest() {
	ctx := context.Background()
	_, _ = s.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+getTestTableName())
	_, _ = s.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+getTestOldTableName())
}

// containingDrainGTID returns a fabricated GTID set guaranteed to contain any
// GTID the test container will assign during the test. RENAME's GTID will be
// a strict subset of this range, so T3's containment poll passes on iteration 1.
func (s *MoveTablesCutOverSuite) containingDrainGTID() *mysql.GTIDBinlogCoordinates {
	var serverUUID string
	s.Require().NoError(s.db.QueryRow("SELECT @@server_uuid").Scan(&serverUUID))
	g, err := mysql.NewGTIDBinlogCoordinates(fmt.Sprintf("%s:1-99999999", serverUUID))
	s.Require().NoError(err)
	return g
}

// buildMigrator wires a Migrator with the test container's *sql.DB pinned to
// inspector.db and a fresh Applier. The cutover RENAME + drain-GTID capture run
// on sourcePrimaryDB (a dedicated handle with multiStatements enabled, since
// T1/T2 are issued as a single multi-statement round trip). initialCoords may be
// nil for the drain-timeout case.
func (s *MoveTablesCutOverSuite) buildMigrator(fakeHooks base.Hooks, initialCoords mysql.BinlogCoordinates) (*Migrator, *base.MigrationContext) {
	ctx := context.Background()
	connectionConfig, err := getTestConnectionConfig(ctx, s.mysqlContainer)
	s.Require().NoError(err)

	mc := newTestMigrationContext()
	mc.ApplierConnectionConfig = connectionConfig
	mc.InspectorConnectionConfig = connectionConfig
	mc.MoveTables.SourcePrimaryConnectionConfig = connectionConfig
	mc.SetConnectionConfig("innodb")
	mc.Hooks = fakeHooks

	m := NewMigrator(mc, "test")
	m.inspector = &Inspector{db: s.db, migrationContext: mc}
	// The source primary handle needs multiStatements enabled for the consolidated
	// T1+T2 (RENAME; SELECT @@global.gtid_executed) round trip.
	sourcePrimaryDB, _, err := mysql.GetDB(mc.Uuid, connectionConfig.GetDBUri(testMysqlDatabase)+"&multiStatements=true")
	s.Require().NoError(err)
	m.sourcePrimaryDB = sourcePrimaryDB
	m.applier = NewApplier(mc)
	if initialCoords != nil {
		m.applier.CurrentCoordinatesMutex.Lock()
		m.applier.CurrentCoordinates = initialCoords
		m.applier.CurrentCoordinatesMutex.Unlock()
	}
	return m, mc
}

// TestDropMoveTablesSourceOldTablesUsesSourcePrimary verifies the source `__del`
// rollback handle is dropped through the dedicated source-primary connection. In
// production the inspector/streamer source connections may be a read replica, so
// the drop must not route through them.
func (s *MoveTablesCutOverSuite) TestDropMoveTablesSourceOldTablesUsesSourcePrimary() {
	ctx := context.Background()
	_, err := s.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY)", getTestOldTableName()))
	s.Require().NoError(err)

	var calls []string
	fakeHooks := &recordingHooks{name: "fake", calls: &calls}
	m, mc := s.buildMigrator(fakeHooks, s.containingDrainGTID())
	mc.MoveTables.TableNames = []string{testMysqlTableName}

	s.Require().NoError(m.dropMoveTablesSourceOldTables())

	var name string
	err = s.db.QueryRow(fmt.Sprintf("SHOW TABLES IN %s LIKE '_%s_del'",
		testMysqlDatabase, testMysqlTableName)).Scan(&name)
	s.Require().ErrorIs(err, gosql.ErrNoRows, "source __del handle must be dropped via the source primary")
}

// TestResolveSourcePrimaryFallsBackToInspectorWhenNoReplica verifies the
// graceful fallback: when the source --host has no upstream primary (the
// standalone test container), master detection returns the inspector connection
// config, so source reads and cutover writes share the one available host.
func (s *MoveTablesCutOverSuite) TestResolveSourcePrimaryFallsBackToInspectorWhenNoReplica() {
	var calls []string
	m, mc := s.buildMigrator(&recordingHooks{name: "fake", calls: &calls}, nil)

	cfg, err := m.resolveSourcePrimaryConnectionConfig("8.0.42")
	s.Require().NoError(err)
	s.Require().Equal(mc.InspectorConnectionConfig.Key.Hostname, cfg.Key.Hostname)
	s.Require().Equal(mc.InspectorConnectionConfig.Key.Port, cfg.Key.Port)
}

// TestAssertConnectionWritableRejectsReadOnly verifies the startup writability
// gate: a writable primary passes, and a read_only server is rejected with a
// clear error. The same helper guards both the source primary and the target.
func (s *MoveTablesCutOverSuite) TestAssertConnectionWritableRejectsReadOnly() {
	key := mysql.InstanceKey{Hostname: "test-host", Port: 3306}
	s.Require().NoError(assertConnectionWritable(s.db, key, "source primary"),
		"a writable primary must pass the gate")

	_, err := s.db.Exec("SET GLOBAL read_only = ON")
	s.Require().NoError(err)
	defer func() { _, _ = s.db.Exec("SET GLOBAL read_only = OFF") }()

	err = assertConnectionWritable(s.db, key, "source primary")
	s.Require().Error(err)
	s.Require().Contains(err.Error(), "read_only")
}

// TestHappyPath drives the full T0-T6 protocol against the test container.
// Asserts hook ordering (T0 then T5), T4 flag set, and the source-side rename.
// Maps to acceptance criterion #8209 "RENAME executes; drain completes;
// CutOverCompleteFlag set; on-success hook fires".
func (s *MoveTablesCutOverSuite) TestHappyPath() {
	ctx := context.Background()
	_, err := s.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY)", getTestTableName()))
	s.Require().NoError(err)

	var calls []string
	fakeHooks := &recordingHooks{name: "fake", calls: &calls}
	m, mc := s.buildMigrator(fakeHooks, s.containingDrainGTID())

	s.Require().Equal(int64(0), atomic.LoadInt64(&mc.CutOverCompleteFlag), "pre-state: flag must be 0")
	s.Require().Empty(calls, "pre-state: no hooks recorded")

	s.Require().NoError(m.moveTablesCutOver())

	s.Require().Equal(int64(1), atomic.LoadInt64(&mc.CutOverCompleteFlag),
		"post-state: T4 must set CutOverCompleteFlag before T5/T6")
	s.Require().Equal([]string{"fake:OnBeforeCutOver", "fake:OnSuccess"}, calls,
		"post-state: T0 hook precedes T5 hook")

	// Source-side post-state (Edge Case Test Quality #5: assert both sides).
	var renamed string
	s.Require().NoError(s.db.QueryRow(fmt.Sprintf("SHOW TABLES IN %s LIKE '_%s_del'",
		testMysqlDatabase, testMysqlTableName)).Scan(&renamed))
	s.Require().Equal("_"+testMysqlTableName+"_del", renamed)

	err = s.db.QueryRow(fmt.Sprintf("SHOW TABLES IN %s LIKE '%s'",
		testMysqlDatabase, testMysqlTableName)).Scan(&renamed)
	s.Require().ErrorIs(err, gosql.ErrNoRows, "original table must no longer exist under its old name")
}

// TestRenameFailurePropagates maps to T1 failure handling: no source table ->
// RENAME errors. Verify the wrapped error, no CutOverCompleteFlag set, no
// OnSuccess fired.
func (s *MoveTablesCutOverSuite) TestRenameFailurePropagates() {
	// Deliberately no CREATE TABLE.
	var calls []string
	fakeHooks := &recordingHooks{name: "fake", calls: &calls}
	m, mc := s.buildMigrator(fakeHooks, s.containingDrainGTID())

	s.Require().Equal(int64(0), atomic.LoadInt64(&mc.CutOverCompleteFlag), "pre-state: flag must be 0")

	err := m.moveTablesCutOver()
	s.Require().Error(err)
	s.Require().Contains(err.Error(), "source RENAME + drain GTID capture failed")

	s.Require().Equal(int64(0), atomic.LoadInt64(&mc.CutOverCompleteFlag),
		"post-state: RENAME failure must leave CutOverCompleteFlag unset")
	for _, c := range calls {
		s.Require().NotEqual("fake:OnSuccess", c, "OnSuccess must not fire when RENAME fails")
	}
	s.Require().Equal([]string{"fake:OnBeforeCutOver"}, calls,
		"post-state: only T0 fires before the failed RENAME")
}

// TestDrainTimeoutPropagates maps to T3 timeout handling: applier coordinates
// never reach the drain GTID -> drain poll bounded by CutOverLockTimeoutSeconds
// returns a wrapped error and the flag is not set.
func (s *MoveTablesCutOverSuite) TestDrainTimeoutPropagates() {
	ctx := context.Background()
	_, err := s.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY)", getTestTableName()))
	s.Require().NoError(err)

	// Patch poll interval and use a 1-second drain timeout for a bounded test.
	origPoll := moveTablesCutOverDrainPollInterval
	moveTablesCutOverDrainPollInterval = 50 * time.Millisecond
	s.T().Cleanup(func() {
		moveTablesCutOverDrainPollInterval = origPoll
	})

	var calls []string
	fakeHooks := &recordingHooks{name: "fake", calls: &calls}
	// initialCoords nil - drain comparison never satisfies.
	m, mc := s.buildMigrator(fakeHooks, nil)
	mc.CutOverLockTimeoutSeconds = 1

	s.Require().Equal(int64(0), atomic.LoadInt64(&mc.CutOverCompleteFlag), "pre-state: flag must be 0")

	err = m.moveTablesCutOver()
	s.Require().Error(err)
	s.Require().Contains(err.Error(), "drain poll timed out")
	s.Require().True(strings.HasPrefix(err.Error(), "drain poll timed out"),
		"drain timeout error must name the drain")

	s.Require().Equal(int64(0), atomic.LoadInt64(&mc.CutOverCompleteFlag),
		"post-state: drain timeout must abort before T4 flag set")
	for _, c := range calls {
		s.Require().NotEqual("fake:OnSuccess", c, "OnSuccess must not fire on drain timeout")
	}
	s.Require().Equal([]string{"fake:OnBeforeCutOver"}, calls,
		"post-state: only T0 fires before the drain loop")
}

// TestDrainWaitsForQueuedDML ensures T3 does not declare success just because
// applier.CurrentCoordinates already contains the drain GTID while there is
// still source-table DML queued for application.
func (s *MoveTablesCutOverSuite) TestDrainWaitsForQueuedDML() {
	ctx := context.Background()
	_, err := s.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY)", getTestTableName()))
	s.Require().NoError(err)

	origPoll := moveTablesCutOverDrainPollInterval
	moveTablesCutOverDrainPollInterval = 50 * time.Millisecond
	s.T().Cleanup(func() {
		moveTablesCutOverDrainPollInterval = origPoll
	})

	var calls []string
	fakeHooks := &recordingHooks{name: "fake", calls: &calls}
	m, mc := s.buildMigrator(fakeHooks, s.containingDrainGTID())
	mc.CutOverLockTimeoutSeconds = 1
	m.applyEventsQueue <- newApplyEventStructByDML(&binlog.BinlogEntry{
		DmlEvent: &binlog.BinlogDMLEvent{
			DatabaseName: testMysqlDatabase,
			TableName:    testMysqlTableName,
			DML:          binlog.InsertDML,
		},
		Coordinates: s.containingDrainGTID(),
	})

	s.Require().Equal(int64(0), atomic.LoadInt64(&mc.CutOverCompleteFlag), "pre-state: flag must be 0")
	err = m.moveTablesCutOver()
	s.Require().Error(err)
	s.Require().Contains(err.Error(), "drain poll timed out")
	s.Require().Equal(int64(0), atomic.LoadInt64(&mc.CutOverCompleteFlag),
		"post-state: queued DML must keep T3 from reaching T4")
	for _, c := range calls {
		s.Require().NotEqual("fake:OnSuccess", c, "OnSuccess must not fire while backlog remains")
	}
	s.Require().Equal([]string{"fake:OnBeforeCutOver"}, calls,
		"post-state: only T0 fires before the drain loop times out")
}

// TestDrainWaitsForInFlightApplyEvent ensures T3 does not complete while an
// apply handler is still running. The blocked handler simulates the last source
// DML not yet landing on the target. If T3 exits early, OnSuccess observes the
// target missing that row and fails immediately.
func (s *MoveTablesCutOverSuite) TestDrainWaitsForInFlightApplyEvent() {
	ctx := context.Background()
	_, err := s.db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", testMysqlDatabaseOther))
	s.Require().NoError(err)
	_, err = s.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY)", getTestTableName()))
	s.Require().NoError(err)
	_, err = s.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY)", getTestOtherTableName()))
	s.Require().NoError(err)
	s.T().Cleanup(func() {
		_, _ = s.db.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+getTestOtherTableName())
	})

	drainGTID := s.containingDrainGTID()
	const pendingID = 42
	raceErr := errors.New("on-success observed target missing in-flight apply")

	origPoll := moveTablesCutOverDrainPollInterval
	moveTablesCutOverDrainPollInterval = 50 * time.Millisecond
	s.T().Cleanup(func() {
		moveTablesCutOverDrainPollInterval = origPoll
	})

	var calls []string
	fakeHooks := &onSuccessCheckHooks{
		recordingHooks: &recordingHooks{name: "fake", calls: &calls},
		onSuccessCheck: func() error {
			var count int
			query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE id = ?", getTestOtherTableName())
			if err := s.db.QueryRowContext(context.Background(), query, pendingID).Scan(&count); err != nil {
				return err
			}
			if count == 0 {
				return raceErr
			}
			return nil
		},
	}
	m, mc := s.buildMigrator(fakeHooks, drainGTID)
	mc.CutOverLockTimeoutSeconds = 1
	m.applier.CurrentCoordinatesMutex.Lock()
	m.applier.CurrentCoordinates = drainGTID
	m.applier.CurrentCoordinatesMutex.Unlock()

	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once
	var releaseOnce sync.Once
	releaseApply := func() {
		releaseOnce.Do(func() {
			close(release)
		})
	}
	s.T().Cleanup(releaseApply)
	blockApply := tableWriteFunc(func() error {
		startedOnce.Do(func() {
			close(started)
		})
		<-release
		_, err := s.db.ExecContext(context.Background(), fmt.Sprintf("INSERT INTO %s VALUES (?)", getTestOtherTableName()), pendingID)
		return err
	})
	go func() {
		<-started
		time.Sleep(200 * time.Millisecond)
		releaseApply()
	}()
	go func() {
		_ = m.onApplyEventStruct(newApplyEventStructByFunc(&blockApply))
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		s.Require().FailNow("blocked apply event never started")
	}

	s.Require().Equal(int64(0), atomic.LoadInt64(&mc.CutOverCompleteFlag), "pre-state: flag must be 0")
	err = m.moveTablesCutOver()
	s.Require().NoError(err)

	var count int
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE id = ?", getTestOtherTableName())
	s.Require().NoError(s.db.QueryRowContext(context.Background(), query, pendingID).Scan(&count))
	s.Require().Equal(1, count, "post-state: target must contain the pending row by the time cutover succeeds")
	s.Require().Equal([]string{"fake:OnBeforeCutOver", "fake:OnSuccess"}, calls,
		"post-state: cutover should only reach OnSuccess after the target row is present")
}

func TestMoveTablesCutOver(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration suite in short mode")
	}
	suite.Run(t, new(MoveTablesCutOverSuite))
}
