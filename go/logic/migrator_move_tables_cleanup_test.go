package logic

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/github/gh-ost/go/base"
)

// capturingLogger records Infof messages so tests can assert on the operator
// commands emitted by the move-tables cleanup path. It embeds base.Logger so it
// only needs to override the one method the tests care about.
type capturingLogger struct {
	base.Logger
	infofs []string
}

func (l *capturingLogger) Infof(format string, args ...interface{}) {
	l.infofs = append(l.infofs, fmt.Sprintf(format, args...))
}

func (l *capturingLogger) has(substr string) bool {
	for _, line := range l.infofs {
		if strings.Contains(line, substr) {
			return true
		}
	}
	return false
}

func newCleanupTestMigrator() (*Migrator, *capturingLogger) {
	logger := &capturingLogger{Logger: base.NewDefaultLogger()}
	mc := base.NewMigrationContext()
	mc.Log = logger
	mc.DatabaseName = "source_db"
	mc.OriginalTableName = "t"
	mc.MoveTables.TableNames = []string{"t"}
	mc.MoveTables.TargetDatabase = "target_db"
	return NewMigrator(mc, "test"), logger
}

// TestMoveTablesFinalCleanup_EmitsOperatorCommands verifies that without
// --ok-to-drop-table, cleanup drops nothing and instead logs the exact
// `drop table` commands for the source rollback handle and the target
// checkpoint table. The applier/streamer are nil, so any real drop would panic.
func TestMoveTablesFinalCleanup_EmitsOperatorCommands(t *testing.T) {
	m, logger := newCleanupTestMigrator()
	m.migrationContext.OkToDropTable = false
	m.migrationContext.Checkpoint = true

	require.NoError(t, m.moveTablesFinalCleanup())

	require.True(t, logger.has("-- drop table `source_db`.`_t_del`"),
		"must emit the command to drop the source rollback handle")
	require.True(t, logger.has(fmt.Sprintf("-- drop table `target_db`.`%s`", m.migrationContext.GetCheckpointTableName())),
		"must emit the command to drop the target checkpoint table")
}

// TestLogMoveTablesRollbackHint_EmitsRenameCommand verifies the failure-path
// hint emits the exact rename command to roll the source table back.
func TestLogMoveTablesRollbackHint_EmitsRenameCommand(t *testing.T) {
	m, logger := newCleanupTestMigrator()

	m.logMoveTablesRollbackHint()

	require.True(t, logger.has("-- rename table `source_db`.`_t_del` to `source_db`.`t`"),
		"must emit the rename command to roll the source table back")
}

// TestDropMoveTablesSourceOldTables_NilSourcePrimaryErrors verifies the source
// `__del` drop fails cleanly (rather than panicking) when the source-primary
// connection was never initialized. The drop must never silently no-op.
func TestDropMoveTablesSourceOldTables_NilSourcePrimaryErrors(t *testing.T) {
	m, _ := newCleanupTestMigrator()

	err := m.dropMoveTablesSourceOldTables()

	require.Error(t, err)
	require.Contains(t, err.Error(), "source primary connection not initialized")
}
