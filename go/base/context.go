/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package base

import (
	"context"
	"fmt"
	"math"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	uuid "github.com/google/uuid"

	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"
	"github.com/openark/golib/log"

	"github.com/go-ini/ini"
)

// RowsEstimateMethod is the type of row number estimation
type RowsEstimateMethod string

const (
	TableStatusRowsEstimate RowsEstimateMethod = "TableStatusRowsEstimate"
	ExplainRowsEstimate     RowsEstimateMethod = "ExplainRowsEstimate"
	CountRowsEstimate       RowsEstimateMethod = "CountRowsEstimate"
)

type CutOver int

const (
	CutOverAtomic CutOver = iota
	CutOverTwoStep
)

type ThrottleReasonHint string

const (
	NoThrottleReasonHint                 ThrottleReasonHint = "NoThrottleReasonHint"
	UserCommandThrottleReasonHint        ThrottleReasonHint = "UserCommandThrottleReasonHint"
	LeavingHibernationThrottleReasonHint ThrottleReasonHint = "LeavingHibernationThrottleReasonHint"
)

const (
	HTTPStatusOK       = 200
	MaxEventsBatchSize = 1000
	ETAUnknown         = math.MinInt64
)

var (
	envVariableRegexp = regexp.MustCompile("[$][{](.*)[}]")
)

type ThrottleCheckResult struct {
	ShouldThrottle bool
	Reason         string
	ReasonHint     ThrottleReasonHint
}

func NewThrottleCheckResult(throttle bool, reason string, reasonHint ThrottleReasonHint) *ThrottleCheckResult {
	return &ThrottleCheckResult{
		ShouldThrottle: throttle,
		Reason:         reason,
		ReasonHint:     reasonHint,
	}
}

// MigrationContext has the general, global state of migration. It is used by
// all components throughout the migration process.
type MigrationContext struct {
	Uuid string

	DatabaseName          string
	OriginalTableName     string
	AlterStatement        string
	AlterStatementOptions string // anything following the 'ALTER TABLE [schema.]table' from AlterStatement

	countMutex               sync.Mutex
	countTableRowsCancelFunc func()
	CountTableRows           bool
	ConcurrentCountTableRows bool
	AllowedRunningOnMaster   bool
	AllowedMasterMaster      bool
	SwitchToRowBinlogFormat  bool
	AssumeRBR                bool
	SkipForeignKeyChecks     bool
	SkipStrictMode           bool
	AllowZeroInDate          bool
	NullableUniqueKeyAllowed bool
	ApproveRenamedColumns    bool
	SkipRenamedColumns       bool
	IsTungsten               bool
	DiscardForeignKeys       bool
	AliyunRDS                bool
	GoogleCloudPlatform      bool
	AzureMySQL               bool
	AttemptInstantDDL        bool
	Resume                   bool
	Revert                   bool
	OldTableName             string

	// SkipPortValidation allows skipping the port validation in `ValidateConnection`
	// This is useful when connecting to a MySQL instance where the external port
	// may not match the internal port.
	SkipPortValidation bool
	UseGTIDs           bool

	config            ContextConfig
	configMutex       *sync.Mutex
	ConfigFile        string
	CliUser           string
	CliPassword       string
	UseTLS            bool
	TLSAllowInsecure  bool
	TLSCACertificate  string
	TLSCertificate    string
	TLSKey            string
	CliMasterUser     string
	CliMasterPassword string

	HeartbeatIntervalMilliseconds       int64
	defaultNumRetries                   int64
	ChunkSize                           int64
	niceRatio                           float64
	MaxLagMillisecondsThrottleThreshold int64
	throttleControlReplicaKeys          *mysql.InstanceKeyMap
	ThrottleFlagFile                    string
	ThrottleAdditionalFlagFile          string
	throttleQuery                       string
	throttleHTTP                        string
	IgnoreHTTPErrors                    bool
	ThrottleCommandedByUser             int64
	HibernateUntil                      int64
	maxLoad                             LoadMap
	criticalLoad                        LoadMap
	CriticalLoadIntervalMilliseconds    int64
	CriticalLoadHibernateSeconds        int64
	PostponeCutOverFlagFile             string
	CutOverLockTimeoutSeconds           int64
	CutOverExponentialBackoff           bool
	ExponentialBackoffMaxInterval       int64
	ForceNamedCutOverCommand            bool
	ForceNamedPanicCommand              bool
	PanicFlagFile                       string
	HooksPath                           string
	HooksHintMessage                    string
	HooksHintOwner                      string
	HooksHintToken                      string
	HooksStatusIntervalSec              int64
	PanicOnWarnings                     bool
	Checkpoint                          bool
	CheckpointIntervalSeconds           int64

	DropServeSocket bool
	ServeSocketFile string
	ServeTCPPort    int64

	Noop                         bool
	TestOnReplica                bool
	MigrateOnReplica             bool
	TestOnReplicaSkipReplicaStop bool
	OkToDropTable                bool
	InitiallyDropOldTable        bool
	InitiallyDropGhostTable      bool
	TimestampOldTable            bool // Should old table name include a timestamp
	CutOverType                  CutOver
	ReplicaServerId              uint

	Hostname                               string
	AssumeMasterHostname                   string
	ApplierTimeZone                        string
	ApplierWaitTimeout                     int64
	TableEngine                            string
	RowsEstimate                           int64
	RowsDeltaEstimate                      int64
	UsedRowsEstimateMethod                 RowsEstimateMethod
	HasSuperPrivilege                      bool
	OriginalBinlogFormat                   string
	OriginalBinlogRowImage                 string
	InspectorConnectionConfig              *mysql.ConnectionConfig
	InspectorMySQLVersion                  string
	ApplierConnectionConfig                *mysql.ConnectionConfig
	ApplierMySQLVersion                    string
	StartTime                              time.Time
	RowCopyStartTime                       time.Time
	RowCopyEndTime                         time.Time
	LockTablesStartTime                    time.Time
	RenameTablesStartTime                  time.Time
	RenameTablesEndTime                    time.Time
	pointOfInterestTime                    time.Time
	pointOfInterestTimeMutex               *sync.Mutex
	lastHeartbeatOnChangelogTime           time.Time
	lastHeartbeatOnChangelogMutex          *sync.Mutex
	CurrentLag                             int64
	currentProgress                        uint64
	etaNanoseonds                          int64
	EtaRowsPerSecond                       int64
	ThrottleHTTPIntervalMillis             int64
	ThrottleHTTPStatusCode                 int64
	ThrottleHTTPTimeoutMillis              int64
	controlReplicasLagResult               mysql.ReplicationLagResult
	TotalRowsCopied                        int64
	TotalDMLEventsApplied                  int64
	DMLBatchSize                           int64
	isThrottled                            bool
	throttleReason                         string
	throttleReasonHint                     ThrottleReasonHint
	throttleGeneralCheckResult             ThrottleCheckResult
	throttleMutex                          *sync.Mutex
	throttleHTTPMutex                      *sync.Mutex
	IsPostponingCutOver                    int64
	CountingRowsFlag                       int64
	AllEventsUpToLockProcessedInjectedFlag int64
	CleanupImminentFlag                    int64
	UserCommandedUnpostponeFlag            int64
	CutOverCompleteFlag                    int64
	InCutOverCriticalSectionFlag           int64
	PanicAbort                             chan error

	// Context for cancellation signaling across all goroutines
	// Stored in struct as it spans the entire migration lifecycle, not per-function.
	// context.Context is safe for concurrent use by multiple goroutines.
	ctx        context.Context //nolint:containedctx
	cancelFunc context.CancelFunc

	// Stores the fatal error that triggered abort
	AbortError error
	abortMutex *sync.Mutex

	OriginalTableColumnsOnApplier    *sql.ColumnList
	OriginalTableColumns             *sql.ColumnList
	OriginalTableVirtualColumns      *sql.ColumnList
	OriginalTableUniqueKeys          [](*sql.UniqueKey)
	OriginalTableAutoIncrement       uint64
	GhostTableColumns                *sql.ColumnList
	GhostTableVirtualColumns         *sql.ColumnList
	GhostTableUniqueKeys             [](*sql.UniqueKey)
	UniqueKey                        *sql.UniqueKey
	SharedColumns                    *sql.ColumnList
	ColumnRenameMap                  map[string]string
	DroppedColumnsMap                map[string]bool
	MappedSharedColumns              *sql.ColumnList
	MigrationLastInsertSQLWarnings   []string
	MigrationRangeMinValues          *sql.ColumnValues
	MigrationRangeMaxValues          *sql.ColumnValues
	Iteration                        int64
	MigrationIterationRangeMinValues *sql.ColumnValues
	MigrationIterationRangeMaxValues *sql.ColumnValues
	InitialStreamerCoords            mysql.BinlogCoordinates
	ForceTmpTableName                string

	IncludeTriggers     bool
	RemoveTriggerSuffix bool
	TriggerSuffix       string
	Triggers            []mysql.Trigger

	recentBinlogCoordinates mysql.BinlogCoordinates

	BinlogSyncerMaxReconnectAttempts  int
	AllowSetupMetadataLockInstruments bool
	SkipMetadataLockCheck             bool
	IsOpenMetadataLockInstruments     bool

	Log Logger
}

type Logger interface {
	Debug(args ...interface{})
	Debugf(format string, args ...interface{})
	Info(args ...interface{})
	Infof(format string, args ...interface{})
	Warning(args ...interface{}) error
	Warningf(format string, args ...interface{}) error
	Error(args ...interface{}) error
	Errorf(format string, args ...interface{}) error
	Errore(err error) error
	Fatal(args ...interface{}) error
	Fatalf(format string, args ...interface{}) error
	Fatale(err error) error
	SetLevel(level log.LogLevel)
	SetPrintStackTrace(printStackTraceFlag bool)
}

type ContextConfig struct {
	Client struct {
		User     string
		Password string
	}
	Osc struct {
		Chunk_Size            int64
		Max_Lag_Millis        int64
		Replication_Lag_Query string
		Max_Load              string
	}
}

func NewMigrationContext() *MigrationContext {
	ctx, cancelFunc := context.WithCancel(context.Background())
	return &MigrationContext{
		Uuid:                                uuid.NewString(),
		defaultNumRetries:                   60,
		ChunkSize:                           1000,
		InspectorConnectionConfig:           mysql.NewConnectionConfig(),
		ApplierConnectionConfig:             mysql.NewConnectionConfig(),
		MaxLagMillisecondsThrottleThreshold: 1500,
		CutOverLockTimeoutSeconds:           3,
		DMLBatchSize:                        10,
		etaNanoseonds:                       ETAUnknown,
		maxLoad:                             NewLoadMap(),
		criticalLoad:                        NewLoadMap(),
		throttleMutex:                       &sync.Mutex{},
		throttleHTTPMutex:                   &sync.Mutex{},
		throttleControlReplicaKeys:          mysql.NewInstanceKeyMap(),
		configMutex:                         &sync.Mutex{},
		pointOfInterestTimeMutex:            &sync.Mutex{},
		lastHeartbeatOnChangelogMutex:       &sync.Mutex{},
		ColumnRenameMap:                     make(map[string]string),
		PanicAbort:                          make(chan error),
		ctx:                                 ctx,
		cancelFunc:                          cancelFunc,
		abortMutex:                          &sync.Mutex{},
		Log:                                 NewDefaultLogger(),
	}
}

func (mctx *MigrationContext) SetConnectionConfig(storageEngine string) error {
	var transactionIsolation string
	switch storageEngine {
	case "rocksdb":
		transactionIsolation = "READ-COMMITTED"
	default:
		transactionIsolation = "REPEATABLE-READ"
	}
	mctx.InspectorConnectionConfig.TransactionIsolation = transactionIsolation
	mctx.ApplierConnectionConfig.TransactionIsolation = transactionIsolation
	return nil
}

func (mctx *MigrationContext) SetConnectionCharset(charset string) {
	if charset == "" {
		charset = "utf8mb4,utf8,latin1"
	}

	mctx.InspectorConnectionConfig.Charset = charset
	mctx.ApplierConnectionConfig.Charset = charset
}

func getSafeTableName(baseName string, suffix string) string {
	name := fmt.Sprintf("_%s_%s", baseName, suffix)
	if len(name) <= mysql.MaxTableNameLength {
		return name
	}
	extraCharacters := len(name) - mysql.MaxTableNameLength
	return fmt.Sprintf("_%s_%s", baseName[0:len(baseName)-extraCharacters], suffix)
}

// GetGhostTableName generates the name of ghost table, based on original table name
// or a given table name
func (mctx *MigrationContext) GetGhostTableName() string {
	if mctx.Revert {
		// When reverting the "ghost" table is the _del table from the original migration.
		return mctx.OldTableName
	}
	if mctx.ForceTmpTableName != "" {
		return getSafeTableName(mctx.ForceTmpTableName, "gho")
	} else {
		return getSafeTableName(mctx.OriginalTableName, "gho")
	}
}

// GetOldTableName generates the name of the "old" table, into which the original table is renamed.
func (mctx *MigrationContext) GetOldTableName() string {
	var tableName string
	if mctx.ForceTmpTableName != "" {
		tableName = mctx.ForceTmpTableName
	} else {
		tableName = mctx.OriginalTableName
	}

	suffix := "del"
	if mctx.Revert {
		suffix = "rev_del"
	}
	if mctx.TimestampOldTable {
		t := mctx.StartTime
		timestamp := fmt.Sprintf("%d%02d%02d%02d%02d%02d",
			t.Year(), t.Month(), t.Day(),
			t.Hour(), t.Minute(), t.Second())
		return getSafeTableName(tableName, fmt.Sprintf("%s_%s", timestamp, suffix))
	}
	return getSafeTableName(tableName, suffix)
}

// GetChangelogTableName generates the name of changelog table, based on original table name
// or a given table name.
func (mctx *MigrationContext) GetChangelogTableName() string {
	if mctx.ForceTmpTableName != "" {
		return getSafeTableName(mctx.ForceTmpTableName, "ghc")
	} else {
		return getSafeTableName(mctx.OriginalTableName, "ghc")
	}
}

// GetCheckpointTableName generates the name of checkpoint table.
func (mctx *MigrationContext) GetCheckpointTableName() string {
	if mctx.ForceTmpTableName != "" {
		return getSafeTableName(mctx.ForceTmpTableName, "ghk")
	} else {
		return getSafeTableName(mctx.OriginalTableName, "ghk")
	}
}

// GetVoluntaryLockName returns a name of a voluntary lock to be used throughout
// the swap-tables process.
func (mctx *MigrationContext) GetVoluntaryLockName() string {
	return fmt.Sprintf("%s.%s.lock", mctx.DatabaseName, mctx.OriginalTableName)
}

// RequiresBinlogFormatChange is `true` when the original binlog format isn't `ROW`
func (mctx *MigrationContext) RequiresBinlogFormatChange() bool {
	return mctx.OriginalBinlogFormat != "ROW"
}

// GetApplierHostname is a safe access method to the applier hostname
func (mctx *MigrationContext) GetApplierHostname() string {
	if mctx.ApplierConnectionConfig == nil {
		return ""
	}
	if mctx.ApplierConnectionConfig.ImpliedKey == nil {
		return ""
	}
	return mctx.ApplierConnectionConfig.ImpliedKey.Hostname
}

// GetInspectorHostname is a safe access method to the inspector hostname
func (mctx *MigrationContext) GetInspectorHostname() string {
	if mctx.InspectorConnectionConfig == nil {
		return ""
	}
	if mctx.InspectorConnectionConfig.ImpliedKey == nil {
		return ""
	}
	return mctx.InspectorConnectionConfig.ImpliedKey.Hostname
}

// InspectorIsAlsoApplier is `true` when the both inspector and applier are the
// same database instance. This would be true when running directly on master or when
// testing on replica.
func (mctx *MigrationContext) InspectorIsAlsoApplier() bool {
	return mctx.InspectorConnectionConfig.Equals(mctx.ApplierConnectionConfig)
}

// HasMigrationRange tells us whether there's a range to iterate for copying rows.
// It will be `false` if the table is initially empty
func (mctx *MigrationContext) HasMigrationRange() bool {
	return mctx.MigrationRangeMinValues != nil && mctx.MigrationRangeMaxValues != nil
}

func (mctx *MigrationContext) SetCutOverLockTimeoutSeconds(timeoutSeconds int64) error {
	if timeoutSeconds < 1 {
		return fmt.Errorf("minimal timeout is 1sec. Timeout remains at %d", mctx.CutOverLockTimeoutSeconds)
	}
	if timeoutSeconds > 10 {
		return fmt.Errorf("maximal timeout is 10sec. Timeout remains at %d", mctx.CutOverLockTimeoutSeconds)
	}
	mctx.CutOverLockTimeoutSeconds = timeoutSeconds
	return nil
}

func (mctx *MigrationContext) SetExponentialBackoffMaxInterval(intervalSeconds int64) error {
	if intervalSeconds < 2 {
		return fmt.Errorf("minimal maximum interval is 2sec. Timeout remains at %d", mctx.ExponentialBackoffMaxInterval)
	}
	mctx.ExponentialBackoffMaxInterval = intervalSeconds
	return nil
}

func (mctx *MigrationContext) SetDefaultNumRetries(retries int64) {
	mctx.throttleMutex.Lock()
	defer mctx.throttleMutex.Unlock()
	if retries > 0 {
		mctx.defaultNumRetries = retries
	}
}

func (mctx *MigrationContext) MaxRetries() int64 {
	mctx.throttleMutex.Lock()
	defer mctx.throttleMutex.Unlock()
	retries := mctx.defaultNumRetries
	return retries
}

func (mctx *MigrationContext) IsTransactionalTable() bool {
	switch strings.ToLower(mctx.TableEngine) {
	case "innodb":
		{
			return true
		}
	case "tokudb":
		{
			return true
		}
	case "rocksdb":
		{
			return true
		}
	}
	return false
}

// SetCountTableRowsCancelFunc sets the cancel function for the CountTableRows query context
func (mctx *MigrationContext) SetCountTableRowsCancelFunc(f func()) {
	mctx.countMutex.Lock()
	defer mctx.countMutex.Unlock()

	mctx.countTableRowsCancelFunc = f
}

// IsCountingTableRows returns true if the migration has a table count query running
func (mctx *MigrationContext) IsCountingTableRows() bool {
	mctx.countMutex.Lock()
	defer mctx.countMutex.Unlock()

	return mctx.countTableRowsCancelFunc != nil
}

// CancelTableRowsCount cancels the CountTableRows query context. It is safe to
// call function even when IsCountingTableRows is false.
func (mctx *MigrationContext) CancelTableRowsCount() {
	mctx.countMutex.Lock()
	defer mctx.countMutex.Unlock()

	if mctx.countTableRowsCancelFunc == nil {
		return
	}

	mctx.countTableRowsCancelFunc()
	mctx.countTableRowsCancelFunc = nil
}

// ElapsedTime returns time since very beginning of the process
func (mctx *MigrationContext) ElapsedTime() time.Duration {
	return time.Since(mctx.StartTime)
}

// MarkRowCopyStartTime
func (mctx *MigrationContext) MarkRowCopyStartTime() {
	mctx.throttleMutex.Lock()
	defer mctx.throttleMutex.Unlock()
	mctx.RowCopyStartTime = time.Now()
}

// ElapsedRowCopyTime returns time since starting to copy chunks of rows
func (mctx *MigrationContext) ElapsedRowCopyTime() time.Duration {
	mctx.throttleMutex.Lock()
	defer mctx.throttleMutex.Unlock()

	if mctx.RowCopyStartTime.IsZero() {
		// Row copy hasn't started yet
		return 0
	}

	if mctx.RowCopyEndTime.IsZero() {
		return time.Since(mctx.RowCopyStartTime)
	}
	return mctx.RowCopyEndTime.Sub(mctx.RowCopyStartTime)
}

// ElapsedRowCopyTime returns time since starting to copy chunks of rows
func (mctx *MigrationContext) MarkRowCopyEndTime() {
	mctx.throttleMutex.Lock()
	defer mctx.throttleMutex.Unlock()
	mctx.RowCopyEndTime = time.Now()
}

func (mctx *MigrationContext) TimeSinceLastHeartbeatOnChangelog() time.Duration {
	return time.Since(mctx.GetLastHeartbeatOnChangelogTime())
}

func (mctx *MigrationContext) GetCurrentLagDuration() time.Duration {
	return time.Duration(atomic.LoadInt64(&mctx.CurrentLag))
}

func (mctx *MigrationContext) GetProgressPct() float64 {
	return math.Float64frombits(atomic.LoadUint64(&mctx.currentProgress))
}

func (mctx *MigrationContext) SetProgressPct(progressPct float64) {
	atomic.StoreUint64(&mctx.currentProgress, math.Float64bits(progressPct))
}

func (mctx *MigrationContext) GetETADuration() time.Duration {
	return time.Duration(atomic.LoadInt64(&mctx.etaNanoseonds))
}

func (mctx *MigrationContext) SetETADuration(etaDuration time.Duration) {
	atomic.StoreInt64(&mctx.etaNanoseonds, etaDuration.Nanoseconds())
}

func (mctx *MigrationContext) GetETASeconds() int64 {
	nano := atomic.LoadInt64(&mctx.etaNanoseonds)
	if nano < 0 {
		return ETAUnknown
	}
	return nano / int64(time.Second)
}

// math.Float64bits([f=0..100])

// GetTotalRowsCopied returns the accurate number of rows being copied (affected)
// This is not exactly the same as the rows being iterated via chunks, but potentially close enough
func (mctx *MigrationContext) GetTotalRowsCopied() int64 {
	return atomic.LoadInt64(&mctx.TotalRowsCopied)
}

func (mctx *MigrationContext) GetIteration() int64 {
	return atomic.LoadInt64(&mctx.Iteration)
}

func (mctx *MigrationContext) SetNextIterationRangeMinValues() {
	mctx.MigrationIterationRangeMinValues = mctx.MigrationIterationRangeMaxValues
	if mctx.MigrationIterationRangeMinValues == nil {
		mctx.MigrationIterationRangeMinValues = mctx.MigrationRangeMinValues
	}
}

func (mctx *MigrationContext) MarkPointOfInterest() int64 {
	mctx.pointOfInterestTimeMutex.Lock()
	defer mctx.pointOfInterestTimeMutex.Unlock()

	mctx.pointOfInterestTime = time.Now()
	return atomic.LoadInt64(&mctx.Iteration)
}

func (mctx *MigrationContext) TimeSincePointOfInterest() time.Duration {
	mctx.pointOfInterestTimeMutex.Lock()
	defer mctx.pointOfInterestTimeMutex.Unlock()

	return time.Since(mctx.pointOfInterestTime)
}

func (mctx *MigrationContext) SetLastHeartbeatOnChangelogTime(t time.Time) {
	mctx.lastHeartbeatOnChangelogMutex.Lock()
	defer mctx.lastHeartbeatOnChangelogMutex.Unlock()

	mctx.lastHeartbeatOnChangelogTime = t
}

func (mctx *MigrationContext) GetLastHeartbeatOnChangelogTime() time.Time {
	mctx.lastHeartbeatOnChangelogMutex.Lock()
	defer mctx.lastHeartbeatOnChangelogMutex.Unlock()

	return mctx.lastHeartbeatOnChangelogTime
}

func (mctx *MigrationContext) SetHeartbeatIntervalMilliseconds(heartbeatIntervalMilliseconds int64) {
	if heartbeatIntervalMilliseconds < 100 {
		heartbeatIntervalMilliseconds = 100
	}
	if heartbeatIntervalMilliseconds > 1000 {
		heartbeatIntervalMilliseconds = 1000
	}
	mctx.HeartbeatIntervalMilliseconds = heartbeatIntervalMilliseconds
}

func (mctx *MigrationContext) SetMaxLagMillisecondsThrottleThreshold(maxLagMillisecondsThrottleThreshold int64) {
	if maxLagMillisecondsThrottleThreshold < 100 {
		maxLagMillisecondsThrottleThreshold = 100
	}
	atomic.StoreInt64(&mctx.MaxLagMillisecondsThrottleThreshold, maxLagMillisecondsThrottleThreshold)
}

func (mctx *MigrationContext) SetChunkSize(chunkSize int64) {
	if chunkSize < 10 {
		chunkSize = 10
	}
	if chunkSize > 100000 {
		chunkSize = 100000
	}
	atomic.StoreInt64(&mctx.ChunkSize, chunkSize)
}

func (mctx *MigrationContext) SetDMLBatchSize(batchSize int64) {
	if batchSize < 1 {
		batchSize = 1
	}
	if batchSize > MaxEventsBatchSize {
		batchSize = MaxEventsBatchSize
	}
	atomic.StoreInt64(&mctx.DMLBatchSize, batchSize)
}

func (mctx *MigrationContext) SetThrottleGeneralCheckResult(checkResult *ThrottleCheckResult) *ThrottleCheckResult {
	mctx.throttleMutex.Lock()
	defer mctx.throttleMutex.Unlock()
	mctx.throttleGeneralCheckResult = *checkResult
	return checkResult
}

func (mctx *MigrationContext) GetThrottleGeneralCheckResult() *ThrottleCheckResult {
	mctx.throttleMutex.Lock()
	defer mctx.throttleMutex.Unlock()
	result := mctx.throttleGeneralCheckResult
	return &result
}

func (mctx *MigrationContext) SetThrottled(throttle bool, reason string, reasonHint ThrottleReasonHint) {
	mctx.throttleMutex.Lock()
	defer mctx.throttleMutex.Unlock()
	mctx.isThrottled = throttle
	mctx.throttleReason = reason
	mctx.throttleReasonHint = reasonHint
}

func (mctx *MigrationContext) IsThrottled() (bool, string, ThrottleReasonHint) {
	mctx.throttleMutex.Lock()
	defer mctx.throttleMutex.Unlock()

	// we don't throttle when cutting over. We _do_ throttle:
	// - during copy phase
	// - just before cut-over
	// - in between cut-over retries
	// When cutting over, we need to be aggressive. Cut-over holds table locks.
	// We need to release those asap.
	if atomic.LoadInt64(&mctx.InCutOverCriticalSectionFlag) > 0 {
		return false, "critical section", NoThrottleReasonHint
	}
	return mctx.isThrottled, mctx.throttleReason, mctx.throttleReasonHint
}

func (mctx *MigrationContext) GetThrottleQuery() string {
	mctx.throttleMutex.Lock()
	defer mctx.throttleMutex.Unlock()

	var query = mctx.throttleQuery
	return query
}

func (mctx *MigrationContext) SetThrottleQuery(newQuery string) {
	mctx.throttleMutex.Lock()
	defer mctx.throttleMutex.Unlock()

	mctx.throttleQuery = newQuery
}

func (mctx *MigrationContext) GetThrottleHTTP() string {
	mctx.throttleHTTPMutex.Lock()
	defer mctx.throttleHTTPMutex.Unlock()

	var throttleHTTP = mctx.throttleHTTP
	return throttleHTTP
}

func (mctx *MigrationContext) SetThrottleHTTP(throttleHTTP string) {
	mctx.throttleHTTPMutex.Lock()
	defer mctx.throttleHTTPMutex.Unlock()

	mctx.throttleHTTP = throttleHTTP
}

func (mctx *MigrationContext) SetIgnoreHTTPErrors(ignoreHTTPErrors bool) {
	mctx.throttleHTTPMutex.Lock()
	defer mctx.throttleHTTPMutex.Unlock()

	mctx.IgnoreHTTPErrors = ignoreHTTPErrors
}

func (mctx *MigrationContext) GetMaxLoad() LoadMap {
	mctx.throttleMutex.Lock()
	defer mctx.throttleMutex.Unlock()

	return mctx.maxLoad.Duplicate()
}

func (mctx *MigrationContext) GetCriticalLoad() LoadMap {
	mctx.throttleMutex.Lock()
	defer mctx.throttleMutex.Unlock()

	return mctx.criticalLoad.Duplicate()
}

func (mctx *MigrationContext) GetNiceRatio() float64 {
	mctx.throttleMutex.Lock()
	defer mctx.throttleMutex.Unlock()

	return mctx.niceRatio
}

func (mctx *MigrationContext) SetNiceRatio(newRatio float64) {
	if newRatio < 0.0 {
		newRatio = 0.0
	}
	if newRatio > 100.0 {
		newRatio = 100.0
	}

	mctx.throttleMutex.Lock()
	defer mctx.throttleMutex.Unlock()
	mctx.niceRatio = newRatio
}

func (mctx *MigrationContext) GetRecentBinlogCoordinates() mysql.BinlogCoordinates {
	mctx.throttleMutex.Lock()
	defer mctx.throttleMutex.Unlock()

	return mctx.recentBinlogCoordinates
}

func (mctx *MigrationContext) SetRecentBinlogCoordinates(coordinates mysql.BinlogCoordinates) {
	mctx.throttleMutex.Lock()
	defer mctx.throttleMutex.Unlock()
	mctx.recentBinlogCoordinates = coordinates
}

// ReadMaxLoad parses the `--max-load` flag, which is in multiple key-value format,
// such as: 'Threads_running=100,Threads_connected=500'
// It only applies changes in case there's no parsing error.
func (mctx *MigrationContext) ReadMaxLoad(maxLoadList string) error {
	loadMap, err := ParseLoadMap(maxLoadList)
	if err != nil {
		return err
	}
	mctx.throttleMutex.Lock()
	defer mctx.throttleMutex.Unlock()

	mctx.maxLoad = loadMap
	return nil
}

// ReadCriticalLoad parses the `--max-load` flag, which is in multiple key-value format,
// such as: 'Threads_running=100,Threads_connected=500'
// It only applies changes in case there's no parsing error.
func (mctx *MigrationContext) ReadCriticalLoad(criticalLoadList string) error {
	loadMap, err := ParseLoadMap(criticalLoadList)
	if err != nil {
		return err
	}
	mctx.throttleMutex.Lock()
	defer mctx.throttleMutex.Unlock()

	mctx.criticalLoad = loadMap
	return nil
}

func (mctx *MigrationContext) GetControlReplicasLagResult() mysql.ReplicationLagResult {
	mctx.throttleMutex.Lock()
	defer mctx.throttleMutex.Unlock()

	lagResult := mctx.controlReplicasLagResult
	return lagResult
}

func (mctx *MigrationContext) SetControlReplicasLagResult(lagResult *mysql.ReplicationLagResult) {
	mctx.throttleMutex.Lock()
	defer mctx.throttleMutex.Unlock()
	if lagResult == nil {
		mctx.controlReplicasLagResult = *mysql.NewNoReplicationLagResult()
	} else {
		mctx.controlReplicasLagResult = *lagResult
	}
}

func (mctx *MigrationContext) GetThrottleControlReplicaKeys() *mysql.InstanceKeyMap {
	mctx.throttleMutex.Lock()
	defer mctx.throttleMutex.Unlock()

	keys := mysql.NewInstanceKeyMap()
	keys.AddKeys(mctx.throttleControlReplicaKeys.GetInstanceKeys())
	return keys
}

func (mctx *MigrationContext) ReadThrottleControlReplicaKeys(throttleControlReplicas string) error {
	keys := mysql.NewInstanceKeyMap()
	if err := keys.ReadCommaDelimitedList(throttleControlReplicas); err != nil {
		return err
	}

	mctx.throttleMutex.Lock()
	defer mctx.throttleMutex.Unlock()

	mctx.throttleControlReplicaKeys = keys
	return nil
}

func (mctx *MigrationContext) AddThrottleControlReplicaKey(key mysql.InstanceKey) error {
	mctx.throttleMutex.Lock()
	defer mctx.throttleMutex.Unlock()

	mctx.throttleControlReplicaKeys.AddKey(key)
	return nil
}

// ApplyCredentials sorts out the credentials between the config file and the CLI flags
func (mctx *MigrationContext) ApplyCredentials() {
	mctx.configMutex.Lock()
	defer mctx.configMutex.Unlock()

	if mctx.config.Client.User != "" {
		mctx.InspectorConnectionConfig.User = mctx.config.Client.User
	}
	if mctx.CliUser != "" {
		// Override
		mctx.InspectorConnectionConfig.User = mctx.CliUser
	}
	if mctx.config.Client.Password != "" {
		mctx.InspectorConnectionConfig.Password = mctx.config.Client.Password
	}
	if mctx.CliPassword != "" {
		// Override
		mctx.InspectorConnectionConfig.Password = mctx.CliPassword
	}
}

func (mctx *MigrationContext) SetupTLS() error {
	if mctx.UseTLS {
		return mctx.InspectorConnectionConfig.UseTLS(mctx.TLSCACertificate, mctx.TLSCertificate, mctx.TLSKey, mctx.TLSAllowInsecure)
	}
	return nil
}

// ReadConfigFile attempts to read the config file, if it exists
func (mctx *MigrationContext) ReadConfigFile() error {
	mctx.configMutex.Lock()
	defer mctx.configMutex.Unlock()

	if mctx.ConfigFile == "" {
		return nil
	}
	cfg, err := ini.Load(mctx.ConfigFile)
	if err != nil {
		return err
	}

	if cfg.Section("client").HasKey("user") {
		mctx.config.Client.User = cfg.Section("client").Key("user").String()
	}

	if cfg.Section("client").HasKey("password") {
		mctx.config.Client.Password = cfg.Section("client").Key("password").String()
	}

	if cfg.Section("osc").HasKey("chunk_size") {
		mctx.config.Osc.Chunk_Size, err = cfg.Section("osc").Key("chunk_size").Int64()
		if err != nil {
			return fmt.Errorf("unable to read osc chunk size: %w", err)
		}
	}

	if cfg.Section("osc").HasKey("max_load") {
		mctx.config.Osc.Max_Load = cfg.Section("osc").Key("max_load").String()
	}

	if cfg.Section("osc").HasKey("replication_lag_query") {
		mctx.config.Osc.Replication_Lag_Query = cfg.Section("osc").Key("replication_lag_query").String()
	}

	if cfg.Section("osc").HasKey("max_lag_millis") {
		mctx.config.Osc.Max_Lag_Millis, err = cfg.Section("osc").Key("max_lag_millis").Int64()
		if err != nil {
			return fmt.Errorf("unable to read max lag millis: %w", err)
		}
	}

	// We accept user & password in the form "${SOME_ENV_VARIABLE}" in which case we pull
	// the given variable from os env
	if submatch := envVariableRegexp.FindStringSubmatch(mctx.config.Client.User); len(submatch) > 1 {
		mctx.config.Client.User = os.Getenv(submatch[1])
	}
	if submatch := envVariableRegexp.FindStringSubmatch(mctx.config.Client.Password); len(submatch) > 1 {
		mctx.config.Client.Password = os.Getenv(submatch[1])
	}

	return nil
}

// getGhostTriggerName generates the name of a ghost trigger, based on original trigger name
// or a given trigger name
func (mctx *MigrationContext) GetGhostTriggerName(triggerName string) string {
	if mctx.RemoveTriggerSuffix && strings.HasSuffix(triggerName, mctx.TriggerSuffix) {
		return strings.TrimSuffix(triggerName, mctx.TriggerSuffix)
	}
	// else
	return triggerName + mctx.TriggerSuffix
}

// ValidateGhostTriggerLengthBelowMaxLength checks if the given trigger name (already transformed
// by GetGhostTriggerName) does not exceed the maximum allowed length.
func (mctx *MigrationContext) ValidateGhostTriggerLengthBelowMaxLength(triggerName string) bool {
	return utf8.RuneCountInString(triggerName) <= mysql.MaxTableNameLength
}

// GetContext returns the migration context for cancellation checking
func (mctx *MigrationContext) GetContext() context.Context {
	return mctx.ctx
}

// SetAbortError stores the fatal error that triggered abort
// Only the first error is stored (subsequent errors are ignored)
func (mctx *MigrationContext) SetAbortError(err error) {
	mctx.abortMutex.Lock()
	defer mctx.abortMutex.Unlock()
	if mctx.AbortError == nil {
		mctx.AbortError = err
	}
}

// GetAbortError retrieves the stored abort error
func (mctx *MigrationContext) GetAbortError() error {
	mctx.abortMutex.Lock()
	defer mctx.abortMutex.Unlock()
	return mctx.AbortError
}

// CancelContext cancels the migration context to signal all goroutines to stop
// The cancel function is safe to call multiple times and from multiple goroutines.
func (mctx *MigrationContext) CancelContext() {
	if mctx.cancelFunc != nil {
		mctx.cancelFunc()
	}
}

// SendWithContext attempts to send a value to a channel, but returns early
// if the context is cancelled. This prevents goroutine deadlocks when the
// channel receiver has exited due to an error.
//
// Use this instead of bare channel sends (ch <- val) in goroutines to ensure
// proper cleanup when the migration is aborted.
//
// Example:
//
//	if err := base.SendWithContext(ctx, ch, value); err != nil {
//	    return err  // context was cancelled
//	}
func SendWithContext[T any](ctx context.Context, ch chan<- T, val T) error {
	select {
	case ch <- val:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
