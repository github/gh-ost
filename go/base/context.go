/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package base

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"

	"gopkg.in/gcfg.v1"
	gcfgscanner "gopkg.in/gcfg.v1/scanner"
)

// RowsEstimateMethod is the type of row number estimation
type RowsEstimateMethod string

const (
	TableStatusRowsEstimate RowsEstimateMethod = "TableStatusRowsEstimate"
	ExplainRowsEstimate                        = "ExplainRowsEstimate"
	CountRowsEstimate                          = "CountRowsEstimate"
)

type CutOver int

const (
	CutOverAtomic  CutOver = iota
	CutOverTwoStep         = iota
)

type ThrottleReasonHint string

const (
	NoThrottleReasonHint          ThrottleReasonHint = "NoThrottleReasonHint"
	UserCommandThrottleReasonHint                    = "UserCommandThrottleReasonHint"
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
	DatabaseName      string
	OriginalTableName string
	AlterStatement    string

	CountTableRows           bool
	ConcurrentCountTableRows bool
	AllowedRunningOnMaster   bool
	AllowedMasterMaster      bool
	SwitchToRowBinlogFormat  bool
	AssumeRBR                bool
	SkipForeignKeyChecks     bool
	NullableUniqueKeyAllowed bool
	ApproveRenamedColumns    bool
	SkipRenamedColumns       bool
	IsTungsten               bool
	DiscardForeignKeys       bool
	Resurrect                bool

	config            ContextConfig
	configMutex       *sync.Mutex
	ConfigFile        string
	CliUser           string
	cliPassword       string
	CliMasterUser     string
	cliMasterPassword string

	HeartbeatIntervalMilliseconds       int64
	defaultNumRetries                   int64
	ChunkSize                           int64
	niceRatio                           float64
	MaxLagMillisecondsThrottleThreshold int64
	replicationLagQuery                 string
	throttleControlReplicaKeys          *mysql.InstanceKeyMap
	ThrottleFlagFile                    string
	ThrottleAdditionalFlagFile          string
	throttleQuery                       string
	ThrottleCommandedByUser             int64
	maxLoad                             LoadMap
	criticalLoad                        LoadMap
	CriticalLoadIntervalMilliseconds    int64
	PostponeCutOverFlagFile             string
	CutOverLockTimeoutSeconds           int64
	ForceNamedCutOverCommand            bool
	PanicFlagFile                       string
	HooksPath                           string
	HooksHintMessage                    string

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
	CutOverType                  CutOver

	Hostname                               string
	AssumeMasterHostname                   string
	ApplierTimeZone                        string
	TableEngine                            string
	RowsEstimate                           int64
	RowsDeltaEstimate                      int64
	UsedRowsEstimateMethod                 RowsEstimateMethod
	HasSuperPrivilege                      bool
	OriginalBinlogFormat                   string
	OriginalBinlogRowImage                 string
	InspectorConnectionConfig              *mysql.ConnectionConfig
	ApplierConnectionConfig                *mysql.ConnectionConfig
	StartTime                              time.Time
	RowCopyStartTime                       time.Time
	RowCopyEndTime                         time.Time
	LockTablesStartTime                    time.Time
	RenameTablesStartTime                  time.Time
	RenameTablesEndTime                    time.Time
	pointOfInterestTime                    time.Time
	pointOfInterestTimeMutex               *sync.Mutex
	CurrentLag                             int64
	controlReplicasLagResult               mysql.ReplicationLagResult
	TotalRowsCopied                        int64
	TotalDMLEventsApplied                  int64
	isThrottled                            bool
	throttleReason                         string
	throttleReasonHint                     ThrottleReasonHint
	throttleGeneralCheckResult             ThrottleCheckResult
	throttleMutex                          *sync.Mutex
	IsPostponingCutOver                    int64
	CountingRowsFlag                       int64
	AllEventsUpToLockProcessedInjectedFlag int64
	CleanupImminentFlag                    int64
	UserCommandedUnpostponeFlag            int64
	CutOverCompleteFlag                    int64
	InCutOverCriticalSectionFlag           int64
	IsResurrected                          int64

	OriginalTableColumnsOnApplier    *sql.ColumnList
	OriginalTableColumns             *sql.ColumnList
	OriginalTableUniqueKeys          [](*sql.UniqueKey)
	GhostTableColumns                *sql.ColumnList
	GhostTableUniqueKeys             [](*sql.UniqueKey)
	UniqueKey                        *sql.UniqueKey
	SharedColumns                    *sql.ColumnList
	ColumnRenameMap                  map[string]string
	MappedSharedColumns              *sql.ColumnList
	MigrationRangeMinValues          *sql.ColumnValues
	MigrationRangeMaxValues          *sql.ColumnValues
	Iteration                        int64
	MigrationIterationRangeMinValues *sql.ColumnValues
	MigrationIterationRangeMaxValues *sql.ColumnValues
	EncodedRangeValues               map[string]string
	AppliedBinlogCoordinates         mysql.BinlogCoordinates
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

var context *MigrationContext

func init() {
	context = NewMigrationContext()
}

func NewMigrationContext() *MigrationContext {
	return &MigrationContext{
		defaultNumRetries:                   60,
		ChunkSize:                           1000,
		InspectorConnectionConfig:           mysql.NewConnectionConfig(),
		ApplierConnectionConfig:             mysql.NewConnectionConfig(),
		MaxLagMillisecondsThrottleThreshold: 1500,
		CutOverLockTimeoutSeconds:           3,
		maxLoad:                             NewLoadMap(),
		criticalLoad:                        NewLoadMap(),
		throttleMutex:                       &sync.Mutex{},
		throttleControlReplicaKeys:          mysql.NewInstanceKeyMap(),
		configMutex:                         &sync.Mutex{},
		pointOfInterestTimeMutex:            &sync.Mutex{},
		AppliedBinlogCoordinates:            mysql.BinlogCoordinates{},
		ColumnRenameMap:                     make(map[string]string),
		EncodedRangeValues:                  make(map[string]string),
	}
}

// GetMigrationContext
func GetMigrationContext() *MigrationContext {
	return context
}

// DumpJSON exports this config to JSON string and writes it to file
func (this *MigrationContext) ToJSON() (string, error) {
	this.throttleMutex.Lock()
	defer this.throttleMutex.Unlock()

	if this.MigrationRangeMinValues != nil {
		this.EncodedRangeValues["MigrationRangeMinValues"], _ = this.MigrationRangeMinValues.ToBase64()
	}
	if this.MigrationRangeMaxValues != nil {
		this.EncodedRangeValues["MigrationRangeMaxValues"], _ = this.MigrationRangeMaxValues.ToBase64()
	}
	if this.MigrationIterationRangeMinValues != nil {
		this.EncodedRangeValues["MigrationIterationRangeMinValues"], _ = this.MigrationIterationRangeMinValues.ToBase64()
	}
	if this.MigrationIterationRangeMaxValues != nil {
		this.EncodedRangeValues["MigrationIterationRangeMaxValues"], _ = this.MigrationIterationRangeMaxValues.ToBase64()
	}
	jsonBytes, err := json.Marshal(this)
	if err != nil {
		return "", err
	}
	return string(jsonBytes), nil
}

// LoadJSON treats given json as context-dump, and attempts to load this context's data.
func (this *MigrationContext) LoadJSON(jsonString string) error {
	this.throttleMutex.Lock()
	defer this.throttleMutex.Unlock()

	jsonBytes := []byte(jsonString)

	if err := json.Unmarshal(jsonBytes, this); err != nil && err != io.EOF {
		return err
	}

	var err error
	if this.MigrationRangeMinValues, err = sql.NewColumnValuesFromBase64(this.EncodedRangeValues["MigrationRangeMinValues"]); err != nil {
		return err
	}
	if this.MigrationRangeMaxValues, err = sql.NewColumnValuesFromBase64(this.EncodedRangeValues["MigrationRangeMaxValues"]); err != nil {
		return err
	}
	if this.MigrationIterationRangeMinValues, err = sql.NewColumnValuesFromBase64(this.EncodedRangeValues["MigrationIterationRangeMinValues"]); err != nil {
		return err
	}
	if this.MigrationIterationRangeMaxValues, err = sql.NewColumnValuesFromBase64(this.EncodedRangeValues["MigrationIterationRangeMaxValues"]); err != nil {
		return err
	}

	return nil
}

// ApplyResurrectedContext loads resurrection-related infor from given context
func (this *MigrationContext) ApplyResurrectedContext(other *MigrationContext) {
	// this.MigrationRangeMinValues = other.MigrationRangeMinValues
	// this.MigrationRangeMaxValues = other.MigrationRangeMaxValues
	if other.MigrationIterationRangeMinValues != nil {
		this.MigrationIterationRangeMinValues = other.MigrationIterationRangeMinValues
	}
	if other.MigrationIterationRangeMaxValues != nil {
		this.MigrationIterationRangeMaxValues = other.MigrationIterationRangeMaxValues
	}

	this.RowsEstimate = other.RowsEstimate
	this.RowsDeltaEstimate = other.RowsDeltaEstimate
	this.TotalRowsCopied = other.TotalRowsCopied
	this.TotalDMLEventsApplied = other.TotalDMLEventsApplied

	this.Iteration = other.Iteration
	this.AppliedBinlogCoordinates = other.AppliedBinlogCoordinates
}

// GetGhostTableName generates the name of ghost table, based on original table name
func (this *MigrationContext) GetGhostTableName() string {
	return fmt.Sprintf("_%s_gho", this.OriginalTableName)
}

// GetOldTableName generates the name of the "old" table, into which the original table is renamed.
func (this *MigrationContext) GetOldTableName() string {
	if this.TestOnReplica {
		return fmt.Sprintf("_%s_delr", this.OriginalTableName)
	}
	if this.MigrateOnReplica {
		return fmt.Sprintf("_%s_delr", this.OriginalTableName)
	}
	return fmt.Sprintf("_%s_del", this.OriginalTableName)
}

// GetChangelogTableName generates the name of changelog table, based on original table name
func (this *MigrationContext) GetChangelogTableName() string {
	return fmt.Sprintf("_%s_ghc", this.OriginalTableName)
}

// GetVoluntaryLockName returns a name of a voluntary lock to be used throughout
// the swap-tables process.
func (this *MigrationContext) GetVoluntaryLockName() string {
	return fmt.Sprintf("%s.%s.lock", this.DatabaseName, this.OriginalTableName)
}

// RequiresBinlogFormatChange is `true` when the original binlog format isn't `ROW`
func (this *MigrationContext) RequiresBinlogFormatChange() bool {
	return this.OriginalBinlogFormat != "ROW"
}

// GetApplierHostname is a safe access method to the applier hostname
func (this *MigrationContext) GetApplierHostname() string {
	if this.ApplierConnectionConfig == nil {
		return ""
	}
	if this.ApplierConnectionConfig.ImpliedKey == nil {
		return ""
	}
	return this.ApplierConnectionConfig.ImpliedKey.Hostname
}

// GetInspectorHostname is a safe access method to the inspector hostname
func (this *MigrationContext) GetInspectorHostname() string {
	if this.InspectorConnectionConfig == nil {
		return ""
	}
	if this.InspectorConnectionConfig.ImpliedKey == nil {
		return ""
	}
	return this.InspectorConnectionConfig.ImpliedKey.Hostname
}

// InspectorIsAlsoApplier is `true` when the both inspector and applier are the
// same database instance. This would be true when running directly on master or when
// testing on replica.
func (this *MigrationContext) InspectorIsAlsoApplier() bool {
	return this.InspectorConnectionConfig.Equals(this.ApplierConnectionConfig)
}

// HasMigrationRange tells us whether there's a range to iterate for copying rows.
// It will be `false` if the table is initially empty
func (this *MigrationContext) HasMigrationRange() bool {
	return this.MigrationRangeMinValues != nil && this.MigrationRangeMaxValues != nil
}

func (this *MigrationContext) SetCutOverLockTimeoutSeconds(timeoutSeconds int64) error {
	if timeoutSeconds < 1 {
		return fmt.Errorf("Minimal timeout is 1sec. Timeout remains at %d", this.CutOverLockTimeoutSeconds)
	}
	if timeoutSeconds > 10 {
		return fmt.Errorf("Maximal timeout is 10sec. Timeout remains at %d", this.CutOverLockTimeoutSeconds)
	}
	this.CutOverLockTimeoutSeconds = timeoutSeconds
	return nil
}

func (this *MigrationContext) SetDefaultNumRetries(retries int64) {
	this.throttleMutex.Lock()
	defer this.throttleMutex.Unlock()
	if retries > 0 {
		this.defaultNumRetries = retries
	}
}

func (this *MigrationContext) MaxRetries() int64 {
	this.throttleMutex.Lock()
	defer this.throttleMutex.Unlock()
	retries := this.defaultNumRetries
	return retries
}

func (this *MigrationContext) IsTransactionalTable() bool {
	switch strings.ToLower(this.TableEngine) {
	case "innodb":
		{
			return true
		}
	case "tokudb":
		{
			return true
		}
	}
	return false
}

// ElapsedTime returns time since very beginning of the process
func (this *MigrationContext) ElapsedTime() time.Duration {
	return time.Since(this.StartTime)
}

// MarkRowCopyStartTime
func (this *MigrationContext) MarkRowCopyStartTime() {
	this.throttleMutex.Lock()
	defer this.throttleMutex.Unlock()
	this.RowCopyStartTime = time.Now()
}

// ElapsedRowCopyTime returns time since starting to copy chunks of rows
func (this *MigrationContext) ElapsedRowCopyTime() time.Duration {
	this.throttleMutex.Lock()
	defer this.throttleMutex.Unlock()

	if this.RowCopyStartTime.IsZero() {
		// Row copy hasn't started yet
		return 0
	}

	if this.RowCopyEndTime.IsZero() {
		return time.Since(this.RowCopyStartTime)
	}
	return this.RowCopyEndTime.Sub(this.RowCopyStartTime)
}

// ElapsedRowCopyTime returns time since starting to copy chunks of rows
func (this *MigrationContext) MarkRowCopyEndTime() {
	this.throttleMutex.Lock()
	defer this.throttleMutex.Unlock()
	this.RowCopyEndTime = time.Now()
}

// GetTotalRowsCopied returns the accurate number of rows being copied (affected)
// This is not exactly the same as the rows being iterated via chunks, but potentially close enough
func (this *MigrationContext) GetTotalRowsCopied() int64 {
	return atomic.LoadInt64(&this.TotalRowsCopied)
}

func (this *MigrationContext) GetIteration() int64 {
	return atomic.LoadInt64(&this.Iteration)
}

func (this *MigrationContext) MarkPointOfInterest() int64 {
	this.pointOfInterestTimeMutex.Lock()
	defer this.pointOfInterestTimeMutex.Unlock()

	this.pointOfInterestTime = time.Now()
	return atomic.LoadInt64(&this.Iteration)
}

func (this *MigrationContext) TimeSincePointOfInterest() time.Duration {
	this.pointOfInterestTimeMutex.Lock()
	defer this.pointOfInterestTimeMutex.Unlock()

	return time.Since(this.pointOfInterestTime)
}

func (this *MigrationContext) SetHeartbeatIntervalMilliseconds(heartbeatIntervalMilliseconds int64) {
	if heartbeatIntervalMilliseconds < 100 {
		heartbeatIntervalMilliseconds = 100
	}
	if heartbeatIntervalMilliseconds > 1000 {
		heartbeatIntervalMilliseconds = 1000
	}
	this.HeartbeatIntervalMilliseconds = heartbeatIntervalMilliseconds
}

func (this *MigrationContext) SetMaxLagMillisecondsThrottleThreshold(maxLagMillisecondsThrottleThreshold int64) {
	if maxLagMillisecondsThrottleThreshold < 100 {
		maxLagMillisecondsThrottleThreshold = 100
	}
	atomic.StoreInt64(&this.MaxLagMillisecondsThrottleThreshold, maxLagMillisecondsThrottleThreshold)
}

func (this *MigrationContext) SetChunkSize(chunkSize int64) {
	if chunkSize < 100 {
		chunkSize = 100
	}
	if chunkSize > 100000 {
		chunkSize = 100000
	}
	atomic.StoreInt64(&this.ChunkSize, chunkSize)
}

func (this *MigrationContext) SetThrottleGeneralCheckResult(checkResult *ThrottleCheckResult) *ThrottleCheckResult {
	this.throttleMutex.Lock()
	defer this.throttleMutex.Unlock()
	this.throttleGeneralCheckResult = *checkResult
	return checkResult
}

func (this *MigrationContext) GetThrottleGeneralCheckResult() *ThrottleCheckResult {
	this.throttleMutex.Lock()
	defer this.throttleMutex.Unlock()
	result := this.throttleGeneralCheckResult
	return &result
}

func (this *MigrationContext) SetThrottled(throttle bool, reason string, reasonHint ThrottleReasonHint) {
	this.throttleMutex.Lock()
	defer this.throttleMutex.Unlock()
	this.isThrottled = throttle
	this.throttleReason = reason
	this.throttleReasonHint = reasonHint
}

func (this *MigrationContext) IsThrottled() (bool, string, ThrottleReasonHint) {
	this.throttleMutex.Lock()
	defer this.throttleMutex.Unlock()

	// we don't throttle when cutting over. We _do_ throttle:
	// - during copy phase
	// - just before cut-over
	// - in between cut-over retries
	// When cutting over, we need to be aggressive. Cut-over holds table locks.
	// We need to release those asap.
	if atomic.LoadInt64(&this.InCutOverCriticalSectionFlag) > 0 {
		return false, "critical section", NoThrottleReasonHint
	}
	return this.isThrottled, this.throttleReason, this.throttleReasonHint
}

func (this *MigrationContext) GetReplicationLagQuery() string {
	var query string

	this.throttleMutex.Lock()
	defer this.throttleMutex.Unlock()

	query = this.replicationLagQuery
	return query
}

func (this *MigrationContext) SetReplicationLagQuery(newQuery string) {
	this.throttleMutex.Lock()
	defer this.throttleMutex.Unlock()

	this.replicationLagQuery = newQuery
}

func (this *MigrationContext) GetThrottleQuery() string {
	var query string

	this.throttleMutex.Lock()
	defer this.throttleMutex.Unlock()

	query = this.throttleQuery
	return query
}

func (this *MigrationContext) SetThrottleQuery(newQuery string) {
	this.throttleMutex.Lock()
	defer this.throttleMutex.Unlock()

	this.throttleQuery = newQuery
}

func (this *MigrationContext) GetMaxLoad() LoadMap {
	this.throttleMutex.Lock()
	defer this.throttleMutex.Unlock()

	return this.maxLoad.Duplicate()
}

func (this *MigrationContext) GetCriticalLoad() LoadMap {
	this.throttleMutex.Lock()
	defer this.throttleMutex.Unlock()

	return this.criticalLoad.Duplicate()
}

func (this *MigrationContext) GetNiceRatio() float64 {
	this.throttleMutex.Lock()
	defer this.throttleMutex.Unlock()

	return this.niceRatio
}

func (this *MigrationContext) SetNiceRatio(newRatio float64) {
	if newRatio < 0.0 {
		newRatio = 0.0
	}
	if newRatio > 100.0 {
		newRatio = 100.0
	}

	this.throttleMutex.Lock()
	defer this.throttleMutex.Unlock()
	this.niceRatio = newRatio
}

func (this *MigrationContext) SetAppliedBinlogCoordinates(binlogCoordinates *mysql.BinlogCoordinates) {
	this.throttleMutex.Lock()
	defer this.throttleMutex.Unlock()

	this.AppliedBinlogCoordinates = *binlogCoordinates
}

// ReadMaxLoad parses the `--max-load` flag, which is in multiple key-value format,
// such as: 'Threads_running=100,Threads_connected=500'
// It only applies changes in case there's no parsing error.
func (this *MigrationContext) ReadMaxLoad(maxLoadList string) error {
	loadMap, err := ParseLoadMap(maxLoadList)
	if err != nil {
		return err
	}
	this.throttleMutex.Lock()
	defer this.throttleMutex.Unlock()

	this.maxLoad = loadMap
	return nil
}

// ReadMaxLoad parses the `--max-load` flag, which is in multiple key-value format,
// such as: 'Threads_running=100,Threads_connected=500'
// It only applies changes in case there's no parsing error.
func (this *MigrationContext) ReadCriticalLoad(criticalLoadList string) error {
	loadMap, err := ParseLoadMap(criticalLoadList)
	if err != nil {
		return err
	}
	this.throttleMutex.Lock()
	defer this.throttleMutex.Unlock()

	this.criticalLoad = loadMap
	return nil
}

func (this *MigrationContext) GetControlReplicasLagResult() mysql.ReplicationLagResult {
	this.throttleMutex.Lock()
	defer this.throttleMutex.Unlock()

	lagResult := this.controlReplicasLagResult
	return lagResult
}

func (this *MigrationContext) SetControlReplicasLagResult(lagResult *mysql.ReplicationLagResult) {
	this.throttleMutex.Lock()
	defer this.throttleMutex.Unlock()
	this.controlReplicasLagResult = *lagResult
}

func (this *MigrationContext) GetThrottleControlReplicaKeys() *mysql.InstanceKeyMap {
	this.throttleMutex.Lock()
	defer this.throttleMutex.Unlock()

	keys := mysql.NewInstanceKeyMap()
	keys.AddKeys(this.throttleControlReplicaKeys.GetInstanceKeys())
	return keys
}

func (this *MigrationContext) ReadThrottleControlReplicaKeys(throttleControlReplicas string) error {
	keys := mysql.NewInstanceKeyMap()
	if err := keys.ReadCommaDelimitedList(throttleControlReplicas); err != nil {
		return err
	}

	this.throttleMutex.Lock()
	defer this.throttleMutex.Unlock()

	this.throttleControlReplicaKeys = keys
	return nil
}

func (this *MigrationContext) AddThrottleControlReplicaKey(key mysql.InstanceKey) error {
	this.throttleMutex.Lock()
	defer this.throttleMutex.Unlock()

	this.throttleControlReplicaKeys.AddKey(key)
	return nil
}

func (this *MigrationContext) SetCliPassword(password string) {
	this.cliPassword = password
}

func (this *MigrationContext) SetCliMasterPassword(password string) {
	this.cliMasterPassword = password
}

func (this *MigrationContext) GetCliMasterPassword() string {
	return this.cliMasterPassword
}

// ApplyCredentials sorts out the credentials between the config file and the CLI flags
func (this *MigrationContext) ApplyCredentials() {
	this.configMutex.Lock()
	defer this.configMutex.Unlock()

	if this.config.Client.User != "" {
		this.InspectorConnectionConfig.User = this.config.Client.User
	}
	if this.CliUser != "" {
		// Override
		this.InspectorConnectionConfig.User = this.CliUser
	}
	if this.config.Client.Password != "" {
		this.InspectorConnectionConfig.Password = this.config.Client.Password
	}
	if this.cliPassword != "" {
		// Override
		this.InspectorConnectionConfig.Password = this.cliPassword
	}
}

// ReadConfigFile attempts to read the config file, if it exists
func (this *MigrationContext) ReadConfigFile() error {
	this.configMutex.Lock()
	defer this.configMutex.Unlock()

	if this.ConfigFile == "" {
		return nil
	}
	gcfg.RelaxedParserMode = true
	gcfgscanner.RelaxedScannerMode = true
	if err := gcfg.ReadFileInto(&this.config, this.ConfigFile); err != nil {
		return err
	}

	// We accept user & password in the form "${SOME_ENV_VARIABLE}" in which case we pull
	// the given variable from os env
	if submatch := envVariableRegexp.FindStringSubmatch(this.config.Client.User); len(submatch) > 1 {
		this.config.Client.User = os.Getenv(submatch[1])
	}
	if submatch := envVariableRegexp.FindStringSubmatch(this.config.Client.Password); len(submatch) > 1 {
		this.config.Client.Password = os.Getenv(submatch[1])
	}

	return nil
}
