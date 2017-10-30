/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"
	"github.com/outbrain/golib/sqlutils"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
)

// Throttler collects metrics related to throttling and makes informed decisison
// whether throttling should take place.
type Throttler struct {
	migrationContext *base.MigrationContext
	applier          *Applier
	inspector        *Inspector
}

func NewThrottler(applier *Applier, inspector *Inspector) *Throttler {
	return &Throttler{
		migrationContext: base.GetMigrationContext(),
		applier:          applier,
		inspector:        inspector,
	}
}

// shouldThrottle performs checks to see whether we should currently be throttling.
// It merely observes the metrics collected by other components, it does not issue
// its own metric collection.
func (this *Throttler) shouldThrottle() (result bool, reason string, reasonHint base.ThrottleReasonHint) {
	generalCheckResult := this.migrationContext.GetThrottleGeneralCheckResult()
	if generalCheckResult.ShouldThrottle {
		return generalCheckResult.ShouldThrottle, generalCheckResult.Reason, generalCheckResult.ReasonHint
	}
	// HTTP throttle
	statusCode := atomic.LoadInt64(&this.migrationContext.ThrottleHTTPStatusCode)
	if statusCode != 0 && statusCode != http.StatusOK {
		return true, fmt.Sprintf("http=%d", statusCode), base.NoThrottleReasonHint
	}

	// 延迟太大了
	// Replication lag throttle
	maxLagMillisecondsThrottleThreshold := atomic.LoadInt64(&this.migrationContext.MaxLagMillisecondsThrottleThreshold)
	lag := atomic.LoadInt64(&this.migrationContext.CurrentLag)
	if time.Duration(lag) > time.Duration(maxLagMillisecondsThrottleThreshold)*time.Millisecond {
		return true, fmt.Sprintf("lag=%fs", time.Duration(lag).Seconds()), base.NoThrottleReasonHint
	}
	checkThrottleControlReplicas := true
	if (this.migrationContext.TestOnReplica || this.migrationContext.MigrateOnReplica) && (atomic.LoadInt64(&this.migrationContext.AllEventsUpToLockProcessedInjectedFlag) > 0) {
		checkThrottleControlReplicas = false
	}
	if checkThrottleControlReplicas {
		lagResult := this.migrationContext.GetControlReplicasLagResult()
		if lagResult.Err != nil {
			return true, fmt.Sprintf("%+v %+v", lagResult.Key, lagResult.Err), base.NoThrottleReasonHint
		}
		if lagResult.Lag > time.Duration(maxLagMillisecondsThrottleThreshold)*time.Millisecond {
			return true, fmt.Sprintf("%+v replica-lag=%fs", lagResult.Key, lagResult.Lag.Seconds()), base.NoThrottleReasonHint
		}
	}
	// Got here? No metrics indicates we need throttling.
	return false, "", base.NoThrottleReasonHint
}

// parseChangelogHeartbeat parses a string timestamp and deduces replication lag
func parseChangelogHeartbeat(heartbeatValue string) (lag time.Duration, err error) {
	heartbeatTime, err := time.Parse(time.RFC3339Nano, heartbeatValue)
	if err != nil {
		return lag, err
	}
	lag = time.Since(heartbeatTime)
	return lag, nil
}

// parseChangelogHeartbeat parses a string timestamp and deduces replication lag
func (this *Throttler) parseChangelogHeartbeat(heartbeatValue string) (err error) {
	if lag, err := parseChangelogHeartbeat(heartbeatValue); err != nil {
		log.ErrorErrorf(err, "parseChangelogHeartbeat failed")
		return err
	} else {
		atomic.StoreInt64(&this.migrationContext.CurrentLag, int64(lag))
		return nil
	}
}

// collectReplicationLag reads the latest changelog heartbeat value
func (this *Throttler) collectReplicationLag(firstThrottlingCollected chan<- bool) {
	collectFunc := func() error {
		if atomic.LoadInt64(&this.migrationContext.CleanupImminentFlag) > 0 {
			return nil
		}

		// 测试服务器上执行replica, 测量方法就不准确了
		// heartbeat injection
		// 直接退化成为利用show slave status??
		//
		if this.migrationContext.TestOnReplica || this.migrationContext.MigrateOnReplica {
			// when running on replica, the heartbeat injection is also done on the replica.
			// This means we will always get a good heartbeat value.
			// When runnign on replica, we should instead check the `SHOW SLAVE STATUS` output.
			if lag, err := mysql.GetReplicationLag(this.inspector.connectionConfig); err != nil {

				log.ErrorErrorf(err, "GetReplicationLag failed")
				return err
			} else {
				atomic.StoreInt64(&this.migrationContext.CurrentLag, int64(lag))
			}
		} else {
			if heartbeatValue, err := this.inspector.readChangelogState("heartbeat"); err != nil {
				log.ErrorErrorf(err, "readChangelogState failed")
				return err
			} else {
				this.parseChangelogHeartbeat(heartbeatValue)
			}
		}
		return nil
	}

	collectFunc()
	// 是否进行throttline呢?
	firstThrottlingCollected <- true

	ticker := time.Tick(time.Duration(this.migrationContext.HeartbeatIntervalMilliseconds) * time.Millisecond)
	for range ticker {
		go collectFunc()
	}
}

// collectControlReplicasLag polls all the control replicas to get maximum lag value
func (this *Throttler) collectControlReplicasLag() {

	replicationLagQuery := fmt.Sprintf(`
		select value from %s.%s where hint = 'heartbeat' and id <= 255
		`,
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetChangelogTableName()),
	)

	readReplicaLag := func(connectionConfig *mysql.ConnectionConfig) (lag time.Duration, err error) {
		dbUri := connectionConfig.GetDBUri("information_schema")

		var heartbeatValue string
		if db, _, err := sqlutils.GetDB(dbUri); err != nil {
			return lag, err
		} else if err = db.QueryRow(replicationLagQuery).Scan(&heartbeatValue); err != nil {
			return lag, err
		}
		lag, err = parseChangelogHeartbeat(heartbeatValue)
		return lag, err
	}

	readControlReplicasLag := func() (result *mysql.ReplicationLagResult) {
		instanceKeyMap := this.migrationContext.GetThrottleControlReplicaKeys()
		if instanceKeyMap.Len() == 0 {
			return result
		}
		lagResults := make(chan *mysql.ReplicationLagResult, instanceKeyMap.Len())
		for replicaKey := range *instanceKeyMap {
			connectionConfig := this.migrationContext.InspectorConnectionConfig.Duplicate()
			connectionConfig.Key = replicaKey

			lagResult := &mysql.ReplicationLagResult{Key: connectionConfig.Key}
			go func() {
				lagResult.Lag, lagResult.Err = readReplicaLag(connectionConfig)
				lagResults <- lagResult
			}()
		}
		for range *instanceKeyMap {
			lagResult := <-lagResults
			if result == nil {
				result = lagResult
			} else if lagResult.Err != nil {
				result = lagResult
			} else if lagResult.Lag.Nanoseconds() > result.Lag.Nanoseconds() {
				result = lagResult
			}
		}
		return result
	}

	checkControlReplicasLag := func() {
		if (this.migrationContext.TestOnReplica || this.migrationContext.MigrateOnReplica) && (atomic.LoadInt64(&this.migrationContext.AllEventsUpToLockProcessedInjectedFlag) > 0) {
			// No need to read lag
			return
		}
		this.migrationContext.SetControlReplicasLagResult(readControlReplicasLag())
	}
	aggressiveTicker := time.Tick(100 * time.Millisecond)
	relaxedFactor := 10
	counter := 0
	shouldReadLagAggressively := false

	for range aggressiveTicker {
		if counter%relaxedFactor == 0 {
			// we only check if we wish to be aggressive once per second. The parameters for being aggressive
			// do not typically change at all throughout the migration, but nonetheless we check them.
			counter = 0
			maxLagMillisecondsThrottleThreshold := atomic.LoadInt64(&this.migrationContext.MaxLagMillisecondsThrottleThreshold)
			shouldReadLagAggressively = (maxLagMillisecondsThrottleThreshold < 1000)
		}
		if counter == 0 || shouldReadLagAggressively {
			// We check replication lag every so often, or if we wish to be aggressive
			checkControlReplicasLag()
		}
		counter++
	}
}

func (this *Throttler) criticalLoadIsMet() (met bool, variableName string, value int64, threshold int64, err error) {
	criticalLoad := this.migrationContext.GetCriticalLoad()
	for variableName, threshold = range criticalLoad {
		value, err = this.applier.ShowStatusVariable(variableName)
		if err != nil {
			return false, variableName, value, threshold, err
		}
		if value >= threshold {
			return true, variableName, value, threshold, nil
		}
	}
	return false, variableName, value, threshold, nil
}

// collectReplicationLag reads the latest changelog heartbeat value
func (this *Throttler) collectThrottleHTTPStatus(firstThrottlingCollected chan<- bool) {
	collectFunc := func() (sleep bool, err error) {
		url := this.migrationContext.GetThrottleHTTP()
		if url == "" {
			return true, nil
		}
		resp, err := http.Head(url)
		if err != nil {
			return false, err
		}
		atomic.StoreInt64(&this.migrationContext.ThrottleHTTPStatusCode, int64(resp.StatusCode))
		return false, nil
	}

	collectFunc()
	firstThrottlingCollected <- true

	ticker := time.Tick(100 * time.Millisecond)
	for range ticker {
		if sleep, _ := collectFunc(); sleep {
			time.Sleep(1 * time.Second)
		}
	}
}

// collectGeneralThrottleMetrics reads the once-per-sec metrics, and stores them onto this.migrationContext
func (this *Throttler) collectGeneralThrottleMetrics() error {

	setThrottle := func(throttle bool, reason string, reasonHint base.ThrottleReasonHint) error {
		this.migrationContext.SetThrottleGeneralCheckResult(base.NewThrottleCheckResult(throttle, reason, reasonHint))
		return nil
	}

	// Regardless of throttle, we take opportunity to check for panic-abort
	if this.migrationContext.PanicFlagFile != "" {
		if base.FileExists(this.migrationContext.PanicFlagFile) {
			this.migrationContext.PanicAbort <- fmt.Errorf("Found panic-file %s. Aborting without cleanup", this.migrationContext.PanicFlagFile)
		}
	}

	criticalLoadMet, variableName, value, threshold, err := this.criticalLoadIsMet()
	if err != nil {
		return setThrottle(true, fmt.Sprintf("%s %s", variableName, err), base.NoThrottleReasonHint)
	}
	if criticalLoadMet && this.migrationContext.CriticalLoadIntervalMilliseconds == 0 {
		this.migrationContext.PanicAbort <- fmt.Errorf("critical-load met: %s=%d, >=%d", variableName, value, threshold)
	}
	if criticalLoadMet && this.migrationContext.CriticalLoadIntervalMilliseconds > 0 {
		log.Errorf("critical-load met once: %s=%d, >=%d. Will check again in %d millis", variableName, value, threshold, this.migrationContext.CriticalLoadIntervalMilliseconds)
		go func() {
			timer := time.NewTimer(time.Millisecond * time.Duration(this.migrationContext.CriticalLoadIntervalMilliseconds))
			<-timer.C
			if criticalLoadMetAgain, variableName, value, threshold, _ := this.criticalLoadIsMet(); criticalLoadMetAgain {
				this.migrationContext.PanicAbort <- fmt.Errorf("critical-load met again after %d millis: %s=%d, >=%d", this.migrationContext.CriticalLoadIntervalMilliseconds, variableName, value, threshold)
			}
		}()
	}

	// Back to throttle considerations

	// User-based throttle
	if atomic.LoadInt64(&this.migrationContext.ThrottleCommandedByUser) > 0 {
		return setThrottle(true, "commanded by user", base.UserCommandThrottleReasonHint)
	}
	if this.migrationContext.ThrottleFlagFile != "" {
		if base.FileExists(this.migrationContext.ThrottleFlagFile) {
			// Throttle file defined and exists!
			return setThrottle(true, "flag-file", base.NoThrottleReasonHint)
		}
	}
	if this.migrationContext.ThrottleAdditionalFlagFile != "" {
		if base.FileExists(this.migrationContext.ThrottleAdditionalFlagFile) {
			// 2nd Throttle file defined and exists!
			return setThrottle(true, "flag-file", base.NoThrottleReasonHint)
		}
	}

	maxLoad := this.migrationContext.GetMaxLoad()
	for variableName, threshold := range maxLoad {
		value, err := this.applier.ShowStatusVariable(variableName)
		if err != nil {
			return setThrottle(true, fmt.Sprintf("%s %s", variableName, err), base.NoThrottleReasonHint)
		}
		if value >= threshold {
			return setThrottle(true, fmt.Sprintf("max-load %s=%d >= %d", variableName, value, threshold), base.NoThrottleReasonHint)
		}
	}
	if this.migrationContext.GetThrottleQuery() != "" {
		// 如果返回结果 > 0, 则要执行throttle, 暂停迁移
		if res, _ := this.applier.ExecuteThrottleQuery(); res > 0 {
			return setThrottle(true, "throttle-query", base.NoThrottleReasonHint)
		}
	}

	return setThrottle(false, "", base.NoThrottleReasonHint)
}

// initiateThrottlerMetrics initiates the various processes that collect measurements
// that may affect throttling. There are several components, all running independently,
// that collect such metrics.
func (this *Throttler) initiateThrottlerCollection(firstThrottlingCollected chan<- bool) {
	// 搜集:RelicationLag
	go this.collectReplicationLag(firstThrottlingCollected)

	go this.collectControlReplicasLag()
	go this.collectThrottleHTTPStatus(firstThrottlingCollected)

	go func() {
		this.collectGeneralThrottleMetrics()
		firstThrottlingCollected <- true

		throttlerMetricsTick := time.Tick(1 * time.Second)
		for range throttlerMetricsTick {
			this.collectGeneralThrottleMetrics()
		}
	}()
}

// initiateThrottlerChecks initiates the throttle ticker and sets the basic behavior of throttling.
func (this *Throttler) initiateThrottlerChecks() error {
	throttlerTick := time.Tick(100 * time.Millisecond)

	throttlerFunction := func() {
		alreadyThrottling, currentReason, _ := this.migrationContext.IsThrottled()
		shouldThrottle, throttleReason, throttleReasonHint := this.shouldThrottle()

		// 记录日志
		if shouldThrottle && !alreadyThrottling {
			// New throttling
			this.applier.WriteAndLogChangelog("throttle", throttleReason)
		} else if shouldThrottle && alreadyThrottling && (currentReason != throttleReason) {
			// Change of reason
			this.applier.WriteAndLogChangelog("throttle", throttleReason)
		} else if alreadyThrottling && !shouldThrottle {
			// End of throttling
			this.applier.WriteAndLogChangelog("throttle", "done throttling")
		}

		// 修改系统状态
		this.migrationContext.SetThrottled(shouldThrottle, throttleReason, throttleReasonHint)
	}

	// 定时检查Metrics, 判断是否应该throttler呢?
	throttlerFunction()
	for range throttlerTick {
		throttlerFunction()
	}

	return nil
}

// throttle sees if throttling needs take place, and if so, continuously sleeps (blocks)
// until throttling reasons are gone
func (this *Throttler) throttle(onThrottled func()) {
	for {
		// IsThrottled() is non-blocking; the throttling decision making takes place asynchronously.
		// Therefore calling IsThrottled() is cheap
		// 处于Throttle状态时，整个工具都在Sleep, 对数据库压力很低
		if shouldThrottle, _, _ := this.migrationContext.IsThrottled(); !shouldThrottle {
			return
		}
		if onThrottled != nil {
			onThrottled()
		}
		time.Sleep(250 * time.Millisecond)
	}
}
