/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"
	"github.com/outbrain/golib/log"
)

var (
	httpStatusMessages map[int]string = map[int]string{
		200: "OK",
		404: "Not found",
		417: "Expectation failed",
		429: "Too many requests",
		500: "Internal server error",
	}
	// See https://github.com/github/freno/blob/master/doc/http.md
	httpStatusFrenoMessages map[int]string = map[int]string{
		200: "OK",
		404: "freno: unknown metric",
		417: "freno: access forbidden",
		429: "freno: threshold exceeded",
		500: "freno: internal error",
	}
)

const frenoMagicHint = "freno"

// Throttler collects metrics related to throttling and makes informed decision
// whether throttling should take place.
type Throttler struct {
	migrationContext  *base.MigrationContext
	applier           *Applier
	inspector         *Inspector
	finishedMigrating int64
}

func NewThrottler(migrationContext *base.MigrationContext, applier *Applier, inspector *Inspector) *Throttler {
	return &Throttler{
		migrationContext:  migrationContext,
		applier:           applier,
		inspector:         inspector,
		finishedMigrating: 0,
	}
}

func (this *Throttler) throttleHttpMessage(statusCode int) string {
	statusCodesMap := httpStatusMessages
	if throttleHttp := this.migrationContext.GetThrottleHTTP(); strings.Contains(throttleHttp, frenoMagicHint) {
		statusCodesMap = httpStatusFrenoMessages
	}
	if message, ok := statusCodesMap[statusCode]; ok {
		return fmt.Sprintf("%s (http=%d)", message, statusCode)
	}
	return fmt.Sprintf("http=%d", statusCode)
}

// shouldThrottle performs checks to see whether we should currently be throttling.
// It merely observes the metrics collected by other components, it does not issue
// its own metric collection.
func (this *Throttler) shouldThrottle() (result bool, reason string, reasonHint base.ThrottleReasonHint) {
	if hibernateUntil := atomic.LoadInt64(&this.migrationContext.HibernateUntil); hibernateUntil > 0 {
		hibernateUntilTime := time.Unix(0, hibernateUntil)
		return true, fmt.Sprintf("critical-load-hibernate until %+v", hibernateUntilTime), base.NoThrottleReasonHint
	}
	generalCheckResult := this.migrationContext.GetThrottleGeneralCheckResult()
	if generalCheckResult.ShouldThrottle {
		return generalCheckResult.ShouldThrottle, generalCheckResult.Reason, generalCheckResult.ReasonHint
	}
	// HTTP throttle
	statusCode := atomic.LoadInt64(&this.migrationContext.ThrottleHTTPStatusCode)
	if statusCode != 0 && statusCode != http.StatusOK {
		return true, this.throttleHttpMessage(int(statusCode)), base.NoThrottleReasonHint
	}
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
		return log.Errore(err)
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
		if atomic.LoadInt64(&this.migrationContext.HibernateUntil) > 0 {
			return nil
		}

		if this.migrationContext.TestOnReplica || this.migrationContext.MigrateOnReplica {
			// when running on replica, the heartbeat injection is also done on the replica.
			// This means we will always get a good heartbeat value.
			// When running on replica, we should instead check the `SHOW SLAVE STATUS` output.
			if lag, err := mysql.GetReplicationLagFromSlaveStatus(this.inspector.informationSchemaDb); err != nil {
				return log.Errore(err)
			} else {
				atomic.StoreInt64(&this.migrationContext.CurrentLag, int64(lag))
			}
		} else {
			if heartbeatValue, err := this.inspector.readChangelogState("heartbeat"); err != nil {
				return log.Errore(err)
			} else {
				this.parseChangelogHeartbeat(heartbeatValue)
			}
		}
		return nil
	}

	collectFunc()
	firstThrottlingCollected <- true

	ticker := time.Tick(time.Duration(this.migrationContext.HeartbeatIntervalMilliseconds) * time.Millisecond)
	for range ticker {
		if atomic.LoadInt64(&this.finishedMigrating) > 0 {
			return
		}
		go collectFunc()
	}
}

// collectControlReplicasLag polls all the control replicas to get maximum lag value
func (this *Throttler) collectControlReplicasLag() {

	if atomic.LoadInt64(&this.migrationContext.HibernateUntil) > 0 {
		return
	}

	replicationLagQuery := fmt.Sprintf(`
		select value from %s.%s where hint = 'heartbeat' and id <= 255
		`,
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetChangelogTableName()),
	)

	readReplicaLag := func(connectionConfig *mysql.ConnectionConfig) (lag time.Duration, err error) {
		dbUri := connectionConfig.GetDBUri("information_schema")

		var heartbeatValue string
		if db, _, err := mysql.GetDB(this.migrationContext.Uuid, dbUri); err != nil {
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
		if atomic.LoadInt64(&this.finishedMigrating) > 0 {
			return
		}
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
		if atomic.LoadInt64(&this.migrationContext.HibernateUntil) > 0 {
			return true, nil
		}
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
		if atomic.LoadInt64(&this.finishedMigrating) > 0 {
			return
		}

		if sleep, _ := collectFunc(); sleep {
			time.Sleep(1 * time.Second)
		}
	}
}

// collectGeneralThrottleMetrics reads the once-per-sec metrics, and stores them onto this.migrationContext
func (this *Throttler) collectGeneralThrottleMetrics() error {
	if atomic.LoadInt64(&this.migrationContext.HibernateUntil) > 0 {
		return nil
	}

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

	if criticalLoadMet && this.migrationContext.CriticalLoadHibernateSeconds > 0 {
		hibernateDuration := time.Duration(this.migrationContext.CriticalLoadHibernateSeconds) * time.Second
		hibernateUntilTime := time.Now().Add(hibernateDuration)
		atomic.StoreInt64(&this.migrationContext.HibernateUntil, hibernateUntilTime.UnixNano())
		log.Errorf("critical-load met: %s=%d, >=%d. Will hibernate for the duration of %+v, until %+v", variableName, value, threshold, hibernateDuration, hibernateUntilTime)
		go func() {
			time.Sleep(hibernateDuration)
			this.migrationContext.SetThrottleGeneralCheckResult(base.NewThrottleCheckResult(true, "leaving hibernation", base.LeavingHibernationThrottleReasonHint))
			atomic.StoreInt64(&this.migrationContext.HibernateUntil, 0)
		}()
		return nil
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
	go this.collectReplicationLag(firstThrottlingCollected)
	go this.collectControlReplicasLag()
	go this.collectThrottleHTTPStatus(firstThrottlingCollected)

	go func() {
		this.collectGeneralThrottleMetrics()
		firstThrottlingCollected <- true

		throttlerMetricsTick := time.Tick(1 * time.Second)
		for range throttlerMetricsTick {
			if atomic.LoadInt64(&this.finishedMigrating) > 0 {
				return
			}

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
		this.migrationContext.SetThrottled(shouldThrottle, throttleReason, throttleReasonHint)
	}
	throttlerFunction()
	for range throttlerTick {
		if atomic.LoadInt64(&this.finishedMigrating) > 0 {
			return nil
		}
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
		if shouldThrottle, _, _ := this.migrationContext.IsThrottled(); !shouldThrottle {
			return
		}
		if onThrottled != nil {
			onThrottled()
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func (this *Throttler) Teardown() {
	log.Debugf("Tearing down...")
	atomic.StoreInt64(&this.finishedMigrating, 1)
}
