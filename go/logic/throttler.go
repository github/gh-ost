/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"
)

var (
	httpStatusMessages = map[int]string{
		200: "OK",
		404: "Not found",
		417: "Expectation failed",
		429: "Too many requests",
		500: "Internal server error",
		-1:  "Connection error",
	}
	// See https://github.com/github/freno/blob/master/doc/http.md
	httpStatusFrenoMessages = map[int]string{
		200: "OK",
		404: "freno: unknown metric",
		417: "freno: access forbidden",
		429: "freno: threshold exceeded",
		500: "freno: internal error",
		-1:  "freno: connection error",
	}
)

const frenoMagicHint = "freno"

// Throttler collects metrics related to throttling and makes informed decision
// whether throttling should take place.
type Throttler struct {
	appVersion        string
	migrationContext  *base.MigrationContext
	applier           *Applier
	httpClient        *http.Client
	httpClientTimeout time.Duration
	inspector         *Inspector
	finishedMigrating int64
}

func NewThrottler(migrationContext *base.MigrationContext, applier *Applier, inspector *Inspector, appVersion string) *Throttler {
	return &Throttler{
		appVersion:        appVersion,
		migrationContext:  migrationContext,
		applier:           applier,
		httpClient:        &http.Client{},
		httpClientTimeout: time.Duration(migrationContext.ThrottleHTTPTimeoutMillis) * time.Millisecond,
		inspector:         inspector,
		finishedMigrating: 0,
	}
}

func (thlr *Throttler) throttleHttpMessage(statusCode int) string {
	statusCodesMap := httpStatusMessages
	if throttleHttp := thlr.migrationContext.GetThrottleHTTP(); strings.Contains(throttleHttp, frenoMagicHint) {
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
func (thlr *Throttler) shouldThrottle() (result bool, reason string, reasonHint base.ThrottleReasonHint) {
	if hibernateUntil := atomic.LoadInt64(&thlr.migrationContext.HibernateUntil); hibernateUntil > 0 {
		hibernateUntilTime := time.Unix(0, hibernateUntil)
		return true, fmt.Sprintf("critical-load-hibernate until %+v", hibernateUntilTime), base.NoThrottleReasonHint
	}
	generalCheckResult := thlr.migrationContext.GetThrottleGeneralCheckResult()
	if generalCheckResult.ShouldThrottle {
		return generalCheckResult.ShouldThrottle, generalCheckResult.Reason, generalCheckResult.ReasonHint
	}
	// HTTP throttle
	statusCode := atomic.LoadInt64(&thlr.migrationContext.ThrottleHTTPStatusCode)
	if statusCode != 0 && statusCode != http.StatusOK {
		return true, thlr.throttleHttpMessage(int(statusCode)), base.NoThrottleReasonHint
	}

	// Replication lag throttle
	maxLagMillisecondsThrottleThreshold := atomic.LoadInt64(&thlr.migrationContext.MaxLagMillisecondsThrottleThreshold)
	lag := atomic.LoadInt64(&thlr.migrationContext.CurrentLag)
	if time.Duration(lag) > time.Duration(maxLagMillisecondsThrottleThreshold)*time.Millisecond {
		return true, fmt.Sprintf("lag=%fs", time.Duration(lag).Seconds()), base.NoThrottleReasonHint
	}
	lockInjected := atomic.LoadInt64(&thlr.migrationContext.AllEventsUpToLockProcessedInjectedFlag) > 0
	checkThrottleControlReplicas := !lockInjected || (!thlr.migrationContext.TestOnReplica && !thlr.migrationContext.MigrateOnReplica)
	if checkThrottleControlReplicas {
		lagResult := thlr.migrationContext.GetControlReplicasLagResult()
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
func (thlr *Throttler) parseChangelogHeartbeat(heartbeatValue string) (err error) {
	if lag, err := parseChangelogHeartbeat(heartbeatValue); err != nil {
		return thlr.migrationContext.Log.Errore(err)
	} else {
		atomic.StoreInt64(&thlr.migrationContext.CurrentLag, int64(lag))
		return nil
	}
}

// collectReplicationLag reads the latest changelog heartbeat value
func (thlr *Throttler) collectReplicationLag(firstThrottlingCollected chan<- bool) {
	collectFunc := func() error {
		if atomic.LoadInt64(&thlr.migrationContext.CleanupImminentFlag) > 0 {
			return nil
		}
		if atomic.LoadInt64(&thlr.migrationContext.HibernateUntil) > 0 {
			return nil
		}

		if thlr.migrationContext.TestOnReplica || thlr.migrationContext.MigrateOnReplica {
			// when running on replica, the heartbeat injection is also done on the replica.
			// This means we will always get a good heartbeat value.
			// When running on replica, we should instead check the `SHOW SLAVE STATUS` output.
			if lag, err := mysql.GetReplicationLagFromSlaveStatus(thlr.inspector.dbVersion, thlr.inspector.informationSchemaDb); err != nil {
				return thlr.migrationContext.Log.Errore(err)
			} else {
				atomic.StoreInt64(&thlr.migrationContext.CurrentLag, int64(lag))
			}
		} else {
			if heartbeatValue, err := thlr.inspector.readChangelogState("heartbeat"); err != nil {
				return thlr.migrationContext.Log.Errore(err)
			} else {
				thlr.parseChangelogHeartbeat(heartbeatValue)
			}
		}
		return nil
	}

	collectFunc()
	firstThrottlingCollected <- true

	ticker := time.NewTicker(time.Duration(thlr.migrationContext.HeartbeatIntervalMilliseconds) * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		if atomic.LoadInt64(&thlr.finishedMigrating) > 0 {
			return
		}
		go collectFunc()
	}
}

// collectControlReplicasLag polls all the control replicas to get maximum lag value
func (thlr *Throttler) collectControlReplicasLag() {
	if atomic.LoadInt64(&thlr.migrationContext.HibernateUntil) > 0 {
		return
	}

	replicationLagQuery := fmt.Sprintf(`
		select value from %s.%s where hint = 'heartbeat' and id <= 255
		`,
		sql.EscapeName(thlr.migrationContext.DatabaseName),
		sql.EscapeName(thlr.migrationContext.GetChangelogTableName()),
	)

	readReplicaLag := func(connectionConfig *mysql.ConnectionConfig) (lag time.Duration, err error) {
		dbUri := connectionConfig.GetDBUri("information_schema")

		var heartbeatValue string
		db, _, err := mysql.GetDB(thlr.migrationContext.Uuid, dbUri)
		if err != nil {
			return lag, err
		}

		if err := db.QueryRow(replicationLagQuery).Scan(&heartbeatValue); err != nil {
			return lag, err
		}

		lag, err = parseChangelogHeartbeat(heartbeatValue)
		return lag, err
	}

	readControlReplicasLag := func() (result *mysql.ReplicationLagResult) {
		instanceKeyMap := thlr.migrationContext.GetThrottleControlReplicaKeys()
		if instanceKeyMap.Len() == 0 {
			return result
		}
		lagResults := make(chan *mysql.ReplicationLagResult, instanceKeyMap.Len())
		for replicaKey := range *instanceKeyMap {
			connectionConfig := thlr.migrationContext.InspectorConnectionConfig.DuplicateCredentials(replicaKey)
			if err := connectionConfig.RegisterTLSConfig(); err != nil {
				return &mysql.ReplicationLagResult{Err: err}
			}

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
		if (thlr.migrationContext.TestOnReplica || thlr.migrationContext.MigrateOnReplica) && (atomic.LoadInt64(&thlr.migrationContext.AllEventsUpToLockProcessedInjectedFlag) > 0) {
			// No need to read lag
			return
		}
		thlr.migrationContext.SetControlReplicasLagResult(readControlReplicasLag())
	}

	relaxedFactor := 10
	counter := 0
	shouldReadLagAggressively := false

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		if atomic.LoadInt64(&thlr.finishedMigrating) > 0 {
			return
		}
		if counter%relaxedFactor == 0 {
			// we only check if we wish to be aggressive once per second. The parameters for being aggressive
			// do not typically change at all throughout the migration, but nonetheless we check them.
			counter = 0
			maxLagMillisecondsThrottleThreshold := atomic.LoadInt64(&thlr.migrationContext.MaxLagMillisecondsThrottleThreshold)
			shouldReadLagAggressively = (maxLagMillisecondsThrottleThreshold < 1000)
		}
		if counter == 0 || shouldReadLagAggressively {
			// We check replication lag every so often, or if we wish to be aggressive
			checkControlReplicasLag()
		}
		counter++
	}
}

func (thlr *Throttler) criticalLoadIsMet() (met bool, variableName string, value int64, threshold int64, err error) {
	criticalLoad := thlr.migrationContext.GetCriticalLoad()
	for variableName, threshold = range criticalLoad {
		value, err = thlr.applier.ShowStatusVariable(variableName)
		if err != nil {
			return false, variableName, value, threshold, err
		}
		if value >= threshold {
			return true, variableName, value, threshold, nil
		}
	}
	return false, variableName, value, threshold, nil
}

// collectThrottleHTTPStatus reads the latest changelog heartbeat value
func (thlr *Throttler) collectThrottleHTTPStatus(firstThrottlingCollected chan<- bool) {
	collectFunc := func() (sleep bool, err error) {
		if atomic.LoadInt64(&thlr.migrationContext.HibernateUntil) > 0 {
			return true, nil
		}
		url := thlr.migrationContext.GetThrottleHTTP()
		if url == "" {
			return true, nil
		}

		ctx, cancel := context.WithTimeout(context.Background(), thlr.httpClientTimeout)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
		if err != nil {
			return false, err
		}
		req.Header.Set("User-Agent", fmt.Sprintf("gh-ost/%s", thlr.appVersion))

		resp, err := thlr.httpClient.Do(req)
		if err != nil {
			return false, err
		}
		defer resp.Body.Close()

		atomic.StoreInt64(&thlr.migrationContext.ThrottleHTTPStatusCode, int64(resp.StatusCode))
		return false, nil
	}

	_, err := collectFunc()
	if err != nil {
		// If not told to ignore errors, we'll throttle on HTTP connection issues
		if !thlr.migrationContext.IgnoreHTTPErrors {
			atomic.StoreInt64(&thlr.migrationContext.ThrottleHTTPStatusCode, int64(-1))
		}
	}

	firstThrottlingCollected <- true

	collectInterval := time.Duration(thlr.migrationContext.ThrottleHTTPIntervalMillis) * time.Millisecond
	ticker := time.NewTicker(collectInterval)
	defer ticker.Stop()
	for range ticker.C {
		if atomic.LoadInt64(&thlr.finishedMigrating) > 0 {
			return
		}

		sleep, err := collectFunc()
		if err != nil {
			// If not told to ignore errors, we'll throttle on HTTP connection issues
			if !thlr.migrationContext.IgnoreHTTPErrors {
				atomic.StoreInt64(&thlr.migrationContext.ThrottleHTTPStatusCode, int64(-1))
			}
		}

		if sleep {
			time.Sleep(1 * time.Second)
		}
	}
}

// collectGeneralThrottleMetrics reads the once-per-sec metrics, and stores them onto migrationContext
func (thlr *Throttler) collectGeneralThrottleMetrics() error {
	if atomic.LoadInt64(&thlr.migrationContext.HibernateUntil) > 0 {
		return nil
	}

	setThrottle := func(throttle bool, reason string, reasonHint base.ThrottleReasonHint) error {
		thlr.migrationContext.SetThrottleGeneralCheckResult(base.NewThrottleCheckResult(throttle, reason, reasonHint))
		return nil
	}

	// Regardless of throttle, we take opportunity to check for panic-abort
	if thlr.migrationContext.PanicFlagFile != "" {
		if base.FileExists(thlr.migrationContext.PanicFlagFile) {
			// Use helper to prevent deadlock if listenOnPanicAbort already exited
			_ = base.SendWithContext(thlr.migrationContext.GetContext(), thlr.migrationContext.PanicAbort, fmt.Errorf("found panic-file %s. Aborting without cleanup", thlr.migrationContext.PanicFlagFile))
			return nil
		}
	}

	criticalLoadMet, variableName, value, threshold, err := thlr.criticalLoadIsMet()
	if err != nil {
		return setThrottle(true, fmt.Sprintf("%s %s", variableName, err), base.NoThrottleReasonHint)
	}

	if criticalLoadMet && thlr.migrationContext.CriticalLoadHibernateSeconds > 0 {
		hibernateDuration := time.Duration(thlr.migrationContext.CriticalLoadHibernateSeconds) * time.Second
		hibernateUntilTime := time.Now().Add(hibernateDuration)
		atomic.StoreInt64(&thlr.migrationContext.HibernateUntil, hibernateUntilTime.UnixNano())
		thlr.migrationContext.Log.Errorf("critical-load met: %s=%d, >=%d. Will hibernate for the duration of %+v, until %+v", variableName, value, threshold, hibernateDuration, hibernateUntilTime)
		go func() {
			time.Sleep(hibernateDuration)
			thlr.migrationContext.SetThrottleGeneralCheckResult(base.NewThrottleCheckResult(true, "leaving hibernation", base.LeavingHibernationThrottleReasonHint))
			atomic.StoreInt64(&thlr.migrationContext.HibernateUntil, 0)
		}()
		return nil
	}

	if criticalLoadMet && thlr.migrationContext.CriticalLoadIntervalMilliseconds == 0 {
		// Use helper to prevent deadlock if listenOnPanicAbort already exited
		_ = base.SendWithContext(thlr.migrationContext.GetContext(), thlr.migrationContext.PanicAbort, fmt.Errorf("critical-load met: %s=%d, >=%d", variableName, value, threshold))
		return nil
	}
	if criticalLoadMet && thlr.migrationContext.CriticalLoadIntervalMilliseconds > 0 {
		thlr.migrationContext.Log.Errorf("critical-load met once: %s=%d, >=%d. Will check again in %d millis", variableName, value, threshold, thlr.migrationContext.CriticalLoadIntervalMilliseconds)
		go func() {
			timer := time.NewTimer(time.Millisecond * time.Duration(thlr.migrationContext.CriticalLoadIntervalMilliseconds))
			<-timer.C
			if criticalLoadMetAgain, variableName, value, threshold, _ := thlr.criticalLoadIsMet(); criticalLoadMetAgain {
				// Use helper to prevent deadlock if listenOnPanicAbort already exited
				_ = base.SendWithContext(thlr.migrationContext.GetContext(), thlr.migrationContext.PanicAbort, fmt.Errorf("critical-load met again after %d millis: %s=%d, >=%d", thlr.migrationContext.CriticalLoadIntervalMilliseconds, variableName, value, threshold))
			}
		}()
	}

	// Back to throttle considerations

	// User-based throttle
	if atomic.LoadInt64(&thlr.migrationContext.ThrottleCommandedByUser) > 0 {
		return setThrottle(true, "commanded by user", base.UserCommandThrottleReasonHint)
	}
	if thlr.migrationContext.ThrottleFlagFile != "" {
		if base.FileExists(thlr.migrationContext.ThrottleFlagFile) {
			// Throttle file defined and exists!
			return setThrottle(true, "flag-file", base.NoThrottleReasonHint)
		}
	}
	if thlr.migrationContext.ThrottleAdditionalFlagFile != "" {
		if base.FileExists(thlr.migrationContext.ThrottleAdditionalFlagFile) {
			// 2nd Throttle file defined and exists!
			return setThrottle(true, "flag-file", base.NoThrottleReasonHint)
		}
	}

	maxLoad := thlr.migrationContext.GetMaxLoad()
	for variableName, threshold := range maxLoad {
		value, err := thlr.applier.ShowStatusVariable(variableName)
		if err != nil {
			return setThrottle(true, fmt.Sprintf("%s %s", variableName, err), base.NoThrottleReasonHint)
		}
		if value >= threshold {
			return setThrottle(true, fmt.Sprintf("max-load %s=%d >= %d", variableName, value, threshold), base.NoThrottleReasonHint)
		}
	}
	if thlr.migrationContext.GetThrottleQuery() != "" {
		if res, _ := thlr.applier.ExecuteThrottleQuery(); res > 0 {
			return setThrottle(true, "throttle-query", base.NoThrottleReasonHint)
		}
	}

	return setThrottle(false, "", base.NoThrottleReasonHint)
}

// initiateThrottlerCollection initiates the various processes that collect measurements
// that may affect throttling. There are several components, all running independently,
// that collect such metrics.
func (thlr *Throttler) initiateThrottlerCollection(firstThrottlingCollected chan<- bool) {
	go thlr.collectReplicationLag(firstThrottlingCollected)
	go thlr.collectControlReplicasLag()
	go thlr.collectThrottleHTTPStatus(firstThrottlingCollected)

	go func() {
		thlr.collectGeneralThrottleMetrics()
		firstThrottlingCollected <- true

		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if atomic.LoadInt64(&thlr.finishedMigrating) > 0 {
				return
			}

			thlr.collectGeneralThrottleMetrics()
		}
	}()
}

// initiateThrottlerChecks initiates the throttle ticker and sets the basic behavior of throttling.
func (thlr *Throttler) initiateThrottlerChecks() {
	throttlerFunction := func() {
		alreadyThrottling, currentReason, _ := thlr.migrationContext.IsThrottled()
		shouldThrottle, throttleReason, throttleReasonHint := thlr.shouldThrottle()
		if shouldThrottle && !alreadyThrottling {
			// New throttling
			thlr.applier.WriteAndLogChangelog("throttle", throttleReason)
		} else if shouldThrottle && alreadyThrottling && (currentReason != throttleReason) {
			// Change of reason
			thlr.applier.WriteAndLogChangelog("throttle", throttleReason)
		} else if alreadyThrottling && !shouldThrottle {
			// End of throttling
			thlr.applier.WriteAndLogChangelog("throttle", "done throttling")
		}
		thlr.migrationContext.SetThrottled(shouldThrottle, throttleReason, throttleReasonHint)
	}
	throttlerFunction()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		// Check for context cancellation each iteration
		ctx := thlr.migrationContext.GetContext()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Process throttle check
		}

		if atomic.LoadInt64(&thlr.finishedMigrating) > 0 {
			return
		}
		throttlerFunction()
	}
}

// throttle sees if throttling needs take place, and if so, continuously sleeps (blocks)
// until throttling reasons are gone
func (thlr *Throttler) throttle(onThrottled func()) {
	timer := time.NewTimer(250 * time.Millisecond)
	defer timer.Stop()
	for {
		// IsThrottled() is non-blocking; the throttling decision making takes place asynchronously.
		// Therefore calling IsThrottled() is cheap
		if shouldThrottle, _, _ := thlr.migrationContext.IsThrottled(); !shouldThrottle {
			return
		}
		if onThrottled != nil {
			onThrottled()
		}
		timer.Reset(250 * time.Millisecond)
		select {
		case <-thlr.migrationContext.GetContext().Done():
			return
		case <-timer.C:
		}
	}
}

func (thlr *Throttler) Teardown() {
	thlr.migrationContext.Log.Debugf("Tearing down...")
	atomic.StoreInt64(&thlr.finishedMigrating, 1)
}
