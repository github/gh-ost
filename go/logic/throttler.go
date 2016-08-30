/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/mysql"
	"github.com/outbrain/golib/log"
)

// Throttler collects metrics related to throttling and makes informed decisison
// whether throttling should take place.
type Throttler struct {
	migrationContext *base.MigrationContext
	applier          *Applier
	inspector        *Inspector
	panicAbort       chan error
}

func NewThrottler(applier *Applier, inspector *Inspector, panicAbort chan error) *Throttler {
	return &Throttler{
		migrationContext: base.GetMigrationContext(),
		applier:          applier,
		inspector:        inspector,
		panicAbort:       panicAbort,
	}
}

// shouldThrottle performs checks to see whether we should currently be throttling.
// It merely observes the metrics collected by other components, it does not issue
// its own metric collection.
func (this *Throttler) shouldThrottle() (result bool, reason string) {
	generalCheckResult := this.migrationContext.GetThrottleGeneralCheckResult()
	if generalCheckResult.ShouldThrottle {
		return generalCheckResult.ShouldThrottle, generalCheckResult.Reason
	}
	// Replication lag throttle
	maxLagMillisecondsThrottleThreshold := atomic.LoadInt64(&this.migrationContext.MaxLagMillisecondsThrottleThreshold)
	lag := atomic.LoadInt64(&this.migrationContext.CurrentLag)
	if time.Duration(lag) > time.Duration(maxLagMillisecondsThrottleThreshold)*time.Millisecond {
		return true, fmt.Sprintf("lag=%fs", time.Duration(lag).Seconds())
	}
	checkThrottleControlReplicas := true
	if (this.migrationContext.TestOnReplica || this.migrationContext.MigrateOnReplica) && (atomic.LoadInt64(&this.migrationContext.AllEventsUpToLockProcessedInjectedFlag) > 0) {
		checkThrottleControlReplicas = false
	}
	if checkThrottleControlReplicas {
		lagResult := this.migrationContext.GetControlReplicasLagResult()
		if lagResult.Err != nil {
			return true, fmt.Sprintf("%+v %+v", lagResult.Key, lagResult.Err)
		}
		if lagResult.Lag > time.Duration(maxLagMillisecondsThrottleThreshold)*time.Millisecond {
			return true, fmt.Sprintf("%+v replica-lag=%fs", lagResult.Key, lagResult.Lag.Seconds())
		}
	}
	// Got here? No metrics indicates we need throttling.
	return false, ""
}

// parseChangelogHeartbeat is called when a heartbeat event is intercepted
func (this *Throttler) parseChangelogHeartbeat(heartbeatValue string) (err error) {
	heartbeatTime, err := time.Parse(time.RFC3339Nano, heartbeatValue)
	if err != nil {
		return log.Errore(err)
	}
	lag := time.Since(heartbeatTime)
	atomic.StoreInt64(&this.migrationContext.CurrentLag, int64(lag))
	return nil
}

// collectHeartbeat reads the latest changelog heartbeat value
func (this *Throttler) collectHeartbeat() {
	ticker := time.Tick(time.Duration(this.migrationContext.HeartbeatIntervalMilliseconds) * time.Millisecond)
	for range ticker {
		go func() error {
			if atomic.LoadInt64(&this.migrationContext.CleanupImminentFlag) > 0 {
				return nil
			}
			changelogState, err := this.inspector.readChangelogState()
			if err != nil {
				return log.Errore(err)
			}
			if heartbeatValue, ok := changelogState["heartbeat"]; ok {
				this.parseChangelogHeartbeat(heartbeatValue)
			}
			return nil
		}()
	}
}

// collectControlReplicasLag polls all the control replicas to get maximum lag value
func (this *Throttler) collectControlReplicasLag() {
	readControlReplicasLag := func(replicationLagQuery string) error {
		if (this.migrationContext.TestOnReplica || this.migrationContext.MigrateOnReplica) && (atomic.LoadInt64(&this.migrationContext.AllEventsUpToLockProcessedInjectedFlag) > 0) {
			return nil
		}
		lagResult := mysql.GetMaxReplicationLag(
			this.migrationContext.InspectorConnectionConfig,
			this.migrationContext.GetThrottleControlReplicaKeys(),
			replicationLagQuery,
		)
		this.migrationContext.SetControlReplicasLagResult(lagResult)
		return nil
	}
	aggressiveTicker := time.Tick(100 * time.Millisecond)
	relaxedFactor := 10
	counter := 0
	shouldReadLagAggressively := false
	replicationLagQuery := ""

	for range aggressiveTicker {
		if counter%relaxedFactor == 0 {
			// we only check if we wish to be aggressive once per second. The parameters for being aggressive
			// do not typically change at all throughout the migration, but nonetheless we check them.
			counter = 0
			maxLagMillisecondsThrottleThreshold := atomic.LoadInt64(&this.migrationContext.MaxLagMillisecondsThrottleThreshold)
			replicationLagQuery = this.migrationContext.GetReplicationLagQuery()
			shouldReadLagAggressively = (replicationLagQuery != "" && maxLagMillisecondsThrottleThreshold < 1000)
		}
		if counter == 0 || shouldReadLagAggressively {
			// We check replication lag every so often, or if we wish to be aggressive
			readControlReplicasLag(replicationLagQuery)
		}
		counter++
	}
}

// collectGeneralThrottleMetrics reads the once-per-sec metrics, and stores them onto this.migrationContext
func (this *Throttler) collectGeneralThrottleMetrics() error {

	setThrottle := func(throttle bool, reason string) error {
		this.migrationContext.SetThrottleGeneralCheckResult(base.NewThrottleCheckResult(throttle, reason))
		return nil
	}

	// Regardless of throttle, we take opportunity to check for panic-abort
	if this.migrationContext.PanicFlagFile != "" {
		if base.FileExists(this.migrationContext.PanicFlagFile) {
			this.panicAbort <- fmt.Errorf("Found panic-file %s. Aborting without cleanup", this.migrationContext.PanicFlagFile)
		}
	}
	criticalLoad := this.migrationContext.GetCriticalLoad()
	for variableName, threshold := range criticalLoad {
		value, err := this.applier.ShowStatusVariable(variableName)
		if err != nil {
			return setThrottle(true, fmt.Sprintf("%s %s", variableName, err))
		}
		if value >= threshold {
			this.panicAbort <- fmt.Errorf("critical-load met: %s=%d, >=%d", variableName, value, threshold)
		}
	}

	// Back to throttle considerations

	// User-based throttle
	if atomic.LoadInt64(&this.migrationContext.ThrottleCommandedByUser) > 0 {
		return setThrottle(true, "commanded by user")
	}
	if this.migrationContext.ThrottleFlagFile != "" {
		if base.FileExists(this.migrationContext.ThrottleFlagFile) {
			// Throttle file defined and exists!
			return setThrottle(true, "flag-file")
		}
	}
	if this.migrationContext.ThrottleAdditionalFlagFile != "" {
		if base.FileExists(this.migrationContext.ThrottleAdditionalFlagFile) {
			// 2nd Throttle file defined and exists!
			return setThrottle(true, "flag-file")
		}
	}

	maxLoad := this.migrationContext.GetMaxLoad()
	for variableName, threshold := range maxLoad {
		value, err := this.applier.ShowStatusVariable(variableName)
		if err != nil {
			return setThrottle(true, fmt.Sprintf("%s %s", variableName, err))
		}
		if value >= threshold {
			return setThrottle(true, fmt.Sprintf("max-load %s=%d >= %d", variableName, value, threshold))
		}
	}
	if this.migrationContext.GetThrottleQuery() != "" {
		if res, _ := this.applier.ExecuteThrottleQuery(); res > 0 {
			return setThrottle(true, "throttle-query")
		}
	}

	return setThrottle(false, "")
}

// initiateThrottlerMetrics initiates the various processes that collect measurements
// that may affect throttling. There are several components, all running independently,
// that collect such metrics.
func (this *Throttler) initiateThrottlerCollection(firstThrottlingCollected chan<- bool) {
	go this.collectHeartbeat()
	go this.collectControlReplicasLag()

	go func() {
		throttlerMetricsTick := time.Tick(1 * time.Second)
		this.collectGeneralThrottleMetrics()
		firstThrottlingCollected <- true
		for range throttlerMetricsTick {
			this.collectGeneralThrottleMetrics()
		}
	}()
}

// initiateThrottlerChecks initiates the throttle ticker and sets the basic behavior of throttling.
func (this *Throttler) initiateThrottlerChecks() error {
	throttlerTick := time.Tick(100 * time.Millisecond)

	throttlerFunction := func() {
		alreadyThrottling, currentReason := this.migrationContext.IsThrottled()
		shouldThrottle, throttleReason := this.shouldThrottle()
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
		this.migrationContext.SetThrottled(shouldThrottle, throttleReason)
	}
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
		if shouldThrottle, _ := this.migrationContext.IsThrottled(); !shouldThrottle {
			return
		}
		if onThrottled != nil {
			onThrottled()
		}
		time.Sleep(250 * time.Millisecond)
	}
}
