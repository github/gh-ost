#!/bin/bash
# Parse gh-ost MTS test log for timeline, throughput, and stall detection.
# Usage: ./script/mts-analyze-log.sh [/tmp/gh-ost-mts-sysbench.log]

set -euo pipefail

LOG="${1:-/tmp/gh-ost-mts-sysbench.log}"
if [ ! -f "$LOG" ]; then
	echo "Log not found: $LOG"
	exit 1
fi

echo "=== gh-ost MTS log analysis ==="
echo "file: $LOG ($(wc -l <"$LOG") lines)"
echo ""

echo "--- Milestones ---"
grep -E 'Starting MTS|Row copy complete|Done migrating|cut-over|Cut-over|AllEventsUpToLock|ERROR|aborted|falling back' "$LOG" || true
echo ""

echo "--- Applied progression (unique values) ---"
grep -oE 'Applied: [0-9]+' "$LOG" | awk '{print $2}' | sort -n | uniq | tr '\n' ' '
echo ""
echo ""

echo "--- Backlog distribution (top) ---"
grep -oE 'Backlog: [0-9]+' "$LOG" | awk '{print $2}' | sort -n | uniq -c | sort -rn | head -10
echo ""

echo "--- Timing ---"
start=$(grep -m1 'Migration started at' "$LOG" | sed 's/# //')
rowcopy=$(grep -m1 'Row copy complete' "$LOG" || true)
done=$(grep -m1 'Done migrating' "$LOG" || true)
echo "start:    ${start:-n/a}"
echo "row copy: ${rowcopy:-NOT REACHED}"
echo "done:     ${done:-NOT REACHED}"
echo ""

if grep -q 'Row copy complete' "$LOG"; then
	rc_line=$(grep -m1 'Time: [0-9]+s\(total\)' "$LOG" | head -1)
	first_applied=$(grep -m1 'Applied: [1-9]' "$LOG" || true)
	last_applied=$(grep 'Applied:' "$LOG" | tail -1)
	echo "first status after copy: $(grep -m1 'Row copy complete' -A1 "$LOG" | grep Copy || true)"
	echo "last status: $last_applied"
fi

max_backlog=$(grep -oE 'Backlog: [0-9]+' "$LOG" | awk '{print $2}' | sort -n | tail -1)
final_applied=$(grep -oE 'Applied: [0-9]+' "$LOG" | tail -1 | awk '{print $2}')
echo ""
echo "max backlog: ${max_backlog:-0}"
echo "final applied counter: ${final_applied:-0}"
echo ""

if ! grep -q 'Done migrating' "$LOG"; then
	echo "WARN: migration did not finish — consistency check invalid until cut-over completes."
fi
