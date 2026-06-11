#!/bin/bash
# Wrapper: native macOS MySQL (default). Set USE_DOCKER=1 for docker-compose flow.

set -euo pipefail

if [ "${USE_DOCKER:-0}" = "1" ]; then
	ROOT="$(cd "$(dirname "$0")/.." && pwd)"
	cd "$ROOT"
	GHOST_BIN="${GHOST_BIN:-/tmp/gh-ost-test}"
	COMPOSE_FILE="${COMPOSE_FILE:-localtests/docker-compose.yml}"

	if [ ! -x "$GHOST_BIN" ]; then
		go build -o "$GHOST_BIN" ./go/cmd/gh-ost
	fi
	if ! docker ps --format '{{.Names}}' | grep -q '^mysql-primary$'; then
		docker compose -f "$COMPOSE_FILE" up -d
	fi
	export PATH="$ROOT/script:$PATH"
	go test ./go/logic/ -run 'TestCommitBarrier|TestMTSSchedule|TestCollectTransaction|TestNotifyLogical|TestEnqueueApply|TestMigrator_Num' -count=1
	localtests/test.sh -b "$GHOST_BIN" -d -g mts-sysbench
	exit 0
fi

exec "$(dirname "$0")/mts-sysbench-macos.sh" "$@"
