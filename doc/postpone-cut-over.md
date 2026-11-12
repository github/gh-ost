
### Postponing Cut-over vs. Throttling for Maintenance Windows

When managing long-running migrations (e.g. 100M+ rows / multi-hour executions), it is important to distinguish between **throttling** and **postponing cut-over**:

| Operational Feature | Throttling (`--throttle-additional-flag-file` / `echo throttle`) | Postponing Cut-over (`--postpone-cut-over-flag-file` / `echo postpone`) |
| :--- | :--- | :--- |
| **Row Copying** | Paused | Completes to 100% |
| **Streamer & Changelog** | Suspended / idle | **Active** (tails binlogs in real-time, near 0s lag) |
| **Heartbeat Injector** | Suspended (HeartbeatLag grows) | **Active** (keeps connection alive) |
| **MySQL `wait_timeout` Risk** | High (idle connections can disconnect if throttled >8h) | **None** (continuous stream prevents connection timeouts) |
| **Database Load** | 0% | Near 0% (only processes incoming live DML events) |
| **Recommended Use Case** | Immediate load shedding during unexpected DB spikes | **Scheduling cut-over table swap for off-peak maintenance windows** |

#### Recommended Multi-Hour Workflow:
1. Start migration with cut-over postponed (`echo postpone`).
2. Allow `gh-ost` to complete row copying in the background while keeping replication lag minimal.
3. During your designated maintenance window, trigger cut-over with `echo unpostpone` (or remove the postpone flag file) for an instant (<1-2s) table swap.

### Note on UUID / Alphanumeric Primary Keys and Progress Metrics

When migrating tables keyed by alphanumeric or UUID strings (`VARCHAR(36)`), `gh-ost` iterates across the lexicographical key space (`0000...` to `ffff...`). 

Because initial total row counts rely on MySQL `EXPLAIN` statistics, the estimated total rows may diverge from the actual density of keys traversed. During such migrations:
- Progress may display percentages greater than 100% (e.g. `139%`, `164%`).
- `ETA: due` indicates that the row copy has traversed past the initial estimate and is in the final portion of the keyspace. Operators should inspect the highest key copied in the ghost table relative to `MAX(pk)` on the original table to observe real-time boundary progress.
