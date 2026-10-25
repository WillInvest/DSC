## Direct replay revision, 2026-10-25

The user explicitly removed recurring RETH comparisons and block-header scans.
The active runner now uses `reconstruction/direct.py` to replay decoded event
files locally in 10,000-block chunks. It writes every block, forward-filling
unchanged state, and serializes state only when relevant events occur. It makes
no state RPC calls for Uniswap. Aave prepares boundary seeds and supplemental
configurator/provider logs once, then also replays locally; those are inputs,
not endpoint checks. The official event handlers remain unchanged.

Output is under `reconstructed/<pool>/event-derived-unchecked-v1/`, with a
separate identity and `validation: none`. Previous checked outputs remain in
place and their progress is preserved as checked-progress.json. Do not interpret
new completion as node-verified coverage. Blocks without events have no invented
block hash. No legacy tracker acceptance bits are set.

The scheduler yields after approximately 30 seconds of replay to inspect decode
backlog, then continues immediately if no decode work is ready. There is no
10-second wait between reconstruction chunks. Resume uses atomic chunk files
and a saved state/cursor; interrupted chunks are rewritten. File-level source
receipts still prevent incomplete transfers being consumed. Repeated decoded
output hash scans have been removed from this direct path.

## Services and recovery

- dsc-sequential-events.service runs the existing Cryo queue.
- dsc-event-pipeline.timer rescans after offline work exits. While reconstructing,
  the runner immediately selects the next task without the timer delay.
- dsc-event-watchdog.timer checks worker heartbeats every minute and restarts
  missing workers. Failed jobs back off five minutes. The offline child has an
  hour without progress before recovery is attempted.
- dsc-pipeline-monitor.timer publishes counters to Cryo's second tmux pane.

All timers start at boot. To deliberately stop work, stop the pipeline and
watchdog timers before stopping the pipeline service. Otherwise recovery is
expected. Runtime state lives in /home/dsc-data/runs/event-pipeline; extraction
state and the immutable plan live in /home/dsc-data/runs/sequential-events.
The fixed cutoff is 26,266,551. It does not automatically extend with the chain.

systemd provides scheduling/restart and process cleanup; flock prevents duplicate
workers; PyArrow provides Parquet; Web3 provides ABI calls/decoding. Project code
only schedules ready inputs, adapts files, applies selected protocol rules and
publishes progress. Reverted extraction is unchanged and is not scheduled here.

The web monitor also shows used/total/available storage for all four servers,
using decimal GB. Data/node/revert usage is cached every minute over existing
read-only SSH commands; dsc-web reads its own filesystem locally. All four
project paths currently share their respective root filesystem. Storage errors
and stale reports are shown rather than substituted with zero usage.
