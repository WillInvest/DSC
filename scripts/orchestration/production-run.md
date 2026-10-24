# Production historical run

`dsc-extraction.service` starts two independent workers on dsc-data at boot:
`dsc-extraction@events-state.service` and `dsc-extraction@reverted.service`.
Both run as `dsc-data`. Systemd restarts a failed worker after 60 seconds. It reuses the same local
tracker, fixed cutoff, profiles and registry; destination-accepted coverage is
not discarded. A successful bounded run exits without restarting indefinitely.
This supervises process failure and reboot recovery, not arbitrary application
hangs. Network/worker errors remain visible in the tracker and monitor.

The initial run covers all 102 addresses from deployment through finalized block
26,259,811 inclusive, with state at every block and events/reverted checks for
empty blocks too. The service command pins that cutoff to the 15 reviewed Aave
profiles. Do not remove the cutoff before extending/reviewing profile coverage.

The coordinator uses private LAN SSH, not Tailscale, between workers. The existing
node watchdog keeps RETH/Lighthouse running. Tailscale is used for phone monitor
access only: https://dsc-web.tail5aad6c.ts.net/ .

```bash
sudo systemctl status dsc-extraction.service
systemctl status dsc-extraction@events-state dsc-extraction@reverted
journalctl -u dsc-extraction@events-state -u dsc-extraction@reverted -n 50 --no-pager
sudo systemctl stop dsc-extraction.service
sudo systemctl start dsc-extraction.service
```

Each worker advances through its own 1,000-block batches. Events/state do not
wait for tracing. Both share the SQLite tracker and fixed cutoff; flags 1/2/4
retain their meaning, and 7 still requires all checks. Each lane has its own
lock, heartbeat, current-batch file and SQLite backup. The parent unit controls
both workers; restart a child unit to recover only that lane. A successful
lane exits and remains Complete while its peer continues.

`--cleanup` removes only acknowledged node staging; it retains the dsc-revert
archive and dsc-data accepted files. No protected datasets or Savva transfer
paths are involved. The current runner retains incoming raw files on dsc-data.
Disk usage must be monitored during this first full backfill; initial free space
was approximately 854 GiB on data, 782 GiB on revert and 11 TiB on node.

Profiles and proof: `contracts/ethereum/aave-profiles/README.md` and its evidence
files. Review receipt: `/tmp/compound-engineering-501/ce-code-review/aave-profiles-20261023`
on the operator workspace (temporary review record, not a runtime dependency).

## Independent-worker rollout (2026-10-24)

Both worker services started successfully with the original monitor run ID and
accepted coverage. Live verification showed events/state working on
`10010355:10011355` while reverted worked on `10009355:10010355` (exclusive stops).
The first batch's combined completion and the second batch's events/state were
retained. Startup reuses accepted batches and incomplete node archive files.

Validation: 80 tests passed in the full data-server suite, six API tests passed
separately, and the additional serial-mode status cleanup regression passed.
Systemd unit validation passed. Review completed without remaining findings.

## Pool concurrency rollout (2026-10-24)

The events/state lane now uses four concurrent pools, each with eight Cryo RPC
requests. The reverted lane remains at two trace workers. Each pool has its own
SQLite connection and remote lock; tracker bits and data fingerprints are
unchanged. Set `--pool-workers 1` in the systemd template and reload/restart the
service to return to serial scheduling without discarding accepted coverage.

Measured node-state worker throughput improved 3.86x on the selection range and
2.93x on a second historical range. This is not a full-backfill ETA multiplier.
See [benchmark report](../../docs/optimize/extraction-throughput-report.md) for
measurements, correctness checks and limitations.
