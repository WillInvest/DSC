# Private extraction monitor

Open **https://dsc-web.tail5aad6c.ts.net/** with Tailscale connected.
The production workspace shows the 102-contract registry as Not started until
an actual production run records its target range. Verified pilots are a
separate workspace. Select a run, filter/search contracts, and tap a card to
inspect up to 1,000 blocks. Cells explain flags 1/2/4 and combined status 7.

## Services and trust boundary

- dsc-web: `dsc-monitor.service`, user `dsc-web`, code `/home/dsc-web/monitor`,
  local listener `127.0.0.1:18084`. Tailscale Serve supplies private HTTPS on 443.
  No Funnel/public listener is configured. The existing beta services are separate.
- dsc-data: fixed report command `/home/dsc-data/scripts/monitor/export.py`,
  configured by `/home/dsc-data/.config/dsc-monitor/sources.json`.
- The web collector connects to `dsc-data@10.246.103.151` with a dedicated key.
  Its authorized-key entry restricts the source to `10.246.103.143`, disables
  forwarding/PTY, and forces the fixed exporter command. Secrets are outside git.
- SQLite stays on dsc-data and is opened with `mode=ro` plus `query_only=ON`.
  The exporter never constructs Tracker or performs schema migrations.

FastAPI/Starlette serve the static HTML and GET-only JSON endpoints. Python's
SQLite library reads coverage; SSH carries reports. Custom code calculates the
project-specific coverage masks, progress and phone display. Runtime dependency
versions are pinned; `requirements-lock.txt` also includes API-test dependencies.

## Reporting behavior

The runner stores immutable `monitor_runs` target manifests and `monitor_jobs`
associations. Targets include registry metadata, requested bounds and exact
per-dataset configuration hashes, split at Aave ABI profile boundaries. These
metadata tables are populated by the runner, never by viewing the page.
Legacy records without manifests do not get inferred percentages.

Reports use interval intersections: overall percent counts contract/block cells
where all three flags are complete. Dataset bars use each flag separately.
Contiguous progress starts at the run's required start, not its highest observed
block. Pre-deployment and outside-run heights are distinguished. Empty checked
results retain their completion bits. Up to ten missing ranges are summarized;
block inspection supports at most 1,000 blocks and ten contracts per request.

The coordinator writes a heartbeat every 15 seconds, even during long remote
calls. The monitor polls every 30 seconds. After 60 seconds without a running
heartbeat it shows Heartbeat missing; a cleanly stopped runner shows Stopped.
No recent stage change alone does not imply extraction failure.

Failed collection keeps the last report and labels the error; after 90 seconds
it is marked stale. Errors are classified into short messages; raw commands,
paths and credentials are not sent to the page. No pause, retry, extraction or
tracker-write endpoints exist.

## API and operations

- `GET /api/overview`: cached workspace/run summaries and freshness.
- `GET /api/contracts?run=...`: selected-run contract progress.
- `GET /api/jobs?run=...`: stage counts and latest 100 jobs.
- `GET /api/coverage?run=...&addresses=...&start=...&count=100`: bounded matrix.
- `GET /healthz`: service/report availability.

On dsc-web, use `systemctl status dsc-monitor` and
`journalctl -u dsc-monitor`. The systemd unit is included beside this README.
Restart only this service after changing backend code. Static assets do not
require a restart. Tailscale Serve configuration is persistent and private.

Deploy reporting/runner changes on dsc-data together. Copy the monitor files to
dsc-web, install the pinned environment, provision the restricted SSH key and
verified host key, then enable `dsc-monitor.service`. Set up Serve with
`tailscale serve --bg --https=443 http://127.0.0.1:18084` after tailnet approval.
Do not expose port 18084 on 0.0.0.0 or publish this app through the public domain.

## Verification (2026-10-23)

- 71 tests pass on dsc-data; four FastAPI tests pass separately on dsc-web.
- Reporter tests cover empty completion, partial intersections, gaps, overlapping
  intervals, config/ABI changes, legacy/missing trackers and read-only DB hashes.
- API tests cover bounded requests, pre-deployment/outside-run cells, rejected
  writes, and stale-cache preservation after collection failure.
- Live HTTPS returned 200 with a valid certificate; Tailscale Serve reports
  tailnet-only access. Existing beta services remained active.
- Chrome at a 390 × 844 CSS viewport had scroll width 390, no console errors,
  working protocol filters, and Aave block detail showing all three checks complete.
- Both verified pilot runs display 100%; production remains Not started.

The final device check is opening the URL on the user's phone with Tailscale on.
No full historical extraction was started to build or test this monitor.
