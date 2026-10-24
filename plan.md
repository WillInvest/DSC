# Unified extraction completion and monitoring plan

Recorded: 2026-10-23.
Status: coordinator, tracker and phone monitor deployed. Production backfill is running through block 26,259,811 with 15 reviewed Aave profiles and systemd restart supervision. See `scripts/orchestration/production-run.md`.

## Objective

Maintain one combined completion record for every tracked contract and block.
Use it as the shared source of truth for extraction scheduling, retries, staging
cleanup, and a future monitor. Keep the three dataset statuses together rather
than maintaining three independent status files.

## Agreed completion matrix

- Rows: block numbers.
- Columns: tracked contract addresses.
- Cell: an integer bitmask showing which datasets are complete.

| Bit | Dataset |
| --- | --- |
| 1 | Events |
| 2 | Block-state |
| 4 | Reverted transactions |

| Value | Completed datasets |
| --- | --- |
| 0 | None |
| 1 | Events |
| 2 | Block-state |
| 3 | Events and block-state |
| 4 | Reverted transactions |
| 5 | Events and reverted transactions |
| 6 | Block-state and reverted transactions |
| 7 | All three |

Example:

| Block number | Contract A | Contract B |
| --- | --- | --- |
| 13,000,000 | 7 | 1 |
| 13,000,001 | 5 | 0 |

The matrix is the logical interface. Physical format: SQLite on dsc-data, with compact interval coverage and a combined per-block bitmask lookup.
Do not silently replace the agreed single combined view with separate dataset
status files. The durable completion record belongs on dsc-data; chain ID and
extraction configuration must identify the scope of its entries.

## What complete means

Set a dataset bit only when its extraction is successful, coverage is validated,
and the result is verified and durably stored on dsc-data. Local extraction alone
is not completion. Database ingestion may be tracked separately; durable file
delivery does not imply PostgreSQL ingestion has finished.

A successfully checked block with no matching events or reverted transactions
still counts as complete. Preserve evidence of successful empty coverage.
For events, completion covers all required event types, not just one successful
Cryo invocation. Decode failures or incomplete query coverage must not set the bit.

Use an atomic bitwise OR to mark completion, so concurrent workers cannot erase
other dataset bits. Repeated acknowledgements must be idempotent. Invalidate only
affected bits when a block hash changes or the extraction configuration changes;
preserve unrelated completed work and retain the reason for invalidation.

Coverage is fixed by user decision: each tracked address from its verified
deployment block through the latest finalized block captured for the run, with
state at every block (step 1). Record the high-water block number/hash and extend
it on subsequent catch-up runs. Blocks before contract creation must not appear
as ordinary missing work. Handle initialization and historical ABI/upgrade
boundaries explicitly without silently dropping requested blocks. Applicability is separate from completion; do not set a bit merely
to hide a dataset that was never required.

## Supporting records

The bitmask is the completion summary, not a complete job log. Keep supporting
records alongside it for:

- Pending, running, failed, and retrying jobs, with timestamps and errors.
- Chain ID, contract, block range/hashes, dataset, ABI and extraction versions.
- Output files, row counts, checksums, verified delivery, and cleanup acknowledgements.
- Sampling and applicability rules for interpreting coverage.

These can share the chosen status store; they do not require three separate
completion matrices. Progress must survive deletion of staging files.

## Responsibilities and data delivery

Preserve [AGENTS.md](AGENTS.md) and the finalized
[event extraction rule](extraction-rule.md).

- **dsc-node:** use Cryo to extract encoded events and end-of-block state; run
  failed-transaction tracing under its separately defined rules.
- **dsc-revert:** retain the complete failed-transaction trace archive, match tracked
  contracts, and deliver the selected reverted data to dsc-data.
- **dsc-data:** decode events and state using the official ABI and an established
  ABI library; retain application data and completion records, and ingest data.
- **dsc-web:** host the backend and monitoring API, read the database on dsc-data,
  and display the future monitor.

Maintain shared reverted-trace archive coverage by block range independently of
the per-contract matrix. Trace once per extraction configuration and reuse the
archive when adding contracts. A contract's reverted-transaction bit means its
selection is complete and delivered, not merely that traces reached dsc-revert.
The archive scope remains mined transactions with receipt status `0x0`.

For node staging cleanup:

1. Finish writing a batch.
2. Transfer completed files to dsc-data.
3. Verify destination size and checksum, validate coverage, and durably record delivery.
4. Set the appropriate completion bits after all required files are accepted.
5. Delete eligible node staging files and record cleanup.

A crash between these steps must be safely retryable. Keep node files after a
failed transfer or verification. Consult the completion store before restarting
Cryo: file skipping alone cannot prevent re-extraction after staging cleanup.
Do not delete the retained failed-transaction archive on dsc-revert under this policy.

## Future monitor

Build the monitor from the same completion record:

- Block-by-contract heatmap: 0 means no completion, 1–6 partial, 7 all three complete.
- Show exactly which datasets are missing for a cell.
- Completion percentages and coverage gaps, respecting applicability and sampling.
- Latest contiguous completed block per contract and dataset; do not substitute
  the highest isolated completed block for contiguous progress.
- Running, failed, and stalled jobs, processing speed, and recent errors, using
  supporting job records and timestamps rather than inferring them from the bitmask.

## Implementation sequence

Build and verify reverted extraction/archive scripts first (see `reverted-extraction.md`); build the unified tracker afterward. Local archive/checkpoint files are not the unified completion matrix.

1. Establish the tracked contract list and deployment blocks; apply the agreed
   deployment-to-current-finalized coverage with state at every block. Resolve
   applicability/upgrade boundaries and choose the single status store's physical format.
2. Implement durable, atomic completion updates and supporting delivery/job records.
3. Connect extraction and verified delivery to the completion store, including
   successful empty ranges and per-event coverage reconciliation.
4. Add resumable transfer and cleanup that preserve completion records.
5. Add the monitoring API and monitor on dsc-web, backed by the database on dsc-data.

## Acceptance checks

- Completing events then state produces 3; adding reverted transactions produces 7.
- Concurrent updates and duplicate acknowledgements preserve completed bits.
- A verified empty result marks its dataset complete; an extraction error does not.
- Failed or interrupted transfer does not mark completion or delete the source.
- Restart after staging deletion skips already completed work using the status store.
- Changed hashes/configurations invalidate affected coverage without erasing unrelated work.
- Adding a contract reuses archived reverted traces where coverage exists.
- Monitor percentages and contiguous progress reflect real gaps and sampling rules.

Writing this plan does not deploy a status store, start extraction, or enable deletion.

## Implemented orchestration interface

`python scripts/run.py --contracts contracts/ethereum/pools.csv` runs on dsc-data.
It owns local SQLite, schedules 1,000-block batches, controls workers over LAN SSH,
and records extraction, transfer, validation, acceptance and cleanup stages.
`--dry-run`, `--status`, inclusive `--from-block`/`--to-block`, `--profiles`,
`--data-root`, `--servers` and opt-in `--cleanup` are supported.

Server-to-server routing uses dsc-node 10.246.103.160 and dsc-revert 10.246.103.26;
dsc-data is 10.246.103.151. dsc-web is not the coordinator.

See `scripts/orchestration/README.md` for implementation details, deployment
and remaining limitations. CLI status and bounded matrix export are implemented;
the monitoring website and application-database ingestion are subsequent work.
Full Aave ABI history is not supplied by the tracker.

## Verified implementation results (2026-10-23)

- 62 automated tests pass, including interval merging, atomic rollback, config
  isolation, backup restoration, verified-empty completion, archive reuse across
  different ranges, missing-profile blocking, and restart after transfer.
- Live private-LAN pilots covered Uniswap V2/V3 and Aave V2 at block 13,000,000,
  and Uniswap V2/V3 and Aave V3 at block 17,000,000. Every tested cell reached 7.
- Six event batches were verified empty and still completed.
- Both repeat runs left all 16 job attempts/stages unchanged: two selection jobs
  accepted and 14 node-staging jobs cleaned. Two block hashes are stored once
  in the tracker, shared across contracts.
- An isolated tracker copy simulated an interrupted acknowledgement after file
  transfer. Recovery reached 7 without running Cryo again.
- Evidence: `scripts/orchestration/pilot-validation.json`; server pilot records
  are under `/home/dsc-data/data/runner-pilot`. The production 102-address CSV
  is deployed at `/home/dsc-data/contracts/ethereum/pools.csv`.

Remaining before the complete historical backfill: reviewed Aave ABI profiles
covering its full history and initialization/upgrade boundaries, plus a larger
capacity/throughput pilot. Aave currently invokes Cryo per block for reserve
calls; measure that overhead without reducing the required frequency.
The monitor UI and PostgreSQL ingestion remain separate work.

## Phone monitor deployment (2026-10-23)

The read-only phone monitor is implemented on dsc-web at
https://dsc-web.tail5aad6c.ts.net/ through private Tailscale Serve HTTPS.
Server-to-server reporting uses LAN SSH to dsc-data, with a restricted fixed-command
key and read-only SQLite access. The page refreshes every 30 seconds and shows
separate production/pilot workspaces, per-dataset progress, jobs/errors, heartbeat,
missing ranges and bounded 0–7 block details. Runner target/configuration manifests
and heartbeat support were added without changing extraction semantics.

See `scripts/monitor/README.md` for deployment, API, testing and operations.
71 data-side tests and four web API tests pass. Live HTTPS and a 390-pixel browser
layout were verified; final phone access is a user-device check.
