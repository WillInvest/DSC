# Extraction coordinator

Run `scripts/run.py` on **dsc-data**, using its existing `.venv-abi` environment.
It controls dsc-node and dsc-revert over private-LAN SSH. No Tailscale connection
is used between servers. SSH keys stay in each account's `.ssh` directory.

```bash
/home/dsc-data/scripts/.venv-abi/bin/python /home/dsc-data/scripts/run.py \
  --contracts /home/dsc-data/contracts/ethereum/pools.csv \
  --profiles /home/dsc-data/data/deployment-profiles --dry-run
```

Remove `--dry-run` to run. `--from-block` and `--to-block` are inclusive pilot
bounds; defaults are deployment through a captured finalized block. State step
is always one. `--status` reports jobs and coverage by address, flag and config.
`--matrix --from-block N --to-block M` exports up to 1,000 rows of the combined
block-by-address matrix; pre-deployment entries are N/A.
An unfinished run retains its cutoff. A successful subsequent run catches up.
The registry's `through_block` is provenance, not a permanent extraction cap.

`--pool-workers 1|2|4` bounds concurrent contracts in the events/state lane
(default 1; production uses 4). Each pool processes events then state with its
own SQLite connection. `--rpc-requests 8|16|32` sets Cryo request concurrency
per process (default and production: 8). Four pool workers can therefore issue
up to 32 concurrent Cryo requests, plus validation/header RPCs and the separate
trace lane. These runtime settings do not change accepted-data identities.
Remote shared lane locks and exclusive per-pool locks allow different pools
together while excluding duplicate work or cleanup for the same pool.

Default SQLite location: `/home/dsc-data/data/tracker.sqlite`. Use `--data-root`
for isolated pilots. Production uses `--lane events-state` and `--lane reverted`
with the same explicit `--to-block`. Each lane advances independently. The
default `--lane all` retains serial pilot behavior. Lane locks exclude duplicate
workers, and the shared root lock excludes simultaneous legacy serial runs.
The coordinator uses transactional
SQLite updates and compact per-dataset coverage intervals. `Tracker.status`
combines those intervals into the single logical 0–7 cell. Missing intervals are
zero; code/profile identities prevent old completion from satisfying changed
extraction. The schema is not a dense address-by-block table.

| Flag | Accepted result |
| --- | --- |
| 1 | Events decoded/validated, including verified empty queries |
| 2 | Every required state call validated and decoded |
| 4 | Reverted selection verified on dsc-data, including empty matches |
| 7 | All three |

The flags mean durable file acceptance, not future PostgreSQL ingestion.
Aave's reserve list is rediscovered per block and all reserve calls are required.
Missing/overlapping Aave profiles are blocked, not complete. Historical profiles select pinned implementation-specific ABIs and include
reviewed initialization/upgrade boundaries. See `contracts/ethereum/aave-profiles/README.md`.

## Files and flow

- `tracker.py`: transactional interval coverage, jobs, invalidation and backups.
- `coordinator.py`: scheduling, SSH dispatch, transfers and acceptance.
- `worker.py`: bounded node/revert operations with server-side locks.
- `artifacts.py`: atomic JSON receipts and checksummed inventories.

Existing Cryo launchers and Web3.py/eth-abi decoders do extraction/decoding.
Event validation reconciles raw log identities with a bounded native RPC query;
this explicit extra query establishes empty and complete coverage. It is a
correctness-first cost that should be measured before a full backfill.

Node writes batches under `/home/node/dsc-extraction/data/runner`. Events/state
are pulled directly to dsc-data's `incoming/<job-id>` and decoded into
`accepted/<job-id>`. Reverted archives move directly from node to
`/home/dsc-revert/data/runner/archive/<archive-id>`. Selection happens only on
dsc-revert and is pulled into dsc-data's `selected/<job-id>`.

Each manifest binds the request, configuration, per-block hashes and payload
checksums. Every stage is recorded in the tracker. A restart verifies/reuses
sealed raw artifacts and existing decoded manifests before acknowledging data.
Unsealed batches rerun the existing resumable scripts and revalidate coverage.
A detected chain hash mismatch invalidates the affected range and stops for
inspection; stale artifacts must be quarantined before recovery.

Reverted archives are retained by configuration and block in `archive-store`.
New contracts and differently bounded batches reuse those blocks. For a partial
archive hit, already archived blocks are restored into the node batch; the
existing extractor skips them and traces only missing blocks. Archive validation
and contract filtering still execute on dsc-revert.

`--cleanup` removes only acknowledged node staging. Without it, staging is
retained. Cleanup never removes dsc-revert archives or unrelated files.
Cleanup failures leave `cleanup_pending` jobs; rerunning the range with
`--cleanup` retries cleanup without re-extracting accepted work. Accepted flags still remain valid.
The local incoming/raw copy is retained in this first version.

The tracker backup is refreshed after each batch through SQLite's backup API.
Do not copy an open database as a backup or mount it across servers.

## Deployment

Default server accounts and paths are in `coordinator.DEFAULTS`. `--servers`
accepts a JSON replacement with `node` and `revert` entries, each containing
`host`, `scripts`, `python`, and `root`. Defaults use `node@10.246.103.160` and
`dsc-revert@10.246.103.26`. Deploy identical orchestration code to all three
servers and matching extraction/protocol/decoder code to node and data.
Worker code fingerprints reject mismatched extraction deployments.

Preflight checks available space (1 GiB minimum), SSH, rsync/flock, Ethereum
chain/finality and profile pins. Dependency/ABI errors fail the relevant job.
Remote subprocess failures retry three times. Archive/selection endpoint
unavailability blocks reverted jobs while event/state work proceeds.

The production coordinator runs under `dsc-extraction.service`, enabled at boot
with failure restart. Its fixed cutoff is 26,259,811. See `production-run.md`
for operations and `../monitor/README.md` for the private phone monitor.
Monitor disk capacity and throughput during this first historical backfill.
