# Independent Aave HF execution while dsc-data is unavailable

The user authorized bypassing the unavailable data machine on November15,2026.
This is a temporary Aave user-history execution change. The application database
and the general finalized extraction coordinator have not been migrated.

Three independent services now run disjoint immutable assignments:

| Role | SSH | Run directory | Processes |
|---|---|---|---:|
| node | node@100.108.171.14 | /home/node/hf-failover-20261115 | 14 |
| revert | dsc-revert@100.91.215.76 | /home/dsc-revert/hf-failover-20261115 | 8 |
| web | fao@100.119.128.31 | /home/fao/hf-failover-20261115 | 6 |

Each directory contains `plan.json`, `code/`, `state/progress.json`,
`state/receipts/<generation>/`, `state/pilot.json`, and `public/status.json`.
Services are `dsc-aave-v3-hf-independent.service` and
`dsc-aave-v3-hf-independent-status.service`. They are enabled at boot. Existing
legacy worker services were left unchanged: automatic approval rejected stopping
and disabling them. No credentials or private keys were copied.

The new generation is
`92e5c07aa3f71cd345cf03935995f786d6023e05725366541b932d49f3f4c009`;
the assignment file SHA256 is
`c73c708670d60c2d69b5b1ddd1cce31eadbfe655c4f4bdce7c8afcaf60bd47be`.
It contains92249 new tasks and45 recovered tasks. Each new task has at most10000
blocks and approximately10000000 logical account rows. All users and every-block
precision remain included. Coverage is deployment16291127 through fixed finalized
cutoff26407881 inclusive; no future coverage is implied.

Input images remain immutable under each home at
`aave-hf-distributed/5197dae89b140ccb/images/1d1a9be72b345989a4a8667573e929c66320535824f480517e6e440633d0e32d`.
All13737 files were verified on each host. A systemd mount namespace presents the
image at `/home/dsc-data`, old worker state read-only at
`/home/dsc-data/worker-state`, and the host's existing `outputs/` directory at
`/home/dsc-data/data/aave-user-history-v3/26407881/accounting/hf-numeric`.
Virtual paths in receipts must be mapped to those physical paths for external
read-only inspections; never rewrite receipts to make a check pass.

Recovery compares file hashes, row counts, owner universe, coverage and retained
validation identities. Duplicate prior-generation outputs must match before
being counted once. The45 recovered tasks total337890530 rows and retain their
original receipts and generation. They overlap the last cached data-machine
acceptance count124724784; never add those two figures. A recovered worker
receipt is not a claim that the unavailable data tracker has already imported it.
The immutable plan selects one counting owner for each recovered interval.

The native integer calculator and original receipt/node checks are reused.
Python multiprocessing.Pool(maxtasksperchild=1) gives each task a fresh process.
The existing materializer accepts an optional bounded `row_group_rows` value;
the independent run uses100000, while its default remains1000. This changes
physical Parquet grouping, not account values. Existing interrupted and accepted
files are preserved. Nominal token balances remain queryable from complete
factors; this export is the six account metrics rather than a fully expanded
all-token nominal table.

`failover_hf.direct_node` reuses Web3.py and the existing `accounting_node`
snapshot helper against the verified own-node LAN endpoint
`http://10.246.103.160:8545`. It verifies chainId and the fixed cutoff hash.
No dRPC or data-to-host SSH is required. Each new job retains start/middle/end
node sampling, exact six-field comparisons and block identity checks.

The private monitor URL remains https://dsc-web.tail5aad6c.ts.net/ . Each host
exposes only aggregate progress at its LAN address, port18879, path`/status`,
using Tornado. There is no control or dataset download route. The web monitor's
`hf-failover-sources.json` fixes identities before aggregation. `processed_rows`
is `new_rows + recovered_rows`; in-flight rows and pilots are separate.
Top-level data-machine errors and old primary pipeline snapshots remain visible;
`hf_fallback_fetched_at` and `hf_fallback_error` identify fresh independent HF
progress. The page was verified in a desktop browser; mobile was not separately
tested.

If data SSH becomes reachable, the independent runners stop their own unfinished
work and report `awaiting_user_decision`, leaving files for reconciliation. This
prevents unreviewed simultaneous recovery with unchanged legacy services. Inspect
both acceptance histories before merging. No automatic deletion or relabeling of
old receipts is allowed.

The previous single-block speed or17-day linear estimate is not a guaranteed
completion date. Continue checking per-host memory, storage, output acceptance,
and node samples. A50GiB free-space reserve is enforced. No remote power-control
or usable wake configuration was found for data; its physical state remains
unconfirmed. Evidence is under `docs/data-collection/aave-v3/evidence/failover-20261115/`
and the append-only research log.

The user subsequently changed agent supervision to every four hours and
authorized measured optimizations and data-machine rejoin after manual startup.
See [REJOIN.md](REJOIN.md) for backup destinations, incremental commands,
retained research variables and the validated handover sequence.


## Cached integer kernel rollout — 2026-11-16T06:43:43.586231+00:00

Current active generation `d0d4f91ef23bc9dc5a91c24b4fdf8d12a1e64d5e6fb2d9f0df4d6a532c556c9e`, plan SHA `cde26d58c9549a98fe4a15b09c320e43f0e2c820a1b4c8a664af4a2736d83e6d`. Read each host's `hf-failover-20261115/active-plan.json`, verify its plan hash, and use `rollouts/<generation>/code` rather than the preserved base/code. The same independent services run node14/revert8/web6; resource limits and the data-return reconciliation guard remain. All three now activate private shm. Startup revalidates recovered file hashes before publishing new-generation progress; a stale previous-generation status is not live progress.

The new plan preserves 1368 accepted intervals / 12783040547 rows with their original receipts and identities, and assigns 90926 remaining intervals without changing the 910310934885 logical user-block rows. The accepted web canary 18187373–18188065 adds 9995664 current-generation rows and is reused normally. Its measured 155.37s versus roughly 291s for adjacent old jobs supports adoption, but is not a measured whole-run speed multiplier.

The kernel reuses Python functools.lru_cache for exact shared price/index inputs; only bounded preparation and reserve-presence validation changed. Formula/rounding and all six metrics are retained. Numeric reference checks covered 220365 user-block observations and all 12 profiles; bitmask equivalence covered 4608 cases. New-production canary passed ordinary node acceptance. Old source/plan/output files are preserved.

Backups for checkpoint 20261116T042153Z are verified on separate machines; see evidence *-backup.json. Backup helpers now honor active-plan and original-generation receipt locations. No application DB migration is implied. Evidence: docs/data-collection/aave-v3/evidence/supervision-20261116T042153Z.
