# Float64 user HF and weighted indicators

The user authorized the reviewed Drive plan on 2026-11-16 and then explicitly
selected the following amendment: **total account debt below USD0.01 is treated
as zero**. The earlier exact-generation pause remains intact. This document
governs the new method; `VECTOR-ROLLOUT.md` records the preserved exact method.

## Scientific definition

- Scope: Ethereum Aave V3 main market, block16291127 through26407881 inclusive,
  with the pinned finalized hash in `hf_f64_contract.py`.
- Every address from its first appearance through the cutoff, including exited
  accounts:258131 addresses and910310934885 logical user/block values. There are
  92294 disjoint tasks. No user or block downsampling.
- The cutoff uses historical Aave debt valuation:1USD=1e8 base units. Exclude an
  account when its total debt is strictly below1000000 base units; exactly one
  cent remains included. Both genuinely debt-free and excluded accounts have
  positive-infinity HF in this analytical dataset. Raw inputs are retained, so
  official account values can still be reconstructed.
- For each borrowed reserve, save `N=sum(user_reserve_debt * user_HF)` and
  `W=sum(user_reserve_debt)`. Its research indicator is `N/W`; protocol N/W is
  also checked against direct user-debt reduction. No-debt denominator means
  null, never zero. These are **research indicators, not official Aave reserve
  or protocol HF getters**.
- Per-block exclusion counts and debt-value estimates are retained in the
  protocol proof row. Counts concern nonzero debt-position owners treated as
  negligible, including positions whose base valuation rounds to zero. Amounts
  are float64 diagnostics, not exact integer accounting. Removing tiny debts can
  still affect a debt-weighted HF numerator when their account HF is large; this
  is an explicit sample rule, not a claim that its aggregate effect is zero.
- All historical scaled-position, interest, price, collateral, threshold,
  eMode/GHO inputs and old accepted outputs remain available. Float64 results
  use an error budget; they are not lossless uint256 reproductions or a guarantee
  of identical HF<1 classification at machine-precision boundaries.

## Implementation and formats

Existing `native_batch.BatchEvaluator` owns ordered historical replay and shared
normalized indices. `hf_batch_f64.py` uses NumPy sparse position arrays and
`bincount` reductions, preserving the historical threshold/eMode/GHO branches.
The unchanged integer kernel handles ambiguous threshold/one-cent boundaries
and remains the reference. Tiny excluded positions need not be fully recomputed.

Old14-column and exact-vector HF are cast, then subjected to the same one-cent
policy; only their debt weights are rebuilt. The frozen reconciliation manifest
and per-job source/role/coverage digests are bound to the generation. Data also
checks each returned source digest before catalog commit. Source files are
verified once per task into an HF-only matrix capped at32M values; the largest
actual retained-source rectangle is25760000 values. Normal output windows are
also bounded at32M values, and each user Parquet tile has at most1M HF elements.

User schema is `wallet_id:uint32`, `first_block_index:uint32`,
`hf_vector:list<float64>`. The true first index is global; file `chunk_start` and
`chunk_stop` identify its physical segment. The shared wallet dictionary and
reserve/protocol namespace mapping are in `users.parquet` and `dataset.json`.
Parquet/Zstd9 is used through PyArrow; decoding must recover identical float bits.

Aggregate files contain scope ID, first index, HF/N/W vectors and protocol-scoped
`excluded_users_vector`/`excluded_debt_base_vector`. Reserve rows have null
exclusion lists because those diagnostics are global, not duplicated by reserve.
User files, aggregate proofs, samples and their validation report are bound by
one immutable receipt. Only Data's accepted SQLite catalog advances coverage;
node folders, partial files and benchmark receipts do not.

## Authority and paths

Current generation:
`db2fce32bfc0a227f5d527ffee45c8212d1da0a939ec26447be07ada39af3efa`.
Plan SHA256:
`e0de3b44ab0ea4fd6004310fa86364e80abfaf51d2a75279b24553965261ca01`.

- Data root: `/var/lib/dsc-aave-hf-f64/20261116-v2`.
  `f64-plan.json`, `tracker.sqlite`, `receipts/`, `public/`, `audits/` and
  `release-gates.json` are authoritative. Use online SQLite backup, never file
  globs, to inspect accepted coverage.
- Node storage: `/home/node/hf-f64-v2-20261116/bulk`.
  Accepted parts live under `outputs/<generation>/jobs/<job_id>/`.
- Other code/checkpoint roots: `<home>/hf-f64-v2-20261116`; all roles use the
  original verified read-only image and their existing private TLS identities.
- Scheduler: private TLS Data LAN18896. Compute slots:Data2, node4, revert2,
  compute2. The canonical new producer is
  `dsc-aave-v3-hf-f64-producer.service`; its new-root `PAUSED_BY_USER` stops
  dispatch. Capacity/result waits poll for pause and have finite deadlines.
- New workers/accept/archive units use `dsc-aave-v3-hf-f64-*`. Never stop or
  restore the protected legacy `dsc-aave-v3-hf-workers.service` for this work.
  Do not remove the old exact root's pause marker or its systemd condition.

Full history may run only when the actual `release-gates.json` has
`approved:true`. A canary-complete state does not imply full-history completion.
The matching gate is approved and the full producer started at
2026-11-16 20:47:55 UTC. Post-canary accepted tasks were observed; history remains
in progress. Four independent node acceptance slots share400% CPU/10G memory,
following a32-core/load9/213GiB-available capacity check.

## Verified evidence and iterations

- Preserved raw-method canary800c:6 jointly accepted tasks,49409070 HF values.
  Its old code, files and evidence remain separate and are not added to the new
  generation's totals.
- Attempt b744:zero accepted coverage. Its plan/dataset/code were preserved when
  a Python3.10 `asyncio.TimeoutError` exposed an incorrect timeout catch. The
  corrected generation handles that actual exception and keeps polling safely.
- Current one-cent method:32 tests passed, including the real217644-user
  checkpoint-vs-full-prefix HF/debt comparison. All12 historical profiles,
  covering1113566 reference user/block states, passed user, denominator,
  aggregate and classification checks under the amended policy.
- Whole-job compute/encode/write:9794116 values, same host/one-core cap,
 82.06s exact versus11.37s float64,7.22x. Output was19.11MB versus7.59MB including
  aggregates.160785 excluded user/block states were independently checked to
  have exact debt below the threshold. This is sample evidence, not a uniform
  all-history compression or throughput guarantee.
- Full transport/write/node-validation/Data-commit benchmark:109.50s versus
 22.08s,4.96x, with6 fresh node getter samples for each method. Benchmark receipts
  are outside the production catalog and do not advance coverage.
- Source-population check verified all92294 task counts against the frozen
  dictionary, not merely their grand total. All18 pinned calculation/acceptance
  source files matched on all four hosts.
- Each server built its checkpoint locally. All checkpoint payload hashes are
  `0810f82a8e84ce532eac4b946412e3f0d96b8cc3f75f429858f6240725f525f5`.
  No account checkpoint was copied through the Mac. Builds took about12s on
  Data/revert/compute and59s on node, demonstrating why endpoint throughput
  must be measured per host.
- Actual code review found and resolved bounded-wait, source-binding and
  repeated-source-scan defects. The pre-fix receipt and follow-up evidence are
  retained under `docs/data-collection/aave-v3/evidence/float64-plan-review-20261116`.
  It is not relabeled as a second review of the fixes.

## Supervision and archive

The existing `aave-v3` automation remains every four hours and has been updated
for this method. `audit_hf_f64.py` snapshots Data's catalog, checks real new files
and selects stored HF values (including values near1) for fresh own-node reads.
No new accepted output means0 extra RPC. Each round caps extra provider methods
at200; normal per-job acceptance requests are recorded separately. The initial
115-call model probe and first audit capped at85 share one200-call budget.
The first stored-vector audit passed12 fresh samples using51 methods, giving166
extra methods in this startup round. Its first harness failed before any RPC;
the canonical child-import fix and failed journal were preserved.

Log actual UTC checks, source/report hashes, all three product coverages,
continuous and highest block, gaps, recent endpoint throughput, cgroup/RSS/disk
evidence, actual errors, fixes and decisions. Do not fabricate missed checks or
count old references as fresh node calls. A mismatch pauses only the new
producer pending diagnosis; preserve all accepted and partial artifacts.

The private monitor is now `https://dsc-compute.tail5aad6c.ts.net/` after checking
the actual Tailscale DNS name and refreshing Serve. Historical role key `web`
and SSH `fao@100.119.128.31` remain compatible. New read-only role/archive endpoints
are Data LAN18897..18901. Monitor API checks passed. Mac Chrome navigation timed
out during UI verification; no claim of universal browser reachability is made.

Drive uses the existing private, limited `drive.file` authorization on compute.
Never print, copy or commit its token. Reused rsync/rclone operations transfer
only manifest-listed accepted artifacts. Each logical batch is about500GB;
250MB subchunks preserve compute's50GiB free reserve. Copy is immutable, all
remote sizes/MD5s are checked, and user/aggregate/dictionary files are fully read
back for SHA256 comparison before releasing **only compute staging**. Node
originals remain. The final-generation pilot verified26908803 bytes; it does not
mean a500GB batch or all-history backup has completed.

`backup_hf_f64_control.py` uses online SQLite backup and Dask `Client.scatter` to
preserve closed catalogs, manifests, dictionary, approval and code on node. The
approval snapshot had32 files/52337349 bytes, all hashes verified. Data remains
the sole live authority. Restore its receipt copies from node's originals using
the catalog IDs/hashes and recheck those hashes before recovery.

Runtime units and actual dated calibration scripts are in
`deployment/f64-20261116/` and `validation/20261116-f64/`. They contain credential
paths, never credential material. Review the captured bounds before rerunning;
do not overwrite accepted calibration or production files.

Activation capacity:Data692.4GiB, node9632.2GiB, compute51.01GiB available.
Revert had49.85GiB and remains an in-memory, read-only worker: no spill, new local
outputs, staging or backup. Its protected trace archive stays intact. The50GiB
hard reserve still applies to actual output/staging writers.

Reused APIs: [NumPy bincount](https://numpy.org/doc/2.2/reference/generated/numpy.bincount.html),
[PyArrow Parquet](https://arrow.apache.org/docs/python/parquet.html),
[Dask futures](https://docs.dask.org/en/stable/futures.html),
[SQLite backup](https://docs.python.org/3.10/library/sqlite3.html#sqlite3.Connection.backup),
[rclone copy](https://rclone.org/commands/rclone_copy/) and
[rclone check](https://rclone.org/commands/rclone_check/).
Custom code is restricted to historical Aave numerical adaptation, explicit
source/coverage/receipt checks, orchestration and research-metric definitions.
