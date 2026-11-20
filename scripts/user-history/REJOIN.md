# Four-hour supervision and data-machine rejoin

User decision on 2026-11-15: retain full-history, all-user end-of-every-block HF
outputs. User authorizes measured performance improvements within that scope,
four-hour node comparisons and research logs, and adding data back after its
planned manual startup. No downsampling or deletion is authorized.

The existing `aave-v3` thread automation is ACTIVE with a four-hour interval.
This changes the agent's supervision cadence, not the continuously running
workers, per-job node validation, or the website's progress refresh frequency.
Every real check appends `periodic_supervision` evidence to
`docs/data-collection/aave-v3/events.jsonl`; never invent missed checks.

## Retained research data

Keep every user HF and the other five account metrics, including collateral
value, debt value and effective liquidation threshold. Keep complete position,
price, collateral-enable, eMode, risk-configuration and interest-index histories
and version identities too. Reserve attribution needs those asset-level inputs;
the current account export alone is not a user-by-reserve position table.
No-debt HF remains uint256 maximum with a separate debt indicator.

## Verified worker IPC issue

Revert's pool stopped producing new receipts while its parent remained active.
Replacement children failed in `SemLock._rebuild` with `FileNotFoundError`.
The host's logind RemoveIPC setting is true; missing host semaphore names and
session-exit timing point to host IPC cleanup. A controlled dummy-semaphore
comparison reproduced the child failure in the shared host filesystem, while a
service-private tmpfs protected the object and the child exited successfully.

The current independent service now has drop-in
`/etc/systemd/system/dsc-aave-v3-hf-independent.service.d/20-private-shm.conf`:

```ini
[Service]
TemporaryFileSystem=/dev/shm:mode=1777,size=64M
```

Revert was restarted with this setting and its actual mount and new task row
growth were checked. Node/web have the same drop-in staged for the next start;
their running instances were not restarted, so they are not yet protected by
this setting. The deployment generator also includes it. No numerical source,
frozen image or calculation generation changed. No global logind setting or
legacy worker was changed. The original data-machine outage remains unexplained;
this is a separate, confirmed worker-startup failure.

Supervision must check worker progress and journal errors, not only parent
heartbeats or systemctl active. The reported active process count is outstanding
pool work, not proof every child started. Preserve real node validation on new
outputs after any restart.

Do not average account HF values and call that an official protocol or reserve
HF. Weighting, mixed-asset attribution and crisis sample rules are subsequent
research choices. Preserve the data needed to compare such definitions. Rounded
account HF must not be used to infer an exact unrounded collateral numerator.

## Backup checkpoint

`backup_hf.py` selects explicit accepted receipts, their immutable Parquet
outputs, validation reports and output sidecars, plus copied configuration/code
and progress metadata. It preserves each original receipt identity. Frozen
input images already have three independently verified copies; the checkpoint
does not duplicate their virtual environments or private worker credentials.

The first SSH tar relay was too slow and was interrupted; its partial files at
`hf-backups/20261116T0030Z/` are not verified backups. The selected path reuses
rsync directly over LAN. A short-lived source service publishes only hard links
to manifest-listed immutable files. It binds the source's LAN address on18880,
allows only the intended destination IP, requires a random task password and
is read-only. It has a3600-second lifetime limit and is stopped after transfer
in a finally block. No new SSH login authorization or private-key copying is
used. Passwords stay outside the published tree and are excluded from manifests;
the destination removes its temporary password file after use.

Python handles only receipt selection, path adaptation, temporary service
wiring and SHA-256 manifests; existing rsync performs copying/checksums. The
destination verifies every file before marking success. A source-side hard link
staging tree is not a second-machine backup. Do not modify those immutable
files through either link. The superseded generic tar relay was removed from
the maintained helper after the rsync path passed real cross-host verification.

Backup destinations, preserving paths relative to the source home:

| Source | Other machine | Stable archive root |
|---|---|---|
| node | revert | `/home/dsc-revert/hf-backups/lan-20261116/node` |
| revert | node | `/home/node/hf-backups/lan-20261116/revert` |
| web | node | `/home/node/hf-backups/lan-20261116/web` |

Each source checkpoint is `hf-failover-20261115/backup-checkpoints/<stamp>/`
with a manifest and immutable metadata copies. Later calls use a new stamp,
retain the stable archive root, hash-check existing files and transfer only
missing files. Conflicts stop the copy; they never overwrite a backup. Keep
50 GiB available on the destination. This is a recoverable project backup, not
an independent off-campus disaster-recovery copy.

The2026-11-16 supervision found a source file-list inactivity timeout under
concurrent production I/O. The sender had authenticated, but the old rsync
`--checksum` pass could reread already backed-up files before transferring data.
The maintained helper now passes the exact missing-file list through rsync's
`--files-from`/`--from0` and uses `--ignore-existing`; the preflight rejects
conflicting existing files and the final complete SHA verification is unchanged.
An interrupted checkpoint can be retried only with its original role/plan and
matching staged hardlink identities. Its immutable manifest is not regenerated.
Receiver failures now retain a sanitized diagnostic report beside the evidence.

From the project root, use a fresh UTC stamp on each actual check:

```sh
python3 scripts/user-history/backup_hf.py lan node revert <stamp> --evidence <new-evidence-directory>/node-backup.json
python3 scripts/user-history/backup_hf.py lan revert node <stamp> --evidence <new-evidence-directory>/revert-backup.json
python3 scripts/user-history/backup_hf.py lan web node <stamp> --evidence <new-evidence-directory>/web-backup.json
```

Only a successful destination verification and local evidence readback count as
backup completion. A prepared source manifest or an in-flight stream does not.
The original accepted files remain on their source machines. Incomplete backup
files remain unverified and must be inspected, not silently accepted or deleted.

## Rejoin sequence after data starts

1. Inspect data's boot ID, memory, free space, journal and existing HF service
   states before enabling anything. Investigate possible OOM evidence; the
   original outage's physical cause remains unconfirmed. Do not add all 32 cores
   before memory and shared-service load are measured.
2. Existing independent runners pause their own unfinished work when data LAN
   SSH returns. This is an intentional handover boundary, not lost accepted
   work. Preserve partial outputs and report the pause in the monitor/log.
3. Read data's tracker through its documented snapshot mechanism; enumerate
   three-host receipts explicitly from the recorded assignment and checkpoint
   manifests. Verify finalized cutoff/hash, frozen input identity and factor
   generation against FAILOVER.md. Never infer coverage from output globs.
4. Stage imported files under a separate recovery area on data. Verify original
   output SHA, sizes, Parquet row counts, owner universe, bounds and validation
   report identity using the existing `materialize_accounts.check_receipt` in a
   correctly mapped read-only validation environment. Receipt absolute paths
   refer to the original virtual namespace; retain receipts unchanged and store
   a separate location mapping. Recheck selected imported rows against node.
5. Reconcile old generation `b9f354...`, temporary generation `92e5c0...` and
   data's own accepted results by interval and user universe. Different physical
   Parquet grouping may produce different file SHA with identical values. For
   overlaps compare logical keys and all six values; do not equate differing
   hashes with numerical conflict or add overlapping row counts. A mismatch
   leaves the affected interval unaccepted pending investigation.
6. Publish a separate reconciled coverage manifest referencing original receipts
   and validation evidence. The old tracker cannot simply import arbitrary
   split-job IDs as its original grid, and must not be manually edited to fake
   acceptance. Preserve both ledgers; adapters must validate interval unions.
7. Build one new immutable assignment epoch for unfinished ranges, adding data
   only after measured CPU/memory limits and checking all legacy services for
   competing execution. One task has one owner. Keep calculation generation
   unchanged only if mathematics, source identity and physical-format rules
   remain identical; assignment identity must change. Never edit an active plan
   in place or restart the superseded per-user EVM main path.
8. Start the reconciled execution, validate the first fresh output against node,
   then update the monitor's expected assignment identities and resume four-hour
   supervision. Preserve all source and backup files until a separate retention
   decision. No application database or general pipeline role changes follow
   from this HF-specific handover.

These steps are authorized routine continuation after verification; a further
user choice is needed only for unresolved numerical conflicts or a genuinely
new scope. The earlier automatic rejection of stopping/disabling original
legacy workers remains binding: do not bypass it to force the handover. If a
competing legacy job cannot be resolved within existing authorization and tool
approval, retain the safe pause and report that specific blocker.

## Optimization queue

Reuse native integer batch evaluation, PyArrow, SQLite snapshots and existing
strict node acceptance. The current run already uses larger row groups, bounded
job sizes and process recycling. Next locate costs on the same real historical
jobs: initialization/replayed prefix, per-block factors, account arithmetic,
Python row construction, Parquet compression and validation. Later ranges may
have different costs; do not extrapolate early empty-market speed.

Prioritize shared per-block price/normalization work, smaller repeated input
loads via verified local checkpoints, and higher-level Arrow batch construction
if profiling supports them. Measure both wall time and peak RSS and compare all
six values, block/user keys and zero-debt behavior. Include all historical rule
versions plus eMode/GHO cases before full deployment. Keep separate candidate
output/generation identities; never replace an active mathematics module under
running jobs. Record baseline/candidate results, source hashes and rollback
paths; retain only measured improvements without changing coverage.


## Cached integer kernel rollout — 2026-11-16T06:43:43.586231+00:00

Current active generation `d0d4f91ef23bc9dc5a91c24b4fdf8d12a1e64d5e6fb2d9f0df4d6a532c556c9e`, plan SHA `cde26d58c9549a98fe4a15b09c320e43f0e2c820a1b4c8a664af4a2736d83e6d`. Read each host's `hf-failover-20261115/active-plan.json`, verify its plan hash, and use `rollouts/<generation>/code` rather than the preserved base/code. The same independent services run node14/revert8/web6; resource limits and the data-return reconciliation guard remain. All three now activate private shm. Startup revalidates recovered file hashes before publishing new-generation progress; a stale previous-generation status is not live progress.

The new plan preserves 1368 accepted intervals / 12783040547 rows with their original receipts and identities, and assigns 90926 remaining intervals without changing the 910310934885 logical user-block rows. The accepted web canary 18187373–18188065 adds 9995664 current-generation rows and is reused normally. Its measured 155.37s versus roughly 291s for adjacent old jobs supports adoption, but is not a measured whole-run speed multiplier.

The kernel reuses Python functools.lru_cache for exact shared price/index inputs; only bounded preparation and reserve-presence validation changed. Formula/rounding and all six metrics are retained. Numeric reference checks covered 220365 user-block observations and all 12 profiles; bitmask equivalence covered 4608 cases. New-production canary passed ordinary node acceptance. Old source/plan/output files are preserved.

Backups for checkpoint 20261116T042153Z are verified on separate machines; see evidence *-backup.json. Backup helpers now honor active-plan and original-generation receipt locations. No application DB migration is implied. Evidence: docs/data-collection/aave-v3/evidence/supervision-20261116T042153Z.


## Capacity forecast at 2026-11-16T08:17Z

All three new-generation producers are generating accepted output; 17.63B rows were accepted (~1.94%). Fresh node comparisons (9 accounts,6 balances,6 normalized indices;111 provider calls) were exact. Since-restart throughput including startup hashing was ~838k rows/s; the resulting ~12.3-day compute-only extrapolation is conditional, not a completion promise.

Recent physical outputs are about24.6–24.7 bytes/user-block. At that ratio, remaining assigned Parquet is node5.25TB/revert9.42TB/web7.36TB. Node has space for its own current assignment, but revert/web do not; full duplicated backups also need space. Current web/revert space above the50GiB guard was174GB/446GB, roughly6.7h/14.1h at observed rates before extra backup writes. Sizes and rates may change; these are forecasts, not exact halt times. No deletion or downsampling was performed. Prioritize lossless layout/compression or additional validated storage placement before the guards pause future work. See supervision-20261116T081725Z/capacity-forecast.json.


## Lossless storage pilot — 2026-11-16

User explicitly requested lossless trials before precision reduction. `benchmark_lossless.py` uses existing PyArrow24, no new dependency. All27 candidate roundtrips across2,870,576 distinct sample rows restored all14 original fields exactly, including uint256MAX, no-debt flags, profile and block identity. Ordinary user-then-block sorted Parquet/Zstd9 won all3 windows: pre-SVB12.57->9.14MB(27.3%), block19233062 window40.63->8.29MB(79.6%), late25200000–25200003 window28.91->10.71MB(62.9%). Default/level1 sorting was faster with slightly larger files; level9 alone without sorting increased all3 samples. Normalization/run/binary packages include dictionary metadata bytes and did not beat the simple sorted layout.

Each sample is at most1M rows; late spans only4 blocks, not a full10M-row job. Single-block queries became slower, while whole-table reads were comparable or faster in warm-cache single-process probes. PeakRSS for the complete trial was2,158,800KiB with4GiB cap; do not multiply this per job across28 workers without an external/bounded-sort resource design. No full-history storage forecast is justified yet. The original producer/receipts/output files and all precision remain unchanged.

The new4-block late sample was computed using existing native kernels and ordinary own-node acceptance:6getter samples matched. Its generation starts`lossless-source-` and is excluded from production coverage. A wrapper failed after saving its accepted source receipt, so the provider-call counter was not persisted; report it unknown, not zero. Recovery checked existing receipt/output/validation hashes and made no repeat RPC. Two NumPy import failures were resolved using native Arrow grouping/run-end APIs rather than installing NumPy. Complete evidence, source hashes,27case size/timing results and interpretation: `docs/data-collection/aave-v3/evidence/lossless-storage-20261116/summary.json`. All trial artifacts remain under revert:/home/dsc-revert/hf-lossless-20261116 and its explicitly named source output; no production switch or deletion occurred.
