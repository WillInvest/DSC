# Independent HF compute and verified storage flow

This operational release supersedes the old f64 startup audit and shared numerical
scheduler. Scientific generation, scope, numerical code and the USD0.01 rule in
`FLOAT64-ROLLOUT.md` are unchanged. The old exact producer stays paused.

## Authority and independence

Data owns `/var/lib/dsc-aave-hf-flow/20261116/flow.sqlite` and the immutable
`numeric-plan.json`. It assigns disjoint complete jobs to four durable local
queues. Each producer uses the original `hf_f64_compute.compute` and
`hf_f64_accept.accept`, then journals acceptance once. A restart loads the local
SQLite done IDs; it does not decode or recompute previously accepted Parquets.
A task interrupted before journal commit is individually reconciled.

Each host independently runs `dsc-aave-v3-hf-flow-local.service`; multiprocessing
spawn isolates per-job caches. NumPy threads are1 per process. Dask TLS is used
only for small catalog snapshots, transfer and archive control. A slow transfer
or unavailable peer does not gate another host's numerical queue. Data outage
leaves local computation running until the explicit disk high-water safeguard.
Data remains the sole acceptance/dispatch authority when it returns.

| Role | Flow root | Configured compute processes |
|---|---|---:|
| Data | `/var/lib/dsc-aave-hf-flow/20261116` |20|
| node | `/home/node/hf-flow-20261116` |20|
| revert | `/home/dsc-revert/hf-flow-20261116` |20|
| compute | `/home/fao/dsc/data/incoming/aave-hf-flow-20261116` |20|

These are process slots, not a claim that all128 hardware threads are saturated.
The first production run measured12 helper processes and20 node processes;
helpers had memory/CPU headroom. The operational rebalance uses relative
remaining-work weights Data3/node1/revert3/compute3, keeping source-affinity jobs
and every completed job's owner. The frozen scientific plan's original worker
weights are not rewritten. Before rebalance,512/204/452/438 local jobs respectively
were preserved;1755 prior canonical accepted tasks were imported metadata-only.
The resulting assigned local task counts are27190/9100/27122/27127, totaling90539;
plus1755 previously accepted tasks equals all92294 tasks.

## Movement and retirement

1. Each helper retains closed accepted results until approximately125GB (decimal)
   or its final partial batch. It transfers explicit receipt-listed files to node
   over the existing encrypted channel. Node independently verifies full SHA256.
2. Only after this proof, a root-owned manifest-limited helper removes those
   redundant source Parquets. It preserves SQLite, input snapshots, metadata,
   receipts, recovery maps and active jobs. The helper normally stops dispatch at
   a150GB local buffered high water and retains50GiB free plus in-flight allowance.
3. Node accumulates about500GB of accepted results, or the final partial batch.
   Explicit files are packed as approximately2GB uncompressed TAR chunks; the
   existing Parquet/Zstd vector bytes and precision are unchanged. Packing reads
   every member back and verifies its SHA and real byte-range offset.
4. Rclone uploads immutable chunks, manifest and index, checks every remote
   object's size and MD5, and reads back user, reserve/protocol and dictionary
   members with SHA256. It also SHA-checks one entire TAR from cloud.
5. After proof, the same root-owned helper retires the accepted node Parquets and
   TAR staging files. The small local manifest, pack index, checksums, receipts,
   SQLite and cleanup journals remain. Node frozen inputs and unrelated data are
   never deleted. A cloud-authoritative job can be restored by explicit indexed
   byte ranges and independently SHA-checked, including for future node audits.

Packing currently needs one extra batch's scratch space (roughly500GB) on node.
Packing and transfer share an exclusive capacity lock; packing publishes its
full scratch reservation so node numerical dispatch cannot spend that budget.
Node had about10TB available at the release check. If free capacity falls below
reserve, numerical dispatch waits for verified retirement rather than deleting
unverified files. Batches are prepared durably in Data's ledger; interrupted
copy/upload/cleanup resumes the same job and checksum scope.

Reader/shared and retirement/exclusive per-job locks prevent an audit from losing
its next file to cloud cleanup. Data records a monotonic node-acceptance sequence transactionally, distinct from
local observation order. Supervision cursors follow this sequence, sampling each
role's new accepted files even when transfers arrive out of order.
Root retirement additionally checks identities,
open file descriptors, hard links and a closed destination proof before unlinking.
No wildcard or wholesale server cleanup is part of this release.

## Services, monitor and supervision

Data: `dsc-aave-v3-hf-flow.service` controls catalog/transfer/archive tasks.
All hosts: `dsc-aave-v3-hf-flow-io.service` provides independent control transport.
Node: `dsc-aave-v3-hf-flow-cloud.service` uploads directly to Drive.
The deployed adapter directory is `code-v5` under each flow root.
`role.json` pins the local assignment and operational adapter hashes.

New public status lives only in Data's flow-root `public/`. Existing read-only
status servers18897–18901 read this directory; the old f64 producers/uploaders
were not disabled to obtain these endpoints. The monitor is
https://dsc-compute.tail5aad6c.ts.net/ and distinguishes local computation, node
acceptance and cloud verification/cleanup. Counts are catalog-driven and do not
sum the offline/legacy trackers a second time.

Existing four-hour supervision checks new outputs, actual process/memory/disk
usage, local/node/cloud transitions and unexpected holds. New numerical output
keeps original per-job own-node acceptance samples. Extra supervision calls use
`audit_hf_flow.py --config <flow-root>/flow-config.json --rpc-budget200`; actual
provider methods are counted, and no-new-output checks make zero audit RPCs.
`backup_hf_flow.py` uses SQLite online backup and verifies a separate node copy of
control metadata/code. Research evidence is append-only in
`docs/data-collection/aave-v3/events.jsonl`.

## Release evidence and limitations

- Four real host canaries:39962317 logical user/block HF values,70748362 Parquet
  bytes. All four original acceptors passed. Restart startup was0.09–0.52seconds,
  compared with the old whole-population startup audit's1800-second timeout.
- Small raw cloud pilot:97085103 verified bytes, all three data products/dictionary
  restored, three helper copies and node Parquets retired. This is not a125GB or
  500GB batch. The subsequent packed pilot is recorded separately.
- Fresh cloud restore and own-node audit:24 samples across four accepted receipts,
  99 actual extra provider methods, max absolute HF error2.220446049250313e-16;
  structural and numerical budgets passed. Normal acceptance RPCs are separate.
- The final733 missing legacy revert archive files passed independent node SHA
  verification at2026-11-17T03:21:18Z. Their consolidation is distinct from the new
  float64 cloud batches; copying old sources does not count as cloud upload.
- Automatic approval rejected disabling/stopping the old f64 uploader/worker
  services. They were left unchanged. New catalog/public paths are isolated.

All92294 numerical tasks and910310934885 values remain the historical target;
release validation is not completion of that full history. Actual large-batch
throughput/Drive quota will be observed and logged as thresholds are reached.

- Packed live pilot:43769012 verified cloud bytes, all11Parquet files restored
  through the pinned range index and SHA-checked, source Parquets/TARs and the
  restored cache retired. ReportSHA
  `5c2735d89c448951c6953785cb1606168381d012be317f9b8b1f266f96d043d6`.
  It creates no new logical HF coverage and uses zero additional node RPCs.
- The dedicated code review completed as `20261116-hf-flow-475cb027`. Two retained
  findings were fixed with red-before/green-after tests: actual node-acceptance
  ordering for delayed transfers, and a shared disk reservation for archive,
  transfer and numerical dispatch. The46flow tests include these regressions;
  Mac skips3Linux-root-only checks, which must also run on the server.
- Existing four-hour automation `aave-v3` was updated in place through the app
  action, preserving the schedule. Old f64 unit retirement was rejected by
  automatic approval review; those units were left unchanged. Independent public
  status paths avoid competing with the preserved units.

Reused APIs:SQLite WAL transactions and online backup, multiprocessing.Pool,
NumPy/PyArrow and the unchanged official-source-derived numerical acceptor,
Dask TLS/fsspec bulk transfer, Python tarfile and rclone copy/check/cat. Custom
code is limited to DSC task/receipt adaptation, operational state, explicit
retirement policy and pinned byte-range recovery. The root-owned retirement
policy cannot delete inputs, metadata, databases or arbitrary server directories.

## 2026-11-17 large-batch recovery (in progress)

The first full revert batch `87631df9d7c40ebdbb18efdf` contains8619jobs,
94317result files and125002399680bytes. Node's full SHA proof was accepted,
but source cleanup failed before unlinking:8619simultaneous job locks exceeded
the root helper's soft descriptor limit1024. `job_locks` now raises only the soft
RLIMIT_NOFILE within the unchanged hard limit, holds every lock, then restores
the previous limit. A real8619-lock read-only probe and47Linux/root tests passed.
The fixed root helper is deployed on all four hosts. Manifest-only retirement is
running on revert under `dsc-hf-flow-source-recovery-20261117.service`; do not
claim this source batch retired until its `recovery/<batch>/complete.json` and
`local/transfers/<batch>/retired.json` exist. Keep its original node proof.

A supervision backup grew above1GB and embedding it into a Dask task graph
OOM-killed the2GiB scheduler. Numerical producers remained separate. Scheduler
restarted; the new flow controller was restarted with an on-failure recovery
unit drop-in. The scheduler now has a persistent code-v5 PYTHONPATH drop-in and
its current process was updated without another restart. Protected legacy units
were not stopped/disabled. No numeric generation or plan changed.

`backup_hf_flow.py` now uses the same SQLite online backup and maintained rsync
through existing Data→node LAN SSH; every destination file is SHA-read back.
No database bytes enter a scheduler graph. Real1074390161-byte/24-file backup
passed. Never rerun `supervision-busy-io.py`'s old whole-database submit path.
The numerical portion of that supervision passed12samples/51actual node methods,
max absolute HF error2.220446049250313e-16 before backup failed.

At13:54Z monitor was live,40,187jobs/396,216,894,865values accepted. Node had20
active numerical processes; three helpers were at their150GB buffer safeguard
waiting for transfer/retirement. This is not yet restored80-process operation.
Node had223.31GB pending cloud bytes; only154.84MB cloud verification from prior
pilots is recorded, not a completed500GB production batch. Transfer throughput
still needs improvement and production-sized cloud retirement remains to verify.

### Grouped retirement and bulk transport follow-up

First production batch125002399680bytes/94317files is now source-retired and
Data's transfer operation is complete. The interrupted old journal had41680
files already absent; the grouped resume removed the other52637files, with
61040180762bytes remaining, in100.90seconds including full preflight. Revert
resumed20numerical processes automatically. Preserve both the append-only
unlink journal and `recovery/87631df9d7c40ebdbb18efdf/complete.json`; a later
idempotent `retired.json` correctly reports94317already-absent files.

Durability grouping:128explicit files per journal intent group. All intents are
flushed/fsynced before the first unlink; every affected directory is fsynced
before a completion group is flushed/fsynced. Existing full SHA/identity/open-FD
checks and all job locks remain. Isolated512-file test on revert measured
28.873s→0.308s, not a full-batch throughput claim. Linux/root regression tests
cover failed intent sync and interruption inside a group.

Optional `hf_flow_rsync.py` is now enabled for the three source helpers.
Maintained rsync moves an explicit null-separated accepted file list over SSH
into per-role/per-batch node scratch. Source shared locks stay held while
copying. Node retains original SHA, canonical receipt, independent destination
proof and capacity-lock acceptance. Staging has no logical coverage of its own.
Data/compute use dedicated private keys kept only on their own servers, with
node authorized_keys forced to `rrsync -wo -no-del -munge`, restricted source LAN
IP and dedicated incoming directory. No shell access is granted. Revert reuses
its pre-existing node SSH route. Do not copy private keys into operator workspace.
Node's public host identity is pinned on the helpers. Separate real accepted-job
canary passed all bytes in5.998s and its idempotent reentry in0.058s. A128MiB test
transfer took2.610s; these are bounded canaries, not125GB production measurements.

A second scheduler OOM exposed large per-batch dictionary task arguments. The
controller now reuses Dask `Client.scatter(direct=True, hash=False)` for full
copy/cleanup/cloud proofs and gathers large results directly from workers.
Task graphs contain Future references; payload references stay alive until their
task resolves. New network IO units alone now useRestart=always/RestartSec=10
because a scheduler disconnect can cause a clean worker exit. Numerical producers
and protected legacy units were not restarted/disabled by this update. The53
Linux/root flow tests, including direct-scatter and rsync-scope regressions,
passed. Production numerical generation and frozen plan SHA remain unchanged.
Transfer errors are now separate from transient catalog-read errors so a healthy
catalog poll cannot erase a failed transfer from the monitor.

Checkpoint14:17Z:40,713jobs/401,409,033,960values accepted; node and revert each
20active processes, Data/compute waiting on their own150GB buffers. Monitor is
healthy. Node has224.73GB accepted pending cloud, below500GB; cloud verification
is still154.84MB of prior pilots. Normal500GB cloud lifecycle remains unproven.
Production rsync receiver handles1402282/1402417 were live after this checkpoint.
A previous controller client's still-live copy process1386241 owns node capacity;
its IO counters progressed through canonical-file hash checks and then started
rsync. Do not kill/restart that work on observation timeout: current controller
probes wait behind it and will recover its immutable `verified.json` when done.

## 2026-11-17 additional delivery recovery and Data outage

The full58Linux/root flow suite passed after known transfer-alias recovery,
fresh full-SHA certificate aggregation and bounded archive-fill scheduling.
Existing canonical copies can have `.transferring`/incoming aliases if a process
dies after publication; only named, SHA-verified aliases are removed, while
unknown hardlinks/conflicting bytes remain and are rejected by strict sealing.
Node now seals every job's Parquets once and combines those fresh independent
seals into an exact full-batch destination proof. Source manifest equality and
final identity checks remain; accepted numerical computation is not repeated.

`archive_fill_transfers=true` preserves ordinary125GB batches and disk reserves,
but can transfer already accepted tails when projected node holdings are within
125GB of500GB and no archive is running. Committed transfer bytes are not counted
twice; quarter-batch minimum avoids tiny trickles. This prepared the exact
revert operation `1123a3670a90e9df96c88bc4`,2151jobs/24268595444bytes.

Data became unreachable after14:43:26Z: Tailscale reports offline; both node and
compute get LAN no-route errors. Its cause is unknown until its system journal
can be read. No saved hardware MAC was found for targeted Wake-on-LAN; the user
was asked to check power/network. Do not infer an OOM or restart a live peer job
from an observation timeout. Node/revert's independent numerical services remain
running. The old Data rsync copy process1386241 terminated; its partial incoming
files are unverified and preserved, not counted as node acceptance.

A bounded scheduler-free transport path is staged only under node's
`recovery-offline/`; production numerical modules/generation remain unchanged.
`pre_staged=true` runs the same canonical file checks/full independent node SHA
and standard `transfers/<id>/verified.json` publication without contacting the
Data scheduler. Seven node transport tests passed, including corrupt/missing
input rejection and no scheduler import/access. It is not a new acceptance DB.

Only original Data-authorized scopes are being advanced. The immutable online
backup at node `control-backups/1794924560924572584` passed its full manifest SHA.
Its exact compute pending operation is `4abe26a99153a4069281beca`,8654jobs. The
2151-row revert backup prefix hashes to the exact last authoritative Data digest
`1123a3670a90e9df96c88bc4`; no guessed scope is allowed. Authorities are under
node `offline-copy/<id>/authority.json`. Compute's authority was copied via the
operator as metadata only; Parquet data travels directly over restricted LAN SSH.
Central Data catalog finalization stays explicitly pending; never report it done.

Live jobs: node `dsc-hf-offline-copy-web-20261117.service` PID1444173 and compute
`dsc-hf-offline-send-web-20261117.service` PID1398917. At15:40Z real receiver
PID1444877 had written about76.1GB, still partial transport. Node's queued revert
receiver `dsc-hf-offline-copy-revert-20261117.service` PID1447519 waits for the
same capacity lock; its sender has NOT started. Start that sender only when its
progress phase is `ready_for_source_staging`. Complete a source retirement only
after its exact standard node proof exists and passes the original authority pins.
Source metadata, inputs, receipts and catalogs must remain. Keep all partial Data
copy files for its eventual original batch resume.

The monitor previously cached old40-process activity indefinitely when all Data
endpoints failed. API response aging now preserves accepted totals but marks
expired activity unknown/stale and shows `waiting_for_inputs`; eight monitor
regressions and JS syntax passed. Actual API verified408598323590last-accepted
values preserved, active=null, previous_active=40, all cached producers stale.
This is truthful unknown activity, not a claim live node/revert stopped. Monitor
rollback is `/home/dsc-web/sequential-events/goal-backup-1794930075760051018`.

Remaining goal gates: source copy/full SHA/retirement completion, compute producer
resume, original revert tail, Data recovery/catalog reconciliation, a real500GB
Drive verification and node/TAR retirement, then fresh own-node numeric audit,
consistent control backup and final monitor/research-log audit. Do not mark the
goal complete or mark full historical production complete from code tests/pilots.

### Publication durability checkpoint (2026-11-17 16:19Z)

Compute batch `4abe26a99153a4069281beca` completed rsync transport, but its
94,664 Parquet files are not yet accepted on node. Source retirement has not run.
The exact source authority, original prepared operation and scientific generation
remain unchanged. Data is still offline; no substitute central catalog was created.

Node receiver `dsc-hf-offline-copy-web-20261117.service` was resumed as PID1457928
with the candidate in `recovery-offline/hf_flow_io.py`. This candidate defers
per-file publication fsync and canonical receipt fsync until a barrier over only
the declared closed files and touched directories. GNU `sync` receives explicit
paths (no `--file-system`). Full file SHA, stable identities and the final durable
proof remain mandatory. `shared.artifacts.write_json` adds an explicit
`durable=False` option; all existing callers keep the original durable default.
Node's operational root helper was backed up to
`recovery-offline/shared-artifacts-before-batch.py`; code-v5 and frozen numerical
images have not been redeployed. Linux transport regressions:9 passed; two new
barrier regressions failed on the previous implementation first. Existing durable
JSON still issues both file and directory fsync; deferred JSON issues neither.

Whole-filesystem sync was experimentally rejected: it waited for unrelated
writeback. SIGKILL was sent only to the isolated experiment PIDs1455357/1456209;
at the checkpoint both remained in uninterruptible `wb_wait_for_completion` until
kernel IO returns. Never repeat this experiment on the node. A fresh-file16-file
microbenchmark did not demonstrate a speed win for scoped serial fsync (6.14s
original vs12.42s grouped), so no throughput claim is made. A separate pre-durable
input ordering probe measured8.98s after-link fsync vs5.91s before-link fsync;
that ordering variant is not deployed. Real full-batch completion is the gate.

The bounded `dsc-hf-offline-audit-web-20261117.service` (PID1452856) waits for the
full node proof, then reuses the own-node validator for first/middle/last jobs,
up to18 samples and200 RPC. It has made0 RPC so far. Compute's prepared
`offline-retire.py` requires the matching full node proof and passing numeric
audit before invoking the existing exact manifest retirement helper.

Revert's queued receiver was recreated as PID1458137 for the original2151-job,
24,268,595,444-byte scope `1123a3670a90e9df96c88bc4`; its sender is not started.
Wait for `ready_for_source_staging` before starting that sender. Node and revert
still have20 active numerical jobs each; compute remains at its150GB buffer
watermark; Data is unavailable. Monitor correctly marks cached activity unknown.
No production500GB Drive cycle has completed. Evidence and append-only event:
`docs/data-collection/aave-v3/evidence/flow-publication-checkpoint-20261117T161931Z`.

### Independent receiving disk and real-node audit (2026-11-17 17:24Z)

Evidence: `docs/data-collection/aave-v3/evidence/flow-storage-isolation-20261117T172402Z`.
Node's existing `/dev/vdb1` had about4.6TiB free and no measured IO while vda
served RETH and other writers. Created only the private Aave directory
`/mnt/storage/dsc-aave-hf-flow-20261116`, bind-mounted at
`/home/node/hf-flow-20261116/storage`. Existing admin directory permissions and
all existing files remain unchanged. Main mount and the web/revert receiving
bind mounts are enabled for boot. New-flow IO/cloud units have mount dependencies;
protected legacy units and numerical producers were not stopped.

The same16-file durable-write benchmark measured6.14s on the busy root disk and
0.249s on the isolated directory; these are small-file measurements, not a full
pipeline speed claim. The first root-to-vdb copy retained original staging but
still read through the busy root disk. Receiving paths were therefore bound
directly to the new disk. Every old web/revert staging directory was preserved
under the exact prefixes recorded in node's
`recovery-offline/staging-relocation-20261117.json`. In particular the old full
web staging is `/home/node/hf-flow-20261116/rsync-incoming-web-vda-20261117`.
Do not retire old staging or old canonical fragments without new independent
byte proof and the usual exact manifest/identity/root-FD gates. Only the exact
current web scope was added to the old-staging cleanup allowlist.

The unchanged8654-job compute scope was received directly on the new disk,
approximately110MB/s observed. Full node acceptance is still pending. The
candidate also supports cross-device `shutil.copyfile`, retains such staging,
and rejects missing data/staging mounts before publication. Linux does not allow
hardlinks across different bind mounts even with matching st_dev. This caused
an explicit preserved failure; tests reproduced it. Node now verifies samefile
between logical receiving roots and physical staging sources, then publishes
through the single main storage mount. EXDEV falls back to high-level copy with
staging retained. Logical SSH receiving paths and credential restrictions did
not change. Source generation/numerical inputs remain pinned and unchanged.

A10-second wall-clock syscall profile found5 alias-recovery fsync calls totaling
11.87 thread-seconds (concurrent sum). Known-alias recovery now supports deferred
durability only under the batch's mandatory declared-file/directory barrier;
standalone default durability remains. This closes a second per-file journal
commit path.16 Linux regressions pass, including the real bind mount, wrong
physical-alias rejection, EXDEV, corrupt/missing data, strict alias handling and
failed-barrier refusal. The candidate is only in node recovery-offline; normal
code-v5/frozen images were not redeployed. Before Data authority resumes normal
routing, reconcile its node paths and verify the new shared JSON API is present
in the normal IO environment; do not assume candidate deployment is global.

Fresh own-node audit: node local ledger sequences8008,8007,8006, receipts created
at17:00:54–17:01:31Z.18 HF samples passed using75 actual RPC
(37 eth_chainId,20 eth_getBlockByNumber,18 eth_call); maximum absolute error
2.220446049250313e-16. Report:
`/home/node/hf-flow-20261116/offline-copy/own-source-audit-1794934909479050918.json`.
This is fresh locally accepted source validation, not the foreign web transfer
proof or central catalog acceptance. Data's audit marker was not advanced.

At17:24Z node/revert each had20 active numerical jobs; compute remained at its
150GB buffer watermark. Data was still unreachable. Active web receiver is
PID1487649; node proof and its waiting own-node audit must finish before compute
retirement/resume. Revert's next2151-job/24.27GB receiver was stopped during the
repair and is NOT currently live; start it only after web has finished, then
start its sender once ready. Never infer liveness from its old progress JSON.
Normal500GB Drive production verification/retirement and four-host recovery are
still unproven. Monitor correctly shows stale authority while Data is offline.

### Completed offline source lifecycles and remaining external gate (2026-11-17 18:06Z)

Evidence: `docs/data-collection/aave-v3/evidence/flow-available-scope-audit-20261117T180647Z`.
Compute scope `4abe26a99153a4069281beca`: full node proof SHA
`91685cab5430d9a4df3af3986095d5a830554fb9e5e814d1fdad8429d19b5f91`,
8654 jobs/94664 files,125001733311 bytes.17 own-node HF samples passed using71
actual RPC, maximum sampled error0. Exact source retirement completed at17:52Z;
94664 files removed, no already-absent files, local buffer fell150.20GB→25.20GB,
and compute resumed20 numerical jobs. New node files remain intact awaiting cloud.

Revert scope `1123a3670a90e9df96c88bc4`: full node proof SHA
`31e5ce82790a0f83a77ae33a7b63189ef443ffed35155dded436da8af902682c`,
2151 jobs/23630 files,24268595444 bytes.18 own-node HF samples passed using75
actual RPC, maximum error1.1102230246251565e-16. Exact source retirement completed
at17:58Z, local buffer fell~76.19GB→52.02GB; its20 numerical jobs continued.
Combined source retirement:118294 files /149270328755 bytes. Including the fresh
node-local source audit, this recovery used221 own-node RPC for53 HF samples;
all passed, largest absolute error2.220446049250313e-16. No central audit marker
or authoritative coverage was advanced while Data was offline.

The tested normal node IO adapter is now promoted to code-v5:
old SHA0795b285cb755216b09bdb3d3ea34270358787eecd4238039a13688d8fcb98a6,
new SHA4a5361de32eafd76be0b712d24a2b7968f6a9a346b94e3bf9881183aef01fea9.
New IO/cloud units use the operational shared-artifact helper through
`70-shared-artifacts.conf`; pkgutil.extend_path preserves other pinned shared
modules.16 tests passed inside the actual frozen runtime. Numeric imports and
all frozen-plan/adapter pins validated; frozen images and mathematical generation
were not edited. The16-case test fixture uses a1MiB copy reserve because the
runtime's private tmpfs is small; production defaults remain4GiB at both
preflight and per-file guards. Data's central config has NOT yet been updated.

The private monitor now shows dated, generation-matched recovery milestones
without changing cached authority counts, fetched_at or unknown live-activity
flags.9 monitor tests and JS syntax passed; HTTPS was verified from the user's
Mac. Node-to-monitor HTTPS timed out, while Mac access, local API and Serve
listeners worked; no ACL/firewall widening was attempted. Rollback is
`/home/dsc-web/sequential-events/goal-recovery-notice-backup-1794938016570537646`.
Cached central count remains408598323590. This is not new accepted coverage.

Data has remained unreachable since14:43:26Z over several consecutive goal
turns; LAN/Tailscale probes still fail. Cause is unknown without its OS journal;
no usable wake MAC/out-of-band control was found. The three reachable numerical
services are running20 jobs each. Prepared, unapplied central patch:
`/home/node/hf-flow-20261116/recovery-offline/data-rejoin-storage-patch.json`.
Inspect live Data authority and pending scopes on return; do not overwrite its
SQLite with the old backup. Reconcile the two offline proofs/source retirements,
then update global node_bulk plus roles.node storage mappings/operational pin.

The full goal is NOT complete: four-host operation, production approximately
500GB Drive checksum/readback/retirement and final consistent authority backup
remain unverified. Only154838854 prior pilot cloud bytes are verified. Do not
call node consolidation or these source retirements a completed500GB cloud cycle.
All old staging, Data's interrupted transfer, inputs, receipts and recovery maps
remain preserved. No protected legacy units were stopped or disabled.


## 2026-11-17 temporary coordinator moved to Compute

Latest user explicitly selected dsc-compute because Data remains offline.
HF authority now lives at `/home/fao/hf-flow-control-20261117` on compute,
with TLS scheduler `10.246.103.143:18896`; Data ledger/assignments preserved.
See [COMPUTE-COORDINATOR.md](COMPUTE-COORDINATOR.md) for services, backup,
reconciliation and rejoin rules. This supersedes the Data-authority operational
notes above for this HF flow only. Numeric generation and scope are unchanged.

Three transfer proofs were reconciled and accepted journals caught up without
numerical replay.11 controller regressions passed; fresh18-sample75-RPC audit
passed (max HF abs error2.22e-16); two independent control backups were fully
SHA-verified on node. Private HTTPS monitor confirmed compute coordination and
40 active numerical processes, with Data explicitly missing. Two new helper
transfer operations were in copy stage at the final evidence snapshot.
500GB production cloud verification remains outstanding; do not count pending
node bytes as cloud coverage. Four-hour supervision now follows compute.

Evidence: `docs/data-collection/aave-v3/evidence/compute-coordinator-20261117T184926Z`. Bounded independent source review found no material
issues and independently passed all11 local controller tests. Deployment scripts
are preserved under `deployment/compute-coordinator-20261117/`. No protected
legacy units or numerical producers were stopped or restarted.


## 2026-11-18 02:16UTC supervision

Compute authority continues without operational errors. Revert completed all its
27122 local tasks, all accepted results reached node and its current unretired
source buffer is0; its terminal producer JSON contains an older buffer reading.
Node has50559 accepted jobs,20277 more than the preceding check. About708.46B
logical user values are locally accepted; Data remains offline, so this is not
full-history completion.

First production~500GB cloud operation95b56cee0bd19fa0d445463a started and is
still building verified TAR chunks (~138GB scratch at check). No new production
cloud acceptance/retirement has been claimed;154838854 old pilot bytes remain
the verified cloud total. Node cloud memory charge was mostly file cache
(~29.5GB vs~3.4GB anon), oom/oom_kill0; no memory-limit changes were needed.

Fresh own-node audit checked12 samples with51 actual RPC. Structural/model
checks passed, sampled max/p99 absolute and relative HF errors0. A new30-file
2.291GB consistent control snapshot passed independent node SHA verification.
Private HTTPS monitor is fresh and reports20 active numerical processes on
Compute; Node and Revert queues are terminal, not failed. No code changes.
Evidence: docs/data-collection/aave-v3/evidence/periodic-flow-20261118T021551Z.

## 2026-11-18 06:06UTC supervision

Compute's 27,127 local assignments are now complete; Node's 9,100 and Revert's
27,122 were already complete. These terminal queues are not failed producers.
Catalog totals including preserved prior-generation imports are 748,994,584,625
logical values (82.28%); 16,349 Data-assigned tasks remain unaccounted while Data
is offline. Do not reassign its potentially completed, unreconciled work or
publish a helper-throughput estimate as a full-history ETA.

No new node-accepted receipts arrived since the preceding supervision: the extra
audit correctly used zero RPC. Two newly produced Compute normal validation
receipts contain 12 own-node HF samples and 54 normal RPC; maximum absolute
error 2.22e-16, within budget. Those are normal acceptance evidence, not a new
extra node audit. The new 30-file 2.379GB consistent control backup passed
independent node SHA verification.

The first production cloud batch is still packing (220 closed TARs, about
440.7GB scratch at inspection), with no completed production cloud proof or
retirement. All observed OOM counters remain zero. Current source buffers are
Compute 115.68GB, Node 95.45GB and Revert zero; terminal progress JSON can retain
older buffer readings. The separately published 495MB SVB sample adds no
production coverage. Evidence:
`docs/data-collection/aave-v3/evidence/periodic-flow-20261118T060627Z/`.

## 2026-11-18 10:14UTC supervision

Compute's final 115.68GB batch `f6a222cb999019921d03143c` passed independent
node SHA verification and exact source retirement (159,643 result files).
Node-accepted jobs increased by 14,546 to 65,105. Current Compute and Revert
unretired source buffers are zero; their terminal JSON retains historical sizes.
Overall calculation remains 82.28%, with Data still offline and 16,349 assigned
tasks unaccounted. No completed-task ownership or numerical scope was changed.

The first production cloud batch finished packing 251 TARs (~502.7GB) and is
now actually uploading. Approximately 62MB/s was observed; shared Drive-project
quota errors are retrying. Rclone's cumulative bytes include retries and are not
unique uploaded coverage. No full-batch cloud proof or node retirement exists
yet; verified production-cloud ledger bytes remain 154,838,854 from pilots.

Fresh own-node audit: six HF samples, 27 actual RPC, structural/numerical checks
passed; maximum absolute error 2.22e-16 and p99 absolute error 2.18e-16.
The new 30-file 2.618GB consistent control backup independently passed node SHA
verification. No OOM observed; no production code/service changes were needed.
Evidence: `docs/data-collection/aave-v3/evidence/periodic-flow-20261118T101359Z/`.

## 2026-11-18 14:11UTC supervision

The first production cloud batch finished copying all 253 objects on retry2 at
13:16UTC and is now in its size/MD5 check (178 objects checked at observation).
Subsequent SHA probes and verified retirement remain pending. Node originals
are preserved; production cloud acceptance is still the prior pilot total.
There are no new numerical or node-accepted outputs: scope remains 82.28%,
65,105 node-accepted jobs and 16,349 unaccounted Data assignments; Data SSH
still times out. The scheduled audit used zero extra RPC. Code/config identities
are unchanged, so the existing independently verified control backup was retained
without repeating it. No OOM observed and no production services changed.
Evidence: `docs/data-collection/aave-v3/evidence/periodic-flow-20261118T141102Z/`.

## 2026-11-18 18:16UTC: first production cloud cycle complete

Batch `95b56cee0bd19fa0d445463a` is now accepted in the authoritative cloud
catalog: 40,931 jobs, 503,547,657,661 uploaded bytes. All 253 remote objects
passed size/MD5 comparison; user, aggregate, dictionary and one complete 2GB
TAR passed SHA readbacks. The pack-index SHA is
`0bd29be16dfd71c2469627ecba5927ba118b6b4e43c9661bcd1bf94657ebd831`.
Production cloud total including earlier pilots is 503,702,496,515 bytes;
the separately shared colleague sample is excluded.

Retirement finished: the preserved journal has 448,846 matching unlink-intent
and unlink-completion records. The last idempotent retry reports those objects
already absent; that is not an additional deletion. All 251 TARs were retired
and none remain. Small indices, receipts, recovery journals, frozen inputs and
188.31GB of pending node results are preserved. No broad cleanup occurred.

The newly accepted cloud state has a fresh 30-file 2.626GB consistent control
backup independently SHA-verified on node. No new numerical/node acceptance
occurred this round, so the scheduled audit used zero extra RPC; no new error
quantiles are claimed. Calculation remains 82.28%, with Data offline and
16,349 assignments unaccounted. This production storage-cycle completion is
not full-history completion. No code changes or OOM observed.
Evidence: `docs/data-collection/aave-v3/evidence/periodic-flow-20261118T181635Z/`.
