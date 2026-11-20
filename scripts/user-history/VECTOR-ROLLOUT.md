# HF vector handover — paused by the user

The user explicitly paused numerical calculation on 2026-11-16 to investigate
another vector calculation method. Do not resume, remove the pause marker or
start archive/compute jobs until an explicit resume instruction. Four-hour
supervision is read-only and must preserve the pause.

Data owns the reconciled ledger at `/var/lib/dsc-aave-hf-vector/20261116`.
`PAUSED_BY_USER` and the producer's `zzz-user-paused.conf` prevent automatic
restart. `dsc-aave-v3-user-hf.service`, the new vector workers on Data/node/revert
and node's vector acceptance workers are stopped. The previously protected
legacy worker units remain unchanged. Private schedulers and monitors may stay
idle. Accepted receipts and unfinished parts are retained.

## Verified handover

- Fixed scope: Ethereum Aave V3, 16291127–26407881 inclusive, all users from
  first appearance, 910310934885 logical user-block HF values.
- Predecessor reconciliation retains 4171 accepted intervals / 40182805784 HF
  values under their original identities. Three host inventories checked the
  original output SHA, size, physical rows, owner universe and validation/factor
  receipts. Data's 129413218 overlapping rows are not added again.
- Four Data overlap slices, 33816409 rows, matched user/block keys and all six
  account values exactly, including a different physical file hash and a parent
  interval split over three predecessor receipts.
- New format generation `60e4873a31048140d1e9f64cd6bd3b047514703751e1103d7a70a5d6c0dc215a`;
  assignment manifest SHA `55db948592a78ab5d13dd2d11acf288f0688fa09ee188d469d5ad43bedc67a51`.
  Full precision HF-only vectors; complete factors and prior full-row results
  retained. See `hf-vectors-20261116/format-v1.json` for representation.
- At pause: 45 vector receipts, 435170725 values, 789469660 output bytes and
  270 retained node getter comparisons, all exact. This includes the prior
  9794116-value canary; 44 receipts / 425376609 values were subsequent production.
  Per-job node acceptance is separate from supervision RPC accounting.
- Code: existing native integer kernels and vector codec/acceptance unchanged;
  `vector_queue.py` is Data's authority, `vector_tasks.py` uses Dask dependencies
  to move buffers directly to node. SQLite acceptance is on Data; bulk outputs
  remain on node. The old retrying coordinator was switched, not the protected
  legacy worker units.

## User's updated machine role and storage direction

The former dsc-web machine, `fao@100.119.128.31` / LAN10.246.103.143, is to be
called **dsc-compute** and assist computation. Tailscale's reported HostName is
already `dsc-compute`; the administratively pinned DNS name still says
`dsc-web.tail5aad6c.ts.net`. The user is signing into the Tailscale admin page to
finish that name change. Do not claim the client list or DNS has changed yet.
Existing login, service paths and historical `web` receipt role remain for
compatibility; do not blindly replace them. The monitor remains at the old URL
until an actual verified DNS/HTTPS cutover. Existing application dependencies
are preserved during cleanup; the server has not been fully cleared.

The user authorized roughly500GB Drive batches using the helper machine as
bounded temporary storage. Drive `drive.file` authorization is installed privately
on that machine; do not expose or archive credentials. Dedicated private folder:
https://drive.google.com/drive/folders/1o2TUruxT2HG67BHyZJTGGe0sjG8pQNMZ .
Actual quota readback had5497558138880 total /5269385111520 free bytes. Quota can
change. `vector_archive.py` is a **draft**, not a deployed or tested archive
service. No500GB batch or HF upload has completed. Its intended high-level path
is Dask control, restricted read-only rsync, rclone copy/check and SHA256 restore
probe, with smaller temporary chunks within a500GB batch. Resume requires an
isolated real upload/readback test and review of that draft. Node originals stay.

## Cleanup and recovery

Only two explicitly verified-backed-up old HF outputs and their10 known hardlinks
have been removed from the helper machine:1309944152 logical bytes, df net increase
1309937664 bytes. Exact independent node backups remain. Original receipts were
not rewritten. Data's `retained-web-location-bootstrap.json` maps the original
paths to node backups; consult this mapping when opening those retained receipts.
Recovery plan on the helper is
`/home/fao/dsc/.ops/runs/web-cleanup-20261116/bootstrap-plan.json`.
Other outputs, inputs, application state, monitor and other users are retained.
The larger node backup rehash was stopped unfinished at the user's pause; it is
not a completed verification or cleanup batch.

## Monitor and supervision

Data read-only role endpoints:10.246.103.151 ports18891..18894, path/status.
The existing private monitor's configuration pins the manifest SHA above.
`paused_by_user` is an explicit state, not materializing or a stale live producer.
The four-hour `aave-v3` automation now checks only the pause and existing artifacts;
no calculation, uploads, resumption or redundant node calls without new output.

Evidence and immutable recovery maps:
`docs/data-collection/aave-v3/evidence/vector-rollout-20261116T1813Z/`.
Append research records to `docs/data-collection/aave-v3/events.jsonl`.
