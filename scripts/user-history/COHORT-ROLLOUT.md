# Node-coordinated eMode cohort replay (2026-11-20)

The user authorized a fresh reserve/protocol HF replay excluding accounts whose
eMode category is nonzero at each block end. Individual HF calculations remain
the original float64 method. The output retains `all`, `non_emode` and `emode`
cohorts, each with weighted numerator, debt denominator, HF and borrower count.
The strict account debt below USD0.01 exclusion remains unchanged.

This is a separate HF run. It does not move the application database or the
continuous event/state/reverted coordinator. Existing HF workers, inputs,
accepted prior results and protected services have not been stopped or replaced.

## Authority and identities

- Node base: `/home/node/hf-cohort-20261120`.
- Current plan: `plan-v2.json`; cluster deployment: `cluster-v2.json`.
- Generation: `51f12482118a4c00040e72f8aad996d9fc2273b5836d304b5649d77fc5cd4f54`.
- Authority: `storage/control-v2/flow.sqlite`, using SQLite WAL/full durability.
- Fixed scope: blocks16,291,127–26,407,881 inclusive,10,116,755 blocks,
  10,124 jobs. Upgrade/checkpoint boundaries remain explicit.
- Node storage is a separate bind mount at `storage`; preserve its systemd mount
  and the admin-only permissions on the original `/mnt/storage` parent.
- The initial v1 generation and its388 accepted jobs are preserved separately.
  Never add their counts to v2 coverage.

Scientific source hashes and frozen inputs determine the generation. Operational
queue/transport hashes are recorded separately. All hosts use the same pinned
256-block Parquet encoding window, NumPy2.2.6 and PyArrow24.0.0. See the package
[README](hf_market_replay/README.md) for module responsibilities and CLI.

## Execution and transport

Node runs `dsc-aave-hf-cohort-20261120-control.service` and the private TLS
scheduler `dsc-aave-hf-cohort-20261120-scheduler.service`, listening on
`tls://10.246.103.160:18920` without a dashboard. The helpers have separate
`dsc-aave-hf-cohort-20261120-io.service` units, authenticating with the existing
HF mutual-TLS identities. No new trust or helper authorized_keys changes were
needed. The user subsequently approved a source-restricted SSH key if needed;
that fallback has not been installed on helpers.

All four hosts run `dsc-aave-hf-cohort-20261120-worker.service`:

| Role | Worker root | Slots | Memory limit |
| --- | --- | ---: | ---: |
| Node | `/home/node/hf-cohort-20261120/storage/worker-v2` |16|24GiB|
| Revert | `/home/dsc-revert/hf-cohort-20261120/worker-v2` |20|20GiB|
| Compute | `/home/fao/hf-cohort-20261120/worker-v2` |20|20GiB|
| Data | `/var/lib/dsc-aave-hf-cohort/20261120/worker-v2` |20|20GiB|

Each numerical unit binds its existing frozen image read-only at
`/home/dsc-data`, uses a private256MiB `/dev/shm` for multiprocessing, one BLAS
thread and independent local queues. Data's run root stays outside its masked
home. Helper IO workers use `Restart=always` because scheduler restart can cause
a clean Dask exit. Node's scheduler must include the versioned package on its
Python path for task deserialization.

Workers retain locally completed results through coordinator outages. Node uses
600-second finite leases, accepts each job once, and compares late payload
hashes. Data returning never changes the authority. A conflicting completion
stops acceptance for investigation; do not overwrite either result.

## Verification and recovery

`evidence/data-offline-rejoin-proof.json` records a controlled Data IO outage.
The other machines continued; Node took over two expired jobs. Data's returned
payloads matched both canonical SHA256 values exactly and were counted once.
This tests connectivity loss and rejoin, not an actual host power-off.

The real two-block canary compared six HF/debt/category samples against RETH
with54 actual RPC calls. Its unfiltered protocol HF exactly matched the earlier
release at1.5303809234957064; non-eMode was1.9404314341449214. A separate late
256-block benchmark took240.33 seconds and about475MiB peak RSS on one process.
Do not extrapolate the empty early-market jobs into a full-run completion ETA.

The Linux suite passed26 numerical, queue, transport and archive tests.
`evidence/emode-changes.parquet` retains34,077 ordered changes for19,138 users
in724,025 bytes, with default category0. Its proof and original Pool event index
remain available. This sparse history supports category lookup at any block.

Inspect `storage/control-v2/progress.json`, the authoritative SQLite ledger and
the four worker `progress.json` files. Progress files publish after a complete
controller pass; during large acceptance batches the ledger can be ahead.
Use SQLite's online backup API rather than copying a live SQLite/WAL pair.

Preserve closed receipts and all files they declare. To hold dispatch for one
worker, place `HOLD` in that new worker root; active local jobs finish. Never use
old protected worker units as a substitute. Restarting only the new controller
resumes accepted coverage and persisted completion cursors.

## Archive and authorized retirement

The new archive stage requires every planned job accepted before publication.
It revalidates receipt-declared files, takes a consistent SQLite backup, retains
definitions/dictionaries/code/eMode changes, and reuses the existing indexed TAR,
immutable rclone copy/check and SHA readback APIs. Cloud acceptance includes
three logical member restores, a full TAR readback and acceptance-report readback.
The stage preserves new node payloads and scratch copies.

The user authorized deleting old node HF *result payloads* once their existing
Drive copies are freshly verified. This supersedes node-copy retention only
for the explicit old result manifests. `hf_market_replay.retirement` is
verification-only: current remote size/MD5 checks, full local SHA certificates
and cloud SHA restores precede any use of the existing privileged retirement
helper. Preserve original replay inputs, events, code, metadata, dictionaries,
indices, receipts, recovery maps, RETH, trace archives and Savva dependencies.
Verification running or files listed in Drive do not imply retirement acceptance.

Runtime proofs and current progress are under node `evidence/` and the local
`docs/data-collection/aave-v3/evidence/cohort-20261120/`. The goal remains active
until complete historical coverage, verified publication and scoped retirement.
