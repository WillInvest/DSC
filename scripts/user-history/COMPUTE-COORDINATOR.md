# Temporary HF coordination on dsc-compute

On 2026-11-17 the user explicitly requested dsc-compute coordinate while Data
remains offline. Only this HF flow moves; the application database does not.

## Current authority and services

SSH `fao@100.119.128.31`, root `/home/fao/hf-flow-control-20261117`.
`flow.sqlite` is the current HF acceptance ledger. Configuration is
`flow-config.json`; `authority-epoch.json` records `compute-20261117`.
TLS scheduler: `tls://10.246.103.143:18896`.

- `dsc-aave-v3-hf-flow-scheduler-compute.service`: TLS control scheduler, 4GiB limit.
- `dsc-aave-v3-hf-flow-control-compute.service`: transfer/catalog/cloud controller,
  8GiB limit; independent numerical producers do not wait for it.
- `dsc-aave-v3-hf-flow-status-compute.service`: LAN18911, serves only `public/`.
- `dsc-aave-v3-hf-flow-audit-compute.service`: bounded own-node numerical audit.
- `dsc-aave-v3-hf-flow-backup-compute.service`: consistent backup and verification.

Node/revert/compute flow IO and node cloud units have
`90-compute-coordinator.conf` overrides. Their numerical producers and protected
legacy units were not restarted or disabled. Data's old scheduler no longer has
these workers. Its local numerical assignments were not reassigned.

Scientific generation remains
`db2fce32bfc0a227f5d527ffee45c8212d1da0a939ec26447be07ada39af3efa`;
plan SHA remains
`e0de3b44ab0ea4fd6004310fa86364e80abfaf51d2a75279b24553965261ca01`.
No mathematical or sample-definition change was made.

## Recovery and verification

The source was node's immutable online backup
`control-backups/1794924560924572584`; all26 file hashes were checked before
SQLite backup API created a separate live ledger on compute. Original Data
and node backups remain unchanged. Accepted receipt provenance was checked
against the frozen plan; no historical numerical results were decoded or rerun.

Three explicit node full-SHA proofs were reconciled with source retirement:

| Transfer | Jobs | Node proof SHA256 |
|---|---:|---|
|87631df9d7c40ebdbb18efdf|8619|1b6074d54be1294972e5d2d5304b32190f0b9efe09e6c8233fcdbe3b577bbc0e|
|4abe26a99153a4069281beca|8654|91685cab5430d9a4df3af3986095d5a830554fb9e5e814d1fdad8429d19b5f91|
|1123a3670a90e9df96c88bc4|2151|31e5ce82790a0f83a77ae33a7b63189ef443ffed35155dded436da8af902682c|

Shared-lock catalog catch-up added3160/6339/1103 journal entries from
node/revert/compute at the initial catch-up. These are recovery observations,
not all newly computed during this migration. Completed IDs remain immutable.
Data's last backed-up accepted count is retained but is not a fresh observation.

The backup predates the existing direct-scatter/bulk-payload scheduler memory
fixes. Those current controller fixes were retained. A new regression also
checks that an unavailable source's prepared transfer cannot suppress online
archive-fill transfers. Its operation remains pending; only the projection
omits unavailable workers. All11 controller tests passed in the actual frozen
runtime after observing the new regression fail before implementation.

Fresh audit `audits/audit-1794941054132133040.json`, SHA
`5d7479a3de0dbc066e780053a96aacf47a3b9e31452a396468989c7d32078935`:
18 HF samples,75 actual own-node RPCs (37chainId,20block,18eth_call), structural
checks passed; max absolute HF error2.220446049250313e-16, p99 absolute
2.0428103653102879e-16. This is float64 agreement within budget, not integer equality.

## Backups, monitor and rejoin

`code/backup_hf_compute_control.py --config <root>/flow-config.json` uses the
existing SQLite online backup API and rsync. The dedicated server-local SSH key
is restricted by source IP and forced `rrsync -wo -no-del` to node's isolated
`/home/node/hf-flow-20261116/compute-control-backups`. Its private material never
leaves compute. Node reads every file and verifies SHA over existing Dask TLS;
the full database never enters a Dask task graph. The initial30-file1.535GB
snapshot passed; subsequent reports are `backups/verified-<stamp>.json`.
The temporary bootstrap read-only key was revoked after copying.

Monitor remains `https://dsc-compute.tail5aad6c.ts.net/`. Its HF sources now read
compute LAN18911 role JSONs and report `coordinator_role=compute`; missing Data
producer heartbeat is explicitly shown. Other pipeline cards may remain stale
while their Data source is offline. The existing four-hour automation was updated.

Preserve the current node storage mounts, old VDA staging and Data's incomplete
`984723d0f17d2e8cfe8a4e44` transfer. Node own9100-job queue completion is legitimate;
it is not all92294 jobs complete. At migration validation,397.334GB was node
cloud-pending and only154838854 pilot bytes had cloud proof. Do not call that a
completed production500GB cloud cycle.

When Data returns, do not reconnect its old coordinator automatically. Compare
its live database and journals with this authority and both backup chains,
reconcile new local receipts and the interrupted transfer, then explicitly
choose a single coordinator. Never overwrite either ledger from a stale snapshot.

## 22:16UTC operational import correction

The first four-hour check found both helper transfers repeatedly failing with
`write_json() got an unexpected keyword argument 'durable'`. The live Dask
worker prepended its frozen cwd to `sys.path`; the old frozen `shared` package
won even though PYTHONPATH listed the updated operational helper. Earlier shell
checks used another cwd, so they missed this production import order.

The minimal correction reuses the identical JSON writer through unique module
`hf_flow_json.py`, without a custom loader or duplicate writer implementation.
Source symlink: `scripts/user-history/hf_flow_json.py -> ../shared/artifacts.py`.
Node deployment symlink points to root-managed
`/usr/local/libexec/dsc-hf-flow/shared/artifacts.py`. Numeric frozen images were
not edited; only new node IO and coordinator were restarted. New IO SHA:
`413bc37fed40b97919e8112f588f73b14742966e825ec9d44dac9477a31f52b8`.
Writer SHA: `bc469f7dd7b0f693c29b4d04b4ce16790e034dd7b8172156659fd24e749a606b`.

The new shadowing regression failed before correction and passed after.16 Linux
tests passed with one bind test skipped; that test then passed separately against
the real bind mounts, completing all17. A complete small copy through real Dask
and its spawned child passed full destination checks. Original production
batches resumed; explicit ledger-listed web receipts were newly published.
That is progress, not complete batch acceptance or cloud proof.

The extra audit reported `no_new_accepted_output` and0 extra RPC because node
acceptance had not advanced. Separately, newly generated helper validation
receipts were inspected: normal per-job own-node checks continued and passed.
Do not describe old audit results or normal acceptance RPC as this round's extra
audit. A new consistent1.792GB control backup passed node SHA verification.
