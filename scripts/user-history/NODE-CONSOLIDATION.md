# Aave HF node consolidation — 2026-11-16

User authorized all four machines to preserve Aave HF archives on node and
retire verified redundant copies elsewhere. Data remains numerical catalog
and application database authority. Preserve active frozen inputs, unfinished
conversion sources, unrelated applications, legacy protected workers, Savva
transfer dependencies and reverted-transaction archives.

## Recovery and acceptance

Node recovery root: `/home/node/hf-centralized/20261116`.
Data control root: `/var/lib/dsc-aave-hf-f64/20261116-v2/consolidation`.
Local evidence: `docs/data-collection/aave-v3/evidence/consolidation-20261116`.

`retired-source-recovery-map.jsonl` maps 1,270 retired fully-consumed files to
node copies, hashes, original aliases, source jobs, cleanup plan and verified
completion receipts. Its SHA256 is
`c6ab3d0ace4675a22123530223802ba895dbafb0c0406ec511d0388482033128`.
Original manifests and accepted receipts are unchanged. Restore exact bytes to
an unoccupied original path only after checking current consumers; do not
replace a newer file or alter frozen source identities.

Revert retired 411 reverse-backup files (~78.8GB) and 698 fully-consumed source
files (~126.4GB). Revert completion df gain ~205.4GB is cumulative for both
operations: do not add reverse-backup gain again. Compute retired 572 consumed
files (~103GB); this excludes the user's prior manual cleanup. Exact root FD
inspection, alias identity checks and append/fsync actions preceded deletion.
Node originals remain. Start-anchored independent content proofs were used;
the earlier finish-time reuse proof was preserved but NOT used for deletion.

## Transfers and archive preparation

`frozen-source-inventory.json` was built from the frozen numeric plan and a
SQLite online snapshot. A file is fully consumed only when every exact
referencing job is accepted. Files with remaining jobs remain locally readable.

`node-source-projection.json` declares 4,608 expected old source files. This is
an expected-location inventory, not proof that all files arrived. It must be
combined with successful immutable transport manifests and hash receipts.
Node originals and pre-existing verified backups are reused without copying.

Missing-file batch `20261116-centralize-v1`: compute 74 files /29,600,603,800
bytes completed with all SHA hashes exact; revert 733 files /291,866,200,567
bytes are in progress. Original validation/output metadata has separate
`20261116-centralize-metadata-v1` batches. Don't overwrite these manifests or
count physical rsync progress as verified coverage. Retain each immutable
manifest and verification report: the latest `backup-verified.json` can refer
to a subset, not the entire role archive.

Source exports are authenticated read-only LAN18880 modules restricted to node,
with a 3,600-second lifetime. Retry the same immutable checkpoint after a
bounded source expires; retain partial files and do not create overlapping
exports. Transfer is LAN server-to-server, without bulk data on the Mac.

Node frozen image package `hf-frozen-inputs.tar.gz` is valid only when its
`.verified.json` reports complete member-hash readback. An `.incoming` file is
unfinished. Legacy Data control archive has 58 files /38,116,818 bytes verified
on node, preserving the old pause marker and 45 accepted receipts. Frozen
inputs and Data's live authority are retained in place.

Current automatic ~500GB direct Drive batches cover NEW accepted float64
outputs. Legacy-source consolidation is separate preparation; don't claim its
files are uploaded merely because they reached node. Keep node originals even
after successful cloud verification. See DIRECT-DRIVE.md for cloud acceptance.


## Node primary copy and Drive backup (2026-11-18; latest user direction)

The user requires all HF datasets on node as the long-term primary copy, with
Google Drive as backup. This supersedes deletion of node TAR packages after
cloud acceptance. Current flow-config.json has retain_node_archives=true and
node_archive_root=/home/node/hf-flow-20261116/storage/primary-archives.
Retain exact TARs and indices there; retire only redundant loose result Parquets
with the existing restricted cleanup helper and verified archive proof.

The first503.55GB batch95b56cee0bd19fa0d445463a had its node TARs removed under
the previous policy. dsc-aave-v3-hf-primary-restore-first.service is restoring
its253 cloud objects into the primary root. Copying is not completed restoration:
require full local SHA checks and indexed restore probes before acceptance.
The real primary-retention canary primary-retention-56903b5d63ef028b passed
cloud verification and retained its node TAR; it adds no logical coverage.

Legacy source backups are separate from new numerical coverage. Node service
dsc-aave-v3-hf-legacy-source-archive.service prepares four pinned manifests
(revert/web/node/inputs), verifies node bytes using backup_hf.verify, and uploads
through the existing rclone copy/check/readback APIs. Installed rclone does not
support SHA256 checksum; do not repeat that unsupported command. Canonical
legacy archive stages live in /home/node/hf-centralized/20261118-archive/catalog.sqlite.

Compute dsc-aave-v3-hf-legacy-helper-retirement.service waits for both node and
Drive acceptance, then uses existing consolidate_hf prepare/proof/retire APIs
on exact manifest-listed legacy outputs, in bounded batches. Compute cleanup
also waits for its active local numerical queue to finish because the cleanup
and acceptance share a writer lock. Keep inputs needed by active jobs; remove
helper HF caches only after dependency checks and verified copies. Preserve
programs, monitor, small logs/receipts, protected legacy services, reverted
transaction archives, Savva transfer dependencies and unrelated data/users.
No blanket home-directory deletion is authorized.
