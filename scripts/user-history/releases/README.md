# Colleague-facing continuous research releases

These helpers publish a small, independently usable subset of accepted HF
outputs. They do not change the numerical generation, global coverage, source
ownership or the production125GB/500GB archive lifecycle.

The first release pins24 node-accepted jobs covering blocks16751127–16831126
inclusive,80000 consecutive block ends,3–15 March2023 (partial boundary days).
Its authority selection was captured with `hf_flow_queue.read_snapshot` from
compute's current ledger and saved as `selection.json` on node. It includes all
3724 addresses seen by the end of the window and229091408 logical HF values.

`export_svb_sample.py` uses shared lifetime locks while copying exact accepted
Parquet bytes. It checks source/destination SHA, every user tile's dictionary,
list lengths, contiguous block/wallet coverage and total logical count; it
checks full aggregate scopes, null handling and N/W. Block times/hashes come
from frozen own-node headers. PyArrow CSVWriter produces ready-made protocol
and positive-debt reserve tables. `read_sample.py` reconstructs any wallet
without the private project environment; its CLI is tested on an actual wallet
before upload. Pandas is optional for plotting, not required by the reader.

The release layout is README/CLAUDE/AGENTS plus numbered quick-start, complete
vectors, dictionaries, validation and code folders. It intentionally includes
no credentials and requires no server access. The source raw inputs and
full-history production release remain separate.

Node staging root:
`/home/node/hf-colleague-samples/svb-20230303-20230315`.
Unit: `dsc-aave-v3-hf-svb-colleague-sample.service`, a bounded manual one-shot
using the existing frozen runtime bindings; not enabled at boot.

Uploads reuse existing rclone helpers into the dedicated HF Drive root under
`01_RESEARCH_READY/AaveV3_SVB_2023-03-03_to_2023-03-15_v1`. Every uploaded object
is size/MD5 checksum-checked. Additional download probes are not a release gate. Only `complete.json` establishes
publication success. `upload-files.json` records the exact object set and the
manifest's own hash separately, avoiding a self-referential checksum.

`publish_drive_index.py` is gated on that verified completion. It publishes root
navigation, a release catalog, CLAUDE/AGENTS entrypoints and an internal archive
README while keeping active archive paths stable. Its root guides receive
size/MD5 checks. Unit: `dsc-aave-v3-hf-research-index.service`.

Never rerun an already completed release to regenerate timestamps/checksums or
count its duplicated bytes as additional production coverage. Inspect its
completion and verification receipts. Updates should use a reviewed new release
version. Neither helper retires source results or changes sharing permissions;
the user chose to share the sample folder with colleagues personally.

The sample, quick-start amendment and root-index one-shots use documented rclone
environment options `RCLONE_FAST_LIST=true`, `RCLONE_TPSLIMIT=2` and
`RCLONE_TPSLIMIT_BURST=2`. These reduce metadata queries and bound API traffic
after shared Drive-project quota errors. They do not change production archive
services, credentials, scientific inputs or file identities. The frozen upload
manifest supports retries without regenerating the release.

After base checksum verification, `finalize_quick_start.py` adds a finite
borrower example and explains the original debt-free example. It retains the
initial completion proof, updates the draft manifest, verifies the final object
set for amended/new files before root catalog publication, retaining unchanged-file proofs.

The user confirmed colleagues can use the transferred sample after its full
size/MD5 check; do not gate this auxiliary release on redundant additional downloads. Earlier full-download attempts hit shared Drive-project quota limits.
Record `all_remote_download_checks_exact=false`, retain those failed attempts,
and state the actual checksum/probe evidence. Production cloud retirement rules
are unchanged; this sample never retires sources or advances production coverage.

Published 2026-11-18 UTC: sample folder ID
`1bVKHLFJVBMxDIe_CJw_5ioBnrQpVB-_Q`, 344 files / 495,223,404 bytes,
80,000 continuous blocks (16,751,127–16,831,126), 3,724 wallets and
229,091,408 logical user HF values. The borrower example has 80,000 finite HF
values. The root index is published and checksum-verified; sharing permissions
were left to the user. Evidence and amended/base receipts are in
`docs/data-collection/aave-v3/evidence/research-svb-20261118T0444Z/`.
