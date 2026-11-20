# Node direct Drive archive

The user authorized direct uploads and the restricted credential transfer on
2026-11-16. This supersedes compute staging in FLOAT64-ROLLOUT.md. The user confirmed compute manual cleanup finished. Aave-only redundant copies
may be retired after verified node preservation and a remaining-consumer check;
see NODE-CONSOLIDATION.md.

Data keeps the existing f64-archive.sqlite and approximately500GB dispatch
threshold. The archive service uses direct-archive-code/direct_hf_archive.py;
direct-archive-config.json separately binds the numeric plan SHA and uploader
code. The numeric plan and generation remain unchanged. Node's only archive
worker is dsc-aave-v3-hf-f64-direct-archive-worker.service. The compute archive
worker is disabled; numerical workers and the monitor remain required.

Only manifest-listed accepted files are sent through rclone copy --immutable,
followed by full size/MD5 check and user/aggregate/dictionary SHA256 readbacks.
Node originals are never removed. A per-batch lock prevents concurrent upload
attempts after controller restarts. No bulk file transfer or staging on compute
is required. The final partial batch is uploaded when calculation completes.

The tool is /home/node/hf-drive/tools/rclone, verified against the original
SHA2563f025d0c1d31eec3106c83da49cb6bac11d17ca403f23b2201fedc3a9798dcf9.
Credentials stay /home/node/.config/rclone/dsc-hf.conf mode0600. They were
transferred worker-to-worker over TLS, never through the Mac or repository.
Do not print credentials or resumable-upload URLs from private logs.

Rollback: stop only the new archive controller/worker, remove its direct-upload
service override, restore compute's archive worker and original controller.
Keep the shared archive catalog and all receipts; never reset uploaded coverage.
Both uploaders must not advertise f64_archive simultaneously.
