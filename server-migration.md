# Beta backend migration — 2026-10-23

## Current deployment

`https://dscbeta.duckdns.org` now runs entirely on dsc-web. Its backend no longer
uses dsc-data. The future application database belongs on dsc-data; connecting
the beta backend to that replacement database is separate work.

| Component | Server | Location / service |
| --- | --- | --- |
| HTTPS entrance | dsc-web | `dsc-test-domain.service` |
| Frontend gateway | dsc-web | `dsc-test-frontend.service`, localhost:18082 |
| Backend | dsc-web | `dsc-backend.service`, localhost:18080 |
| Backend files | dsc-web | `/home/fao/dsc-scripts/app`, service user `fao` |
| Published catalog pointer | dsc-web | `/home/fao/dsc-test-baseline-20260909/.runtime/data-release.json` |
| Published datasets | dsc-web | `/home/fao/dsc/data/catalog-releases/` |
| New extraction project | dsc-data | `/home/dsc-data/scripts/` and `/home/dsc-data/data/` |
| Source checkout | dsc-data | `/home/dsc-data/repository/` |

The backend preserves the account/research/request SQLite databases, DuckDB
user data, exports, signing key, Python environment, and pinned chat-agent tools.
The chat credential snapshot is private to the application runtime. No credential
values are stored in this repository.

The existing host guard now validates dsc-web. The chat executable ownership and
isolation checks remain enabled. The frontend service requires the local backend;
the former SSH backend tunnel is disabled. The catalog reads the existing local
publisher directly, so the old dsc-data sync job is no longer needed.

## Verification

- The source and destination runtime were synchronized with checksum comparison
  while both backend processes were stopped. The final copy transferred no changes.
- All 31,494 dataset files in release `714c7a6ea42e34103097f040` passed the existing
  SHA-256 verifier (27,075,721,274 bytes).
- The catalog loaded 237 datasets and a real Parquet preview returned successfully.
- All three SQLite databases passed integrity checks; chat-agent readiness passed.
- Public HTTPS frontend, health, and account-policy requests succeeded.
- Unauthenticated catalog access returned HTTP 401.
- Tests passed with the old backend stopped, then again after removing its
  PostgreSQL cluster. The destination service is enabled at boot.
- Extraction source/config hashes matched dsc-data; all 47 extraction tests passed.
- Temporary migration SSH access was removed.

These are deployment smoke and integrity checks, not a full browser or live
chat-generation test. The remote backend edits were checked directly; they are
not part of this extraction repository's Git diff.

## Database cleanup

The obsolete PostgreSQL 14 `main` cluster has been removed. PostgreSQL packages
remain installed; there is no replacement cluster or database yet. The old
backend and sync units have been removed from dsc-data. Unused Docker/containerd,
desktop login, printing, and local discovery services were stopped and disabled.
Unrelated model files, container data, root application caches, stale website
configuration, and retired-user temporary files were removed.

Cleanup completed: `dsc-data` is the only remaining human login and the only
home directory under `/home`. Removed accounts: hfsladmin, shiftpub, vmportal,
arbOp, openclaw, fao, vibe-projects, localllm, agenthon, and the retired dsc-link
tunnel account. Required OS/service accounts remain. SSH and passwordless sudo
for dsc-data were verified. New extraction scripts, environments, pilot data,
source checkout, and Git backup were preserved.

Root filesystem usage fell from approximately 216 GiB to 37 GiB, leaving
854 GiB available. No files or services on dsc-node were deleted or changed.
The fao account on dsc-web remains required by the migrated application.

## Git preservation

Repository: https://github.com/WillInvest/DSC, branch `main`.
Extraction commit: `9609619` (47 passing tests).
A recoverable bundle is retained at
`/home/dsc-data/backups/dsc-scripts-9609619.bundle`.
