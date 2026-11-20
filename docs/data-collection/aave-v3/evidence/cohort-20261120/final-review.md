# Final incremental cohort review

Read-only review of archive.py, metadata.py, transport.py and coordinator.py, with runner/ledger and existing archive APIs followed where needed. No remote state or production code was changed.

## Result

No new actionable defect or serious production-launch blocker was identified in this bounded source review.

The earlier persistence finding is resolved by LocalResults requiring a generation identity and rejecting incompatible populated roots. The encoding-window finding is resolved by reading the plan-pinned part_blocks and rejecting host overrides. The corresponding focused tests are present and pass.

## Checked contracts

- Archive creation requires every planned job to be node-accepted with exact contiguous block bounds. It reopens each declared receipt, validates its numerical payload identity, hashes its declared parts and excludes stray files.
- The catalog snapshot uses SQLite online backup. Archive metadata includes fixed scope, reserve mapping, metric/cohort definitions, original input link and pinned scientific and operational code. Explicit extra metadata is copied and hashed into the exact archive manifest.
- TAR construction reuses the existing full-member SHA checker and immutable index. Remote acceptance follows copy/check, three indexed-member SHA readbacks and one full TAR SHA readback. accepted.json is separately copied and SHA-read before complete.json is written. The report accurately describes sampled SHA readback plus complete remote size/MD5 checking rather than asserting a full cloud SHA scan.
- eMode metadata preserves ordered category changes, including the prefix before a requested scope, with category-zero default and end-of-block ordering semantics. Exported Parquet is read back before publication.
- TLS transport locates exactly the configured named worker on its expected host. It transfers closed receipt-declared files through bounded Dask reads and fsyncs result files/directories before node acceptance. Individual host/transfer unavailability leaves the other roles running and permits finite-lease reassignment.

## Verification

Executed locally with PYTHONDONTWRITEBYTECODE=1 and PYTHONPATH=scripts/user-history:scripts:

python3 -m unittest hf_market_replay.test_execution hf_market_replay.test_transport hf_market_replay.test_archive -v

All 19 tests passed. They cover generation restart, encoding override rejection, offline expiry/late duplicates, exact worker selection, timed-out reads, receipt traversal, closed-file transfer, incomplete archive refusal, immutable catalog backup and archive retry after an upload failure.

## Coverage limits

This review did not operate servers, rerun the numerical model, inspect the live TLS configuration, or claim full-history archive completion. The parent reported the real Data IO outage/late-duplicate pilot passed. Actual final release acceptance still requires its full node scope and the real Drive copy/check/readback receipt. metadata.py has source inspection and its built-in Parquet readback, with no dedicated local numerical-dependency test run in this pass. No formal ce-code-review workflow receipt is claimed.

## Reviewed file identities

```json
{
  "scripts/user-history/hf_market_replay/archive.py": "f1b2413d2a21d9c3c22458353054b87d8ccc15d19cde408b69d4e1a83721adb0",
  "scripts/user-history/hf_market_replay/metadata.py": "1403e33f22c0604af576553c07f527d1117ac00a6ff91d326dde3a087bd0c744",
  "scripts/user-history/hf_market_replay/transport.py": "47ea4ee6d74c5e169ac252b5555f60c3fba0feb9e31a77daa2daa486f1aa7e61",
  "scripts/user-history/hf_market_replay/coordinator.py": "4d2b006bdd298857bb277ee3b186a630b6952907b0cfd7841b3f5cc6f6ca8cd6",
  "scripts/user-history/hf_market_replay/runner.py": "493c40dcde96fff28dbb40ea9fe86dc514f5b06985c06af228f22f6b75765638",
  "scripts/user-history/hf_market_replay/ledger.py": "b0a71129d0ded0e132f66d08d9234e030ed2485e7f75f2d83b440da3891b4a97"
}
```
