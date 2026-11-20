# Hosted failed-transaction tracing

Authorized production scope: consume accepted receipt batches covering blocks
10,008,355–26,266,551. The shared tracing queue runs on dsc-revert against configured dRPC keys and
32 concurrent RETH slots using the private LAN endpoint. EVM replay for those
slots executes on dsc-node. Receipt scanning is complete. The existing tracker
assigns each pending block to one provider at a time; both providers preserve
the same archive and selection identity.
No account creation, key replacement, subscription change or billing changes
are performed. Successful key probes do not establish independent/free quotas.

## Entrypoints and services

- `trace_received.py`: dsc-revert, `.venv-web3` (Web3.py 8.0.0).
  `dsc-hosted-traces.service` is boot-enabled and restarts on failure.
- `trace_watchdog.py`: fixed-unit heartbeat watchdog every minute via
  `dsc-trace-watchdog.timer`; restarts stale (>300 seconds) or stopped services.
  Explicitly disabling the trace service is respected.
- `deliver_selected.py`: dsc-data, driven by `dsc-selected-delivery.timer` every
  30 seconds after the previous delivery completes. Pulls directly from revert.
- Configuration inventory: `/home/dsc-revert/.config/dsc/drpc-accounts.json`.
  It references private mode-0600 key files. Keys never belong in source/logs.

To pause intentionally: `sudo systemctl disable --now dsc-hosted-traces.service`
on dsc-revert. Resume: `sudo systemctl enable --now dsc-hosted-traces.service`.

## Rule and data handling

No failures: record an empty result without RPC. Otherwise compute the sum of
failed transaction indices plus one. If greater than total block transactions,
call `debug_traceBlockByHash` (hash pins the receipt archive's block); otherwise
call `debug_traceTransaction` for each failed hash. Use native `callTracer` with
all child calls and logs disabled. Full transactions are not fetched.

One in-flight request per account, paced to at most one request/second per key.
HTTP/rate errors cause cooldown and durable block retry. Explicit quota/payment/
authorization failures disable that key in the tracker; they are not bypassed by
creating keys. Remaining configured keys continue their assigned work. Disabled
keys need operator review before reset. Generic failures retry with bounded
backoff, never becoming empty results. Monitor shows the affected blocks/keys.

Trace structure, failed root status, transaction hash/index and block result
count are checked. Complete raw failed call trees and failed receipts are
retained on dsc-revert. All call destinations are matched against the 102 tracked
addresses, including successful nested calls inside a failed transaction.
Selected transactions carry every matched contract and retain the entire tree.
Successful top-level transactions (including caught internal failures) remain
outside this dataset's scope.

## Persistence and delivery

Root: `/home/dsc-revert/data/archive/reverted/drpc-v1/`.
- `tracker.sqlite`: WAL/full-sync SQLite. Committed 1,000-block chunks, current
  block completion, durable retries and account cooldowns. Finished blocks are
  removed from the live block table after their chunk is archived.
- `.work/`: checksummed atomic gzip block results. Individual trace checkpoints
  survive interruption halfway through a block. Only committed chunk work is
  removed. At most uncommitted/in-flight requests may be repeated after a crash.
- `archive/`: complete failed-transaction archive in 1,000-block gzip JSONL files.
- `selected/chunks/`, `selected/metadata/`: matching transactions and checksum
  manifests. Empty selections retain explicit block coverage.
- `progress.json`: atomic compact monitor snapshot, no credentials.

On dsc-data, `/home/dsc-data/data/reverted/drpc-v1/` receives only selected files.
Checksums, coverage and selected counts are validated once; `accepted.json`
prevents duplicate acceptance. This delivers files, not database ingestion.
The complete trace archive is preserved on dsc-revert for future reselection.
The selected-list identity is pinned; a different list requires a separate
selection pass from the archive, not re-tracing.

## Monitor and evidence

Web: https://dsc-web.tail5aad6c.ts.net/
Terminal: `ssh -t dsc-revert@100.91.215.76 'tmux attach -r -t reverted-traces'`.
The node's existing receipt-scan/cryo-events monitors also show tracing totals.
Monitor reports processed/archived blocks, failed and selected transactions,
per-key request counts, cooldowns/errors, heartbeat and dsc-data acceptance.
Request counters are operational estimates, not provider billing/quota evidence.

First 1,000-block production batch: 562 block-mode, 194 individual-mode, 244
empty blocks; 4,796 failed traces; no tracked matches in this early batch.
A separate existing recent-block sample had 10 matching transactions, verifying
positive selection against live trees. Six hosted-trace tests, ten existing
reverted tests and two delivery tests passed. Controlled stop/watchdog recovery
continued beyond the prior checkpoint; the initial tracker was ~45 KB.

Reuse: Web3.py HTTPProvider, native debug APIs, existing validate_trace and
called_addresses, gzip, SQLite, systemd and rsync. Custom code is limited to
receipt-to-trace dispatch, accepted-data adaptation, checkpoint/selection
orchestration, delivery acceptance and monitor integration.

## RETH joins the queue (2026-10-26)

`--reth-rpc http://10.246.103.160:8545 --reth-workers 32` enables bounded local
node tracing alongside dRPC. A single shared Web3.py HTTPProvider supplies the
RETH slots. Provider failure uses the existing per-slot cooldown; RETH being
unavailable does not prevent dRPC from starting. Each RETH slot verifies mainnet
before its first trace. Existing raw archives and partial checkpoints remain
valid. New trace entries record their provider; a resumed individual-mode block
can correctly report mixed providers. Old individual checkpoints were dRPC-only.

New keys account-11, account-12 and account-13 passed chain ID and exact
252-transaction baseline comparison. Activation brings the configured inventory
to 14 keys (original plus account-1 through account-13). No wallet signup occurs.

Sample comparisons found client-specific error text differences (`out of gas`
versus `out of gas: out of gas`) and malformed revertReason Unicode decoding
differences. The latest 384-block comparison matched all remaining trace fields,
including raw output. Preserve raw text and provenance; do not claim byte-for-byte
provider equivalence for every historical block. Short throughput tests are
cache-sensitive and cannot establish whole-history ETA.

Reuse remains Web3.py HTTPProvider, native RETH debug methods, and the existing
thread pool/SQLite scheduler. Custom additions only select providers, retain
provenance across resume, and expose provider worker counts in both monitors.


## Retry routing and graceful stop (2026-10-26)

All dRPC and RETH slots still share one dispatcher and one archive lock. Failed
block tasks now persist their preferred next provider: dRPC failures prefer RETH,
and RETH failures prefer dRPC. If that provider has no enabled slots, the other
provider can handle the retry. Per-block backoff and account cooldowns remain;
an available key at the front of the inventory cannot steal a retry assigned to
the other provider. Legacy HTTP 408 retries get a one-time local-node retry on
migration. `retry_history` retains failed attempts and subsequent recovery outcomes
(provider, account name, block hash, timing and redacted error), without keys.
This is block-task history, not proof that a remote RPC executed exactly once.

`--max-open-batches` defaults to 4 (allowed range 1–16). A stalled batch may coexist
with later work; completed later batches are archived and delivered independently.
Every batch is still complete and checksum-verified before publication. SQLite
completion/retry deletion and totals are scoped to that batch's block interval.
Incomplete batches keep their checkpoints across restart. If all four open batches
are blocked, the runner waits rather than growing an unbounded backlog. `--once`
continues to use one batch. Archive/config identities and tracer strategy are unchanged.

SIGTERM/SIGINT stop new assignments and drain current RPCs. In individual mode the
current transaction response is checkpointed, then the remaining transactions wait
for resume. Whole-block responses finish validation and checkpointing. The service
allows 240 seconds to stop (RPC read timeout is 150 seconds); forced termination,
host failure, and lost remote responses can still require repeating unfinished work.
The heartbeat continues during draining. Completed output is reused on restart.

Regression coverage includes provider routing, completion of a later batch while
an earlier retry cools down, preservation of earlier partial coverage, and a real
SIGTERM/restart test proving that a saved block is not requested again. Existing
checkpoint, receipt, selection, and delivery tests remain in place. Reuse remains
Web3.py HTTPProvider, concurrent.futures, signal, systemd, SQLite, and the existing
checksummed artifact helpers; custom code is limited to this archive's routing,
batch ownership and resume rules.


## Bounded dRPC connections (2026-10-27)

A live failure reached 1,023 open file descriptors (1,018 sockets) under the
service's 1,024 soft limit. Web3.py's default session cache is per provider and
executor thread; dRPC accounts moved across the 63 threads, multiplying idle
connections. Errors included failed checkpoint writes and transport errors,
followed by process restarts. Previous functional tests did not exercise this
cross-thread connection reuse.

Each dRPC account now supplies an explicit requests.Session to Web3.py
HTTPProvider, using HTTPAdapter with pool_connections=1, pool_maxsize=1,
pool_block=True and max_retries=0. The installed Web3.py 8 implementation was
verified to reuse an explicit session across threads. The dispatcher still allows
only one in-flight task per account, so a dRPC session is never concurrently used.
ExitStack closes the sessions after executor draining. RETH retains one shared
HTTPProvider with its default per-thread sessions (at most 63 executor threads).
Steady-state RPC connections are therefore approximately 31 dRPC plus at most 63
RETH, rather than one connection per dRPC-key/thread combination. This does not
increase worker count, change archive identities, or introduce hidden HTTP retries.

A real keep-alive HTTP regression test runs four keys across eight threads: the
old implementation used 32 connections; the fixed implementation uses four.
All 30 hosted-trace, reverted, receipt and delivery tests passed in the deployment
environment. The old process was temporarily allowed 4,096 descriptors to drain;
the replacement retains the existing 1,024 soft limit. On recovery, only local
Errno 24 retry/account cooldown deadlines are made eligible again; attempt counts
and failure history remain intact. Persisted checkpoints are reused.
