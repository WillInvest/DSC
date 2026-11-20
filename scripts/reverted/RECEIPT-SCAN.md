# Receipt-only historical scan

The user authorized receipt scanning separately from tracing on 2026-10-25.
Coverage is blocks 10,008,355 through 26,266,551 inclusive. No debug tracing is
started by these services, and there is no contract filtering in this stage.

- Node: `dsc-receipt-scan.service`, enabled, restarts on failure after 30 seconds.
  Runs `scan_receipts.py` with 32 processes using existing Web3.py HTTPProvider
  transport and native `eth_getBlockReceipts`. No Cryo or custom receipt decoder.
- Data coordinator: `dsc-receipt-delivery.timer` invokes the receiver on
  dsc-revert every 30 seconds after the previous delivery finishes.
- Revert: `receive_receipts.py` pulls files directly from node over private LAN
  SSH with rsync. It verifies SHA-256 and record counts once before acceptance.

Each immutable gzip JSONL file covers 1,000 blocks (last file may be shorter).
One row per block contains block number/hash, total transaction count, failed
count, sum of failed indices plus one, and complete raw failed receipts. Empty
failed lists explicitly record scanned blocks without failures. RPC errors and
missing/null receipts fail rather than becoming empty successful results.
Block metadata is fetched only for empty receipt responses to distinguish real
empty blocks. Nonempty responses check block identity, sequential transaction
indices, unique hashes and statuses; their count is supplied by the native RPC,
not independently checked against a second block-metadata call.

Node files: `/home/node/dsc-reverted/receipts/historical-v1/`.
Destination: `/home/dsc-revert/data/receipts/historical-v1/`.
The scanner publishes atomic chunk files then checksum metadata. Restart verifies
and reuses committed batches; at most the current uncommitted batch is repeated.
Destination totals count accepted batches only. Source data is retained.

Monitor: https://dsc-web.tail5aad6c.ts.net/ and
`ssh -t node@100.108.171.14 'tmux attach -r -t receipt-scan'`.
The existing cryo-events progress pane also includes the receipt stage.
Counters show completed blocks, current batch, failed transactions, throughput,
heartbeat, and destination acceptance. ETA is a changing throughput projection.

Benchmarks on node under `/home/node/dsc-reverted/benchmarks/`:
- Thread concurrency 8/16/32/64: roughly 40–52 blocks/s.
- Sustained 3,000-block process tests: warmed 16 ~299 blocks/s;
  32 ~406–415 blocks/s, CPU ~81–83%, I/O wait ~0.2–1.2%.
- Including gzip writes: 32 ~396 blocks/s vs 64 ~305 blocks/s.
Ranges sampled around blocks 11M, 18M and 26.264M. Cache warming affects these
measurements; they are not guarantees for the complete historical scan.

Custom scope is limited to batching, failed-receipt selection, durable resume,
transfer acceptance and progress integration. Compression, RPC transport,
process scheduling and transfer use gzip, Web3.py, ProcessPoolExecutor and rsync.
