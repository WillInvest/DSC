# Extraction throughput sweep — 2026-10-24

The retained change schedules four pools concurrently in the events/state lane,
using eight Cryo RPC requests per process. The trace lane stays at two workers.
Existing extraction, ABI decoding, coverage validation, data routes, and artifact
identities are unchanged. Concurrency uses Python's ThreadPoolExecutor, separate
SQLite connections, and Linux flock; custom code only coordinates existing jobs.

## Measurements

Four Uniswap pools (two V2, two V3), 250 blocks each; complete native node state
workers include both canonical header checks and output validation. They exclude
SSH transfer and decoding. Same input/output digests were verified across settings.

| Pool workers | Time, seconds | Contract-blocks/second |
| --- | ---: | ---: |
| 1 | 30.87, 29.49 | 32.39, 33.91 |
| 2 | 14.54 | 68.77 |
| 4 | 7.98, 7.66 | 125.39, 130.58 |

Median throughput increased from 33.15 to 127.98 contract-blocks/second:
+94.83, or +286% (3.86x). The two repeated samples show run-to-run variation;
these are small samples, not a confidence interval or backfill ETA estimate.
A second range at block 21,000,000 confirmed 30.26 → 10.32 seconds, or
33.04 → 96.93 contract-blocks/second (+193%, 2.93x), on one warmed pair.
The cold baseline for that second range was valid but failed the disk-latency
gate; it is retained in the evidence and excluded from the healthy pairing.
The original and final configurations use identical extraction code; only the
number of parallel jobs differs. The decision helper returned keep for both pairs.

The preliminary raw-Cryo sweep used four pools × 200 blocks:

| Pool workers | RPC requests/process | Seconds |
| --- | --- | ---: |
| 1 | 8 | 3.79, 3.50, 3.71 |
| 1 | 16 | 3.37 |
| 1 | 32 | 3.56 |
| 2 | 8 | 1.91 |
| 2 | 16 | 1.79 |
| 4 | 8 | 1.19 |
| 4 | 16 | 1.11 |

The raw pilot suggested roughly 3.1x from parallel pools; complete node workers
confirmed a gain on a different workload (not a directly comparable forecast).
Sixteen requests at four pools added only about 7%, below the chosen 10%
selection margin. Eight requests remains the production setting. The first raw
baseline partly overlapped a header diagnostic and is not used as sole evidence.
Cryo's [native concurrency controls](https://github.com/paradigmxyz/cryo/blob/main/README.md)
are reused; no replacement RPC extraction engine was written.

A separate 32-block failed-transaction pilot measured warmed times of 14.73,
8.06 and 6.19 seconds with 2, 4 and 8 workers. Normalized archive payloads matched.
The cold two-worker run took 60.49 seconds and failed the disk-latency gate.
The trace results are exploratory: cache effects are large and no second-range
confirmation was performed. They are not deployed as a production increase.
Changing this setting also needs an explicit artifact-identity compatibility
change because its default currently resides in fingerprinted extraction code.

## Hardware and correctness

In the four-pool worker samples, mean node CPU use was 18–22%, mean disk busy
2–30%, and available RAM about 191 GiB. RETH remained synchronized. These samples
establish headroom at four pools, not the maximum safe load for all historical
ranges or Aave reserve workloads. Two cold trials exceeded the conservative
100 ms single-sample disk-await gate even at baseline concurrency. No payload
validation failed. Health is sampled during trials and evaluated after each
bounded trial; this harness is not an automatic production load controller.

The full suite ran 90 tests: 84 passed and six optional tests were skipped.
Linux flock integration passed on dsc-data. Independent review found no concrete
correctness issues. Original and staged event/state/archive/reverted fingerprints
matched. A full four-pool pipeline pilot accepted all eight event/state jobs,
decoded 640 event rows and 5,000 state rows, and cleaned only acknowledged node
staging. Its tracker recorded 1,000 contract-blocks for each dataset.
The full pipeline took 32.63 seconds with four pools, versus 80.71 seconds on a
subsequent one-pool run (2.47x). This single warmed comparison includes SSH,
transfer, decoding, validation and cleanup; it is not a repeated capacity test.

## Remaining opportunity and limits

A diagnostic measured 17.89 seconds for 1,000 sequential header reads. Workers
repeat these checks before and after each job; batching with Web3.py is a likely
next opportunity, but was not implemented or included in the measured gain.
Preserving accepted coverage requires deliberate compatibility handling before
changing fingerprinted worker code. Events, Aave workloads, and long-running
mixed tracing/state contention are not represented by the worker speedup table.
Full-backfill completion time remains unconfirmed.

Raw result files: `../benchmarks/extraction-throughput/*/result.json`.
Local resume ledger: `.context/compound-engineering/ce-optimize/extraction-throughput/experiment-log.yaml`.
No new packages or paid services were used; server time was existing capacity,
and model cost was not observable. The bounded sweep stopped after identifying
and confirming a useful setting, before the 20-minute automatic-resume deadline.

Accounting: 22 native measurement runs plus two integrated pipeline runs; all
payload validations passed, two native runs failed the health gate, and none
timed out. One scheduling change was retained; five other raw concurrency
combinations were not selected. Three trace-worker settings remain exploratory.
The sweep and integrated checks ran for about eight minutes within the
20-minute pause cap. Production resumed with both services active; restart
checks revisit accepted ranges before continuing new extraction.
