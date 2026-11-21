# Aave market HF cohort replay

This stage computes every-block reserve and protocol debt-weighted wallet HF for
three cohorts: all included borrowers, borrowers whose account eMode category is
zero, and borrowers whose category is nonzero. Membership is evaluated at each
block end. Changing cohort membership leaves individual account math intact.

The historical scope is Ethereum Aave V3 blocks 16,291,127–26,407,881. The
original analytical rule excludes total account debt strictly below USD0.01.
The existing frozen inputs and historical Aave/GHO rules remain the numerical
source. The previous complete wallet HF dataset remains in its original release.

## Module boundaries

| Module | Responsibility |
| --- | --- |
| `calculation.py` | Adapt a bounded block job to the existing `BatchEvaluator` and `FloatBatch` |
| `cohorts.py` | Select debt edges and reduce them through the existing weighted aggregate API |
| `validation.py` | Encode Parquet, check coverage and partitions, compare samples with RETH |
| `ledger.py` | Record node acceptance and finite worker assignments in SQLite |
| `runner.py` | Execute local jobs, publish closed receipts, resume accepted local results |
| `dispatch.py` | Read worker status and receive explicit assignments |
| `coordinator.py` | Assign work, receive results, validate and accept coverage once |
| `transport.py` | Reuse authenticated Dask TLS and fsspec for control and closed-file transfer |
| `bootstrap.py` | Freeze scientific scope and source identities into an explicit job plan |
| `deployment.py` | Install separate host services using the preserved runtimes |
| `retirement.py` | Verify exact prior HF archive payloads against their Drive backups |
| `archive.py` | Publish complete accepted coverage with indexed TARs and Drive restore proofs |
| `metadata.py` | Export ordered sparse eMode category changes from preserved Pool events |

`calculation.compute` is the callable stage boundary for future integration with
`scripts/main.py` and the event/state/reverted pipeline. It accepts an explicit
job with inclusive start and exclusive stop, a checkpoint and pinned source map.
It returns bounded Parquet parts and provenance. The caller owns durable
publication, validation and acceptance. Production source pins are required by
the worker plan, and the plan has its own generation and output schema.

## Incremental input handoff

The Node candidate uses a separate run directory and the accepted image mounted
read-only. It prepares the first successor window as follows.

1. `hf_incremental_boundary` reads the accepted cohort publication, restores a
   historical replay seed, and advances to block 26,407,881. It seals scaled
   positions, collateral flags, eMode, GHO and reserve state only after the
   existing account and category comparisons against RETH pass.
2. `hf_incremental_window` pins contiguous finalized headers after the sealed
   checkpoint. Its receipt identifies the parent release, checkpoint hash,
   half-open block range, and final hash.
3. `hf_incremental_oracle` verifies Oracle routes and reserve lists at every
   block in the window. It runs the existing Cryo price collector and verifies
   the raw and decoded files and direct getter samples before publishing input
   receipts for the window.
4. `hf_incremental_pool` slices existing Pool events through the shared-lock
   Node catalog. Its receipt links the source hashes and records an exact
   comparison with the corresponding bounded RETH log query.

These receipts describe input coverage. Numerical coverage also requires the
versioned token and configuration changes, complete wallet and cohort
outputs, and their own validation. The incremental wallet tile format
`hf-vector-f64-v3` requires an explicit range, window SHA-256 and wallet
dictionary SHA-256 when encoding and decoding. Calls for the frozen historical
window still use `hf-vector-f64-v2`. Keep the deployed historical scientific
source pins when verifying the original archive.

## Output

Parquet rows identify block, reserve/protocol scope and cohort. Each row retains
weighted numerator, debt denominator, nullable HF and distinct borrower count.
Numerator units are Oracle base currency multiplied by dimensionless user HF.
The denominator uses the historical USD base unit, 1 USD = 100,000,000 units.
Protocol rows include account-level dust diagnostics. A zero denominator has
null aggregate HF. Protocol scope ID is declared in each file's metadata and
reserve addresses are stored in receipts.

The three cohorts preserve enough statistics to include or exclude the entire
eMode cohort without another numerical replay. Historical category changes and
original position/price/configuration inputs remain reconstruction inputs.
A receipt binds the job, generation, source hashes, Parquet hashes, independent
checkpoint reductions and own-node account/category/debt comparisons.

## Execution

Use the package from a Python path containing `scripts/user-history` and
`scripts`, or from its versioned deployment directory with the frozen runtime.

```sh
python -m hf_market_replay worker --config /path/to/role.json
python -m hf_market_replay coordinator --config /path/to/cluster.json
```

Node owns the coordinator ledger. Each host has its own worker root, local
completion database and closed result directory. Worker and coordinator roots
must differ. The shared plan pins scope, source hashes and the encoding window.
Every worker root is bound to one generation. Scientific fingerprints cover the
calculation and its inputs. Operational fingerprints record the queue and
transport code separately, allowing future pipeline integration to reuse the
same scientific outputs.

The IO scheduler runs on node and reuses the HF system's existing mutual-TLS
identities. IO workers on the helpers expose queue operations and receipt-declared
file reads through Dask. Numerical jobs use local multiprocessing queues. File
transfers retain fsspec streaming and have a bounded wait when a host disappears.
The scheduler and clients must have the same versioned module path. Worker
services use private shared memory to preserve multiprocessing semaphores across
operator login/logout, and IO services reconnect after scheduler restarts.

Workers finish their local assigned jobs through coordinator outages. A host
whose heartbeat becomes stale stops receiving lease renewals. Node reassigns
expired unfinished work to available machines. Matching late results count once.
A conflicting result stops acceptance for investigation. Retained immutable
receipts allow a restart to resume without recalculating completed jobs.

The controller and worker status files are read-only monitoring inputs. A future
integrated pipeline can submit jobs and consume accepted receipts while leaving
host dispatch outside the scientific calculation module.

## Verification

```sh
PYTHONPATH=scripts/user-history:scripts python -m unittest hf_market_replay.test_calculation hf_market_replay.test_execution hf_market_replay.test_transport hf_market_replay.test_archive -v
```

Numerical tests require the pinned Linux NumPy and PyArrow environment. The
execution tests use Python's standard library and the existing shared artifact
helper. Real-input canaries additionally compare historical account values,
eMode categories and debt weights against the project's own RETH node.

Old-result retirement first runs current size/MD5 comparisons, full local SHA
checks and SHA restore probes. Deletion uses the existing privileged manifest
retirement helper after exact-root policy authorization. Reconstruction inputs,
small metadata, dictionaries, indices, receipts and recovery proofs are retained.

Archive CLI: `python -m hf_market_replay.archive --config /path/to/cluster.json`.
It requires the reused `hf_flow_cloud`, `hf_flow_packing` and their existing
dependencies on the deployment Python path. See [rollout](../COHORT-ROLLOUT.md)
for verified host paths, services, recovery and current generation.
