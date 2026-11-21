---
title: Node-coordinated DSC research pipeline
date: 2026-11-21
status: active
---

# Product contract

Run one research pipeline from finalized Ethereum blocks through verified storage,
with dsc-node providing RETH, coordination and canonical research storage.
Data, Revert and Compute run local jobs. Google Drive holds the verified backup.
Preserve the existing application database, application runtime, RETH and Savva
transfer paths. The requirement for organized database storage applies here to
the research datasets and the SQLite catalogs that describe them.

## Storage and deployment acceptance

The sole canonical on-server research dataset root is
`/mnt/storage/dsc-data` on dsc-node's `/dev/vdb1`. The `node` account can
also open that directory at `/home/node/dsc-data`. RETH chain data remains
on `/dev/vda3`. Google Drive holds the verified independent backup.

Publish `events/`, `block-state/` and `reverted-tx/` under each of
`dex/uniswapV2`, `dex/uniswapV3`, `lending/aaveV2` and `lending/aaveV3`.
Keep the complete failed-receipt and trace archive under `revert/` and the
wallet and cohort HF products under `lending/aaveV3/health-factor/`.
Store catalogs, coverage and restore records under `metadata/`. Details
are in the [dataset catalog](../../scripts/pipeline/DATASET-CATALOG.md).

Deploy the tested release through `/home/node/dsc-scripts`, with the four
stage directories and a top-level `main.py`. Verify the launcher with the
real data mount and a bounded run after the production handoff. Leave
scheduling for the later deployment step.

After the single-writer cutover, check the published payloads and restore
them independently from Node and Drive. Repoint the consumers and clear
exact manifest-listed superseded research datasets from Data, Revert and
Compute and from old Node paths on both disks. Do not retire a file whose
new catalog, Drive coverage, or downstream dependency is still unresolved.
Required inputs, worker runtime state, credentials and unrelated application
data stay outside the retirement manifests.

| Requirement | Completion evidence |
| --- | --- |
| R1. Readable executable main | One named call per stage, explicit inputs and outputs, and public `once`, `continuous` and `status` commands. A recovered cycle keeps the same finalized block/hash. |
| R2. DEX and lending | Raw Cryo events, decoded events and every-block core state match accepted coverage and an isolated replay. Aave upgrade inputs and state seeds remain available. |
| R3. Reverted transactions | Store failed receipts, complete call trees and tracked-contract selections on Node. Revert uses its existing private dRPC configuration and reviewed RETH fallback. Replay preserves existing receipt status and nested match semantics. |
| R4. Lending HF | Complete all-wallet HF vectors and reserve/protocol HF for all, eMode and non-eMode users. Historical output is adopted by evidence. A versioned checkpoint supports the first incremental interval with independent indexed samples against RETH. |
| R5. Canonical storage and backup | A reader can discover current and historical releases by catalog, resolve formats and schemas, and restore samples from Drive with verified byte identity. |
| R6. Monitoring and recovery | Node exposes finalized head, accepted coverage, worker attempts and lag by stage, Node storage status and Drive verification status. A helper outage is visible without losing accepted output. |
| R7. Historical migration | Each accepted Data/Revert artifact is either already present under a verified canonical identity or copied and verified on Node and Drive. Exact redundant helper files are retired only after the covered data and dependent readers are verified. |
| R8. Verification and cutover | Stage tests, public-main tests, isolated historical replay, fresh finalized block verification, and single-writer handoff all pass. |

## Existing system and baseline

Live inspection on November 21 showed Data's `dsc-pipeline.service` running.
Its ledger had committed through block 26,463,976 at hash
`0x2972aa9322e4e2d719b19c8e958eac7a9ceb2d3245c380544df5d4369b070a54`.
The next cycle was already active. Capture a fresh consistent snapshot before
any adoption or cutover. This observed cycle position differs from physical
compaction and backup coverage and cannot substitute for either.

Data's inspected data root used about 85 GiB and its coordinator root about
2 GiB. Revert's inspected archive root used about 61 GiB. These are directory
sizes. The accepted inventories must also include historical roots, compacted
catalogs, active tails, source inputs and recovery evidence. At the observed
compaction endpoint of 26,463,976, shared-lock reads found these file sets.

| Catalog | Accepted files | Open files | Accepted bytes |
| --- | ---: | ---: | ---: |
| Node | 2,310 | 103 | 343,688,042 |
| Data | 12,394 | 555 | 896,404,997 |
| Revert | 594 | 3 | 987,528,940 |

These catalogs describe compacted incremental data and seeded tails. The
historical prefixes are inventoried separately. The separate Node
storage volume had about 4.6 TiB available at inspection while the existing HF
wallet consolidation was staging downloads. Reserve capacity against the
in-flight wallet publication and actual new Drive quota before full copy.

The historical Ethereum Aave V3 HF release begins at 16,291,127 and ends at
26,407,881 inclusive. It has a different coverage boundary from each pool,
the failed receipt and trace scan, and the live pipeline. The cohort HF
generation and ongoing wallet-only archive publication remain separate
provenance identities in the new catalog. The latter is adopted by validated
manifest and restore proof when available.

## Approach and reuse

- Extend `scripts/main.py` and `scripts/pipeline/` around the existing tracker,
  stage commands, compaction, verification, and execution transports. Declare
  hosts and roots in runtime configuration. Dispatch same-host work locally
  and reuse SSH/rsync for remote stage artifacts. Keep block/hash pins and
  run/attempt identity attached to retries.
- Keep Cryo for bulk logs, Web3.py's ABI event decoder, and the existing direct
  replay implementations. Keep the existing hash-pinned dRPC `callTracer`
  routing, validation and archive code. Extend selected-trace delivery to
  canonically accept full traces by their existing sealed checksums and
  manifests. Re-select from accepted full traces when tracking scope changes.
- Reuse `accounting_inputs`, `hf_f64_compute`, `hf_replay_checkpoint`,
  `FloatBatch.evaluate` and `hf_market_replay/cohorts.py` where the pinned
  generation and historical file formats apply. Version the range, dictionary
  and checkpoint interfaces that currently assume a fixed terminal block.
  Keep the old decoder available and adopt the historical source receipts.
- Use the shared-lock compaction snapshot API, SQLite online backup, existing
  SHA-256 verification, rclone copy/check and existing indexed-pack restore
  where applicable. Custom code handles stage dependencies, accepted artifact
  identities, worker routing, migration and the read-only status projection.

[Finalized JSON-RPC](https://ethereum.org/developers/docs/apis/json-rpc/) ·
[Cryo](https://github.com/paradigmxyz/cryo/blob/main/README.md) ·
[Web3.py event decoder](https://web3py.readthedocs.io/en/stable/web3.contract.html) ·
[dRPC block tracing](https://drpc.org/docs/ethereum-api/debugandtrace/debug_traceBlockByHash) ·
[SQLite backup](https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.backup) ·
[rclone copy](https://rclone.org/commands/rclone_copy/) ·
[rclone check](https://rclone.org/commands/rclone_check/)

## Executable shape

```python
cycle = resume_or_pin_finalized_cycle()
registry = load_tracked_contract_registry(cycle)
profiles = review_contract_profiles(cycle, registry)
events = extract_protocol_events(cycle, profiles)
decoded = decode_events(events, profiles)
state = reconstruct_every_block(decoded, previous_state)

receipts = collect_failed_receipts(cycle)
full_traces = trace_failed_transactions(receipts)
selected_traces = select_tracked_calls(full_traces, registry)

commit_node_outputs(cycle, events, decoded, state, receipts,
                    full_traces, selected_traces)

hf_interval = next_contiguous_hf_range(cycle.finalized_target, hf_watermark)
hf_inputs = prepare_aave_hf_inputs(hf_interval, accepted_inputs, previous_hf_checkpoint)
wallet_hf, cohort_hf, next_hf_checkpoint = calculate_aave_hf(hf_inputs)
commit_hf_outputs(hf_interval, wallet_hf, cohort_hf, next_hf_checkpoint)
publish_progress()
```

Each named operation resolves existing accepted evidence or invokes a worker
and validates its output. A stage can commit only evidence from the pinned
plan and input identity. The backup service derives pending jobs from
accepted Node artifact IDs that lack Drive verification, including after restart.
It publishes its own progress. Node acceptance can advance while Drive retries
within a configured storage budget. Keep backup payloads pinned until Drive
covers the same logical artifact. Retire only source entries covered by both
acceptance records.

## Dataset catalog and storage

Use a Node storage root configured separately from RETH. Use the same logical
layout in the Drive backup index. Catalog entries can point to immutable
legacy paths or verified pack members until bytes are consolidated.

| Layer | Dataset families |
| --- | --- |
| `raw/ethereum` | DEX and lending logs, chain metadata, failed receipts, Aave token/Pool/provider/configurator logs, oracle and reserve/configuration seeds |
| `decoded/ethereum` | ABI-decoded events by protocol, contract and profiled schema |
| `state/ethereum` | Uniswap and Aave reserve core state, state checkpoints |
| `reverted/ethereum` | Complete traced call trees and independently versioned contract selections |
| `analysis/ethereum/aave-v3/hf` | One all-wallet vector family, three reserve cohorts, three protocol cohorts, dictionaries and HF checkpoints |
| `metadata` | Dataset schemas, registry, SQLite catalog snapshots, coverage/checkpoint lineage, manifest/index files, validation evidence, restore and migration journals |

Use immutable keys combining chain ID, dataset, scope, schema version,
generation and a half-open block interval. Include endpoint hashes, parent
checkpoint, input IDs, registry/ABI IDs, source, row count, explicit empty
coverage, format, SHA-256, byte size, code revision and validation evidence.
Record Node and Drive locations and their verification separately. Catalog
snapshots resolve state under shared locks. Physical paths alone confer no
coverage. Preserve original 10,000-block deployment-anchored event/state grids,
1,000-block receipt/trace grids anchored at 10,008,355, and accepted historical
partial tails and their lineage.

## Implementation units and acceptance

### U1. Pin the working baseline and expose the cycle order (R1, R8)

Record code revisions, read-only coverage and live topology. Make stage
calls explicit in the existing coordinator before changing execution roles.
Keep the existing SQLite stage IDs and resume semantics. Test the stage order,
a retry of a failed step, and no advancement on incomplete evidence.

### U2. Introduce configurable Node coordinator and worker routes (R1–R3, R6)

Parameterize data roots, binaries and worker selection in the runtime, stage
and compaction adapters. Give each job a frozen input manifest and isolated
attempt output. Data, Revert and Compute may take eligible CPU work. Revert
retains dRPC dispatch and account administration locally. A worker may
reconnect or be replaced for eligible work without reviving a stale attempt's
right to commit. Surface a blocked trace watermark if the eligible route is
unavailable. Keep separate counters for full and matched trace acceptance.
Keep worker dispatch identities limited to allowlisted stages and scoped
artifact delivery. Revert's local trace service reads its keys. Test that
dispatch succeeds with that service and that the dispatch identity cannot read
credentials or start an unrelated command. Test redaction in monitor and
failed-job evidence.

### U3. Extend Aave HF past its historical cutoff (R4)

Build finalized input intervals from canonical Pool data, aToken/debt token
ledgers, token and risk configuration, oracle evidence and headers. Schedule
bounded HF intervals from the HF watermark using the accepted catalog. At
the inspected baseline, 56,095 blocks separate the HF release end from the live
extraction position. Close that gap contiguously before publishing HF for newer
blocks. Advance the HF watermark only with its accepted output and next
checkpoint. Freeze input identities at each attempt. Validate block boundaries and preserve stable wallet/reserve IDs
across new listings and owners. Carry scaled balances, debt, active collateral
and historical eMode/configuration state through checkpoint restart. Reuse a
shared evaluation for all-wallet HF and three cohort aggregates. Apply the
existing strict total debt below USD 0.01 convention. Validate debt-weighted
numerators and denominators and the all-cohort recombination against accepted
historical inputs. Keep original replay inputs and source provenance.

### U4. Verify storage and migrate existing research data (R5, R7)

Inventory accepted Node/Data/Revert artifacts and existing Drive indices while
production runs. Use SQLite online backups and catalog-consistent manifests.
Migrate immutable history first. Pin accepted open tail payloads under the
existing shared snapshot lock before copying them to Node. Use bounded copies
for files with a pending retirement identity, which includes link count. Seal the
canonical Node manifest, verify copied bytes and reuse existing exact accepted
copies. Publish immutable manifests and explicitly scoped batches to Drive,
confirm remote payload checksums, and restore representative members per
dataset and archive format into an isolated location. Confirm Drive restores
can succeed using only its published catalog snapshot and backup objects.
After cutover and dependency audit, retire exact eligible helper files under
a source identity check with a persisted recovery journal. Keep protected
runtime paths, small control evidence, required source inputs and any file
still required by ongoing jobs. Old HF mixed packs become eligible only after
the wallet-only and cohort outputs both have verified Node and Drive
coverage. Reversible Drive trash is the default for confirmed redundant
objects.

### U5. Verify normal entrypoints and cut over (R1–R8)

Run stage-specific tests for extraction, decoding, replay, receipt and trace
acceptance, HF source coverage and numeric results, catalog integrity and
backup/restore. Add a public-entrypoint fixture exercising a pinned chain,
worker failures and restart, duplicate delivery, invalid evidence, and a
round-trip restore. Before the historical replay, route all worker outputs and
ABI artifacts into verification-only roots and remove the write to the live
`current.json` from verification. Add a regression test that verifies both
properties. Run `pipeline/verify.py --blocks 100` against an isolated historical
interval with known results, then shadow a fresh finalized cycle
including HF above its prior cutoff. Exercise Data offline and a Drive retry.
Expose old and new status during the shadow. Finish the last Data-owned cycle and fence the Data writer.
Capture the final ledger, catalogs and pending work. Copy and verify the
remaining files, including the open tails and predecessor checkpoints.
Grant Node the production lease after it resolves the sealed boundary and
required predecessor evidence under its own roots. Repoint the existing
view-only monitor. Validate committed
progress, new cycle resume and Drive backup before retiring source files. A
rollback restores one writer by reconciling the last Node acceptance before
restarting the Data authority.

## Verification starting points

- `tests/test_pipeline_tracker.py`, `tests/test_pipeline_verification.py`,
  `tests/test_compaction.py`, `tests/test_historical_profiles.py`,
  `tests/test_decode_event_files.py`, `tests/test_incremental_replay.py`
- `tests/test_receipt_scan.py`, `tests/test_hosted_traces.py`,
  `tests/test_selected_delivery.py`
- `scripts/user-history/test_hf_f64_checkpoint.py`,
  `test_hf_f64_vectors.py`, `test_hf_f64_regressions.py`,
  `test_hf_flow_cloud.py` and the cohort tests in `hf_market_replay`

This plan records the live inspection and integration scope. Production
cutover, complete backup and retirement require their own acceptance evidence.
