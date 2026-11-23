---
title: Store wallet health factor as sparse Parquet
type: feat
date: 2026-11-23
artifact_contract: ce-unified-plan/v1
product_contract_source: ce-plan-bootstrap
execution: code
---

# Store wallet health factor as sparse Parquet

## Goal Capsule

Reduce the physical size of historical and upcoming Aave V3 wallet HF while preserving every wallet query result and its coverage status.
Publish the current scripts to GitHub as the dense baseline before implementation.
Implement sparse wallet HF, migrate accepted history on Node, and enable sparse output for new accepted windows as a follow-up.

## Product Contract

### Requirements

- R1. Preserve the wallet dictionary, IDs, first-seen blocks, and every finite HF bit pattern. Omit a wallet row only if every value in its existing portion of that tile is positive infinity. Keep the full row when any value is finite.
- R2. A query distinguishes an HF value, analytical no effective debt, wallet not yet seen, and unavailable coverage. The analytical debt rule remains total debt below USD 0.01. Collateral and transaction activity are independent of this rule.
- R3. Keep the existing block-range by wallet-range tiling and range-first filenames. Use a versioned footer and an accepted inventory to distinguish dense and sparse tiles and verify complete file coverage.
- R4. Convert every accepted historical wallet tile and the dense incremental tail from their existing Parquet values. Preserve coverage across the historical and incremental boundary. Resume interrupted work from verified receipts.
- R5. For new windows, compute the existing full HF and cohorts, then persist sparse wallet tiles. The Node acceptance checks and backup and restore tooling must cover the new format.
- R6. Preserve reserve and protocol aggregates and the `all`, `emode`, and `non_emode` cohorts. Keep existing recovery copies until Node equivalence, independent Drive restore, and exact dependency and retirement gates pass.

### Acceptance examples

- Wallet 1500 has only positive infinity in an accepted tile. Its row is absent from the sparse Parquet. A covered block at or after first seen still returns no effective debt.
- Wallet 1500 borrows again midway through a tile. The whole wallet row is present, including its earlier infinity values.
- A tile with zero stored wallet rows still exists as a valid zero-row Parquet with its declared block and wallet envelope. A missing file expected by the published manifest is a coverage failure.
- A block before first seen returns a distinct status. A query past the accepted watermark returns unavailable coverage.

## Planning Contract

### Key technical decisions

- KTD1. Reuse PyArrow's Parquet reader and writer and Arrow row filtering for historical conversion. Use the current NumPy matrix and versioned Parquet writers for new output. Project code owns the eligibility rule, dictionary binding and source-to-output proof. [Arrow Table filter](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table.filter) and [Parquet writer](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_table.html)
- KTD2. Add a new sparse format identity, provisionally `hf-vector-f64-v4-sparse`. Keep `wallet_id`, `first_block_index`, `hf_vector`, and the existing footer range fields. Bind each tile to its exact dictionary and accepted manifest. Readers support both dense generations and the new sparse one during rollout. Reconstruct `+inf` only for IDs in a verified sparse tile with first-seen block at or before the queried block.
- KTD3. Keep logical wallet-block values separate from physically stored rows and values. The accepted inventory records the expected tile bounds, files, checksums, logical count, stored count, and omitted all-infinite rows. A zero-row file is included in the inventory. Do not represent missing files as empty tiles.
- KTD4. Preserve the source generation and build into a separate versioned destination on `vdb1`, with bounded workers, disk headroom checks, and resumable receipts. Use an atomic catalog activation after readback, with an exact dense rollback target. Keep the normal producer running during historical staging and reconcile the newly accepted dense tail before activation.
- KTD5. For rollout, coordinate the Node launcher and HF workers at an accepted window boundary. Roll the deployment and readers together so the next window uses the verified predecessor state and matching worker code. Preserve the prior catalog and release for rollback. Preserve original archive signatures and attach fresh sparse release acceptance and restore evidence.

### Observations and measurement

The sampled accepted block `26,480,692` contains 261,885 known wallets, of which 236,134 have infinite analytical HF. A 32-block tail sample reduced compressed wallet tiles from 6,710,071 bytes to 5,058,227 bytes at the current Zstd settings, a 24.6% reduction. Measure historical vintages separately and report both current disk usage and projected reclaimable bytes after retirement.

## Implementation Units

### U1. Add the sparse codec and update readers

R1–R3 and R5. Extend the versioned wallet codec in `scripts/user-history/hf_f64_vectors.py`. Update wallet query and validation readers to use the file envelope, format, wallet dictionary, and accepted coverage. Provide a shared filter for dense Parquet conversion and a sparse decoder that reconstructs the original logical panel. Test all-infinite and mixed tiles, wallets first seen mid-tile, missing and corrupt parts, duplicate IDs, sparse and dense read paths, and exact float round trips.

### U2. Migrate accepted history into a sparse release

R1–R4 and R6. Use the interval catalogs and verified source dictionaries in `scripts/pipeline/query_layout.py` and `scripts/pipeline/DATASET-CATALOG.md`. Stage sparse copies tile by tile with source and output hashes and exact expanded equivalence. Benchmark early, middle, late and current intervals before sizing the full batch. Validate resume after interruption and compare total coverage and file inventories, including all-zero-row tiles. Update the file naming README with sparse missing-row semantics.

### U3. Wire the live wallet writer and publish the new generation

R1–R6. Wire the sparse codec through `hf_incremental_compute.py`, Node and helper verification, `hf_incremental_archive.py`, and the scheduled query publisher. Review inventory and restore tools that consume dense row counts or verify wallet parts. Keep the aggregate calculation and wallet discovery on the existing path. Exercise a real finalized new window with the matching worker release, confirm its sparse readback and aggregate parity, reconcile the dense tail and atomically activate the accepted sparse catalog. Capture rollback evidence before retiring superseded data under the project's backup and dependency rules.

## Verification Contract

- For every migrated tile, verify declared block and ID envelopes, dictionary identity, source and result SHA-256, stored counts, and exact expanded HF equivalence. Compare finite values bit-for-bit and compare both missing and infinity masks. Verify the dataset inventory covers all accepted intervals exactly.
- On real data, compare wallet history and per-block snapshots before and after migration. Include debt repayment, dust debt, subsequent borrowing, pre-first-seen and incomplete coverage. Confirm reserve and protocol results remain byte-identical where the payload is reused.
- Test restart from a partially staged batch, a retained zero-row tile, a missing or corrupt tile, dense and sparse reads during rollout, generation switching, and rollback.
- On Node, verify a new scheduled window produces sparse files and advances accepted watermarks. Independently restore a sparse backup and repeat the same coverage and wallet readback checks before any retirement.

## Definition of Done

The dense script baseline is available on GitHub. Every historical and newly accepted wallet HF interval is readable through the sparse release with the same values and statuses. Node and Drive provide independent acceptance evidence, and the catalog documents byte savings. Any retirement follows the exact scope and project recovery gates.
