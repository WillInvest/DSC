# DSC research dataset catalog

Start with `integrated-pipeline/metadata/dataset-index.json` on Node. Its
release entries identify the physical catalog, the Node acceptance record
and the Drive verification record for each dataset. The index stores
references to existing catalogs and proofs. `availability=present` means a
reference resolved when the index was written. Open the referenced proof
to determine the verified coverage.

| Layer | Unit of coverage | Authoritative catalog |
| --- | --- | --- |
| `raw/ethereum` | Contract logs per block, failed receipts per scanned block, and pinned Aave replay inputs | Market catalog streams, Revert archive manifest and HF window input manifests |
| `decoded/ethereum` | Contract events by protocol, ABI interval and event schema | Market catalog streams and accepted ABI profiles |
| `state/ethereum` | Core state for each configured market at the end of every block | State Parquet, its stream specification and the predecessor checkpoint |
| `reverted/ethereum` | Full failed-transaction call trees and the separately identified tracked-contract selection | Receipt, trace and selection manifests |
| `analysis/ethereum/aave-v3/hf` | One all-wallet HF family, reserve aggregates and protocol aggregates, each aggregate split into all, eMode and non-eMode | Historical release indexes, incremental window manifests and successor checkpoints |

## Index fields

- `format` identifies the index schema, `chain_id` identifies Ethereum
  Mainnet, and `indexed_at` records the observation time in Unix seconds.
- `releases[].id` identifies a catalog or release and `layers` identifies
  the datasets it supplies. `layout` explains how to open its contents.
  `node` gives its local directory, and `drive` gives its backup root or an
  evidence file that maps each object to a remote location.
- `evidence[].role` identifies a catalog, schema, Node acceptance, Drive
  acceptance or provenance record. `bytes` and `mtime` describe the
  referenced file as observed. A configured proof that has yet to appear is
  recorded as `pending`.

## Coverage and evidence

Use half-open `[start, stop)` intervals when joining partitions. A progress
record's `through` is inclusive and `next_block` is exclusive. Historical
market coverage starts at each contract's deployment block. A market catalog
lookup is keyed by the stream ID and interval. Its SQLite `files` table
records the relative path, row count, seal state and SHA-256. The `streams`
table records format and schema identities. The `sources` and `batches`
tables record input lineage and accepted publication watermarks. Empty
scanned intervals have evidence in the corresponding batch or archive.

Aave HF windows include chain anchors, accepted input identities, result
parts and a successor checkpoint. The wallet dictionary fixes the mapping
from wallet IDs to addresses. Reserve and protocol aggregates weight by
debt within their respective scope. Cohort membership uses the wallet's
eMode category at the end of the block.

For a restore, resolve the Drive proof for the same release and interval.
Use the indexed TAR reader for historical HF packs, the catalog restore
command for unified market storage, or the packed delta restore command
with its recorded predecessor. Verify the restored manifest, coverage and
hashes against the receipt before using the restored files.

Generate a new index with
`python -m pipeline.dataset_index CONFIG --output PATH`. Source progress
and acceptance records remain authoritative as new intervals finish.
