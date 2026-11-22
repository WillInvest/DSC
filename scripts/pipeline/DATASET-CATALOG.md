# DSC research dataset catalog

## Protocol directories on Node

`/home/node/dsc-data` is the node-user mount of `/mnt/storage/dsc-data` on
`vdb1`. The publication command reads accepted imports and exposes this
layout without rewriting their historical Parquet files.

```text
dsc-data/
  dex/{uniswapV2,uniswapV3}/
  lending/{aaveV2,aaveV3}/
    events/<contract>/<schema>/<event>/<first>-<last>.parquet
    block-state/<contract>/<first>-<last>.parquet
    reverted-tx/<first>-<last>.jsonl.gz
  revert/{receipts,traces,selected}/<first>-<last>.jsonl.gz
  metadata/dataset-index.json
  metadata/publication.json
```

The three child datasets apply to each of the four protocol directories.
File ranges are inclusive. Event rows contain `block_number`, `name` and
the original decoded attributes. Several events can share a block number.
State has one row per block per contract and uses the state schema. Protocol
revert files contain matching transactions and retain the full call tree for
each match. The global archive retains failed receipts, complete traces and
all selected transactions. Historical market coverage advances in
`metadata/publication.json`. Independently published tail intervals have
receipts under `metadata/accepted-sources/`.
Accepted Parquet and archive files on the same disk are published as hard
links. The publisher slices accepted inputs only where the historical and
migrated block ranges overlap.

Original encoded Cryo logs live under
`metadata/replay-inputs/encoded/<protocol>/<contract>/` with inclusive
range filenames and schema folders for compacted inputs. Historical
acceptance manifests establish empty ranges. The raw publication watermark
is in `metadata/encoded-publication.json`.

`python -m pipeline.research_layout --root DATA --global-revert-only
--historical-reverts IMPORT` places the accepted global historical archive
while market publication is still underway. It checks that all three
archive families cover every block in each verified historical shard.
`metadata/global-revert-publication.json` records this watermark. The
normal publication pass also creates each protocol's matching subset.
`--protocol-revert-only --registry REGISTRY` can publish those subsets in
parallel with market placement. It joins only completed global selections
and checks that the contract registry matches their accepted fingerprint.
`--protocol-workers 4` lets one protocol publisher process independent
partitions in parallel. Revert publication commands in this release share a
lock. The standalone protocol publisher records contiguous coverage only
after each batch succeeds and retries from its last recorded block.

The `lending/aaveV3/health-factor/` directory holds historical indexed packs
and completed incremental windows. Run `python -m pipeline.hf_storage
SOURCE_WINDOWS DRIVE_WORK HEALTH_FACTOR_ROOT` to place already accepted
incremental windows from the same filesystem. Each window includes its
original result and checkpoint plus the exact Drive manifest, pack index
and verified receipt. `window-placement.json` records the contiguous range
with Drive restore proof. `local-accepted/<first>-<last>/` holds windows
whose inputs, outputs and checkpoints passed local validation.
Archived input metadata retains its original replay paths.

For historical wallet HF, open `historical/catalog/wallet-coverage.json`
under the health-factor root. Each accepted batch gives the directory under
`historical/wallets/` and the SHA-256 of its `pack-index.json`. This index
maps each archive member to its pack, byte offset, size and SHA-256. Use the
[indexed TAR reader](../user-history/releases/read_archive.py) with
`--folder`, `--member`, `--output` and `--index-sha256` to restore one member
and verify its bytes. A batch's `local-accepted.json` records Node acceptance
for a local publication. Earlier batches use `accepted.json`. A null `drive`
in the coverage entry means the newly published batch has no verified Drive
copy registered. Empty wallet intervals use their original job receipts and
validation records as evidence of zero wallet values.

Start with `/mnt/storage/dsc-data/metadata/dataset-index.json` on Node.
Its release entries link the physical catalogs and available acceptance
records. `availability=present` means the reference resolved when the
index was written. Open the referenced proof to determine the verified
coverage.

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
