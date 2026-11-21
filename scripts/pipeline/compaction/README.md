# Backfill-aligned compaction

The continuous coordinator compacts completed cycles before capturing another
finalized target. On startup it first catches up any completed backlog, then
resumes an interrupted cycle. The active incomplete cycle is never a source.

| Dataset | Blocks per interval | Original alignment |
|---|---:|---|
| Encoded events on node/data | 10,000 | Each pool's deployment block |
| Decoded event tables on data | 10,000 | Same pool grid; separate event, ABI pin and Arrow schema |
| Event-derived state on data | 10,000 | Same pool grid; exactly one row per block |
| Receipt archive on node/revert | 1,000 | Block 10,008,355 |
| Full traces and selected data on revert | 1,000 | Block 10,008,355 |
| Accepted selected data on data | 1,000 | Block 10,008,355 |

The initial backfill's final partial intervals are copied into the canonical
store once and filled with incremental data. Historical files remain unchanged.
For WETH/USDT V2 this produces `26263341-26273340.parquet`, followed by
`26273341-26283340.parquet`. Receipt/trace intervals start with
`26266355-26267354.jsonl.gz`. These retain the original grids, not new cycle grids.

A full interval is sealed and never rewritten. Only the newest partial interval
is extended by publishing a verified replacement with its actual ending block in
the filename. Empty event periods need no decoded event table; their accepted
coverage remains in the cycle receipts. JSONL archives include every block,
including empty failed/selected lists. Raw receipt and complete trace objects,
including provider/configuration identities, remain unchanged.

Role-local stores:

- node: `/home/node/dsc-extraction/compacted/`
- data: `/home/dsc-data/data/compacted/`
- revert: `/home/dsc-revert/data/compacted/`

`catalog.sqlite` is authoritative for active files, stream layout, original
anchors, source checksums/identities, output checksums and pending retirements.
`progress.json` is a monitor snapshot. The coordinator's `compactions` SQLite
table records the all-role committed watermark. A request contains only cycles
after that watermark, so it does not grow with the full extraction history.

PyArrow Dataset filters combine one logical interval at a time. Each Parquet
output is read back and compared against the input values. JSONL uses the existing
trace packer, then compares every complete nested record and block sequence.
Every-block state is checked for gaps and duplicates. Schema/ABI identities are
separate streams and cannot be silently cast into each other.

Files and parent directories are synced before a catalog transaction publishes
the replacements. New files cannot overwrite a conflicting target. Only after
commit can exact, hash/identity-matched source Parquet/gzip fragments and replaced
partial outputs be removed. Retirement resumes safely after interruption. The
original backfill, JSON acceptance receipts, plans, checkpoints, environments and
one-time Aave inputs are preserved. Metadata/checkpoint JSON is intentionally
retained; this compacts the primary dataset files, not operational records.

Read through a catalog snapshot, not a recursive file glob: an interrupted writer
can leave an unreferenced prepared file. Hold the snapshot while consuming files:

```python
from pipeline.compaction.reader import snapshot
import pyarrow.dataset as ds

with snapshot('/home/dsc-data/data/compacted') as files:
    paths = [f['absolute_path'] for f in files
             if f['stream'].startswith('state/uniswap-v2/ADDRESS/')]
    table = ds.dataset(paths, format='parquet').to_table()
```

For a complete historical view, use the original backfill files strictly before
the stream's `historical_before` boundary and canonical files from that boundary
onward. Do not also include the preserved original partial tail. Stream metadata
in `streams.spec` and the `meta.layout` record define this boundary and the pinned
registry. Future application database/catalog ingestion remains separate.

The main entrypoint invokes `run.compact_completed`; `worker.py` is an internal
role-local CLI. A second compactor cannot write the same catalog concurrently.
The monitor shows extraction and compaction progress separately. A compaction
failure preserves its evidence and stops the next pipeline cycle for recovery.
## Materialized migration captures

The capture manifest records every catalog-selected payload and pins the
replaceable open files at capture time. To materialize the capture, copy the
sealed payloads from the source store and the pinned open payloads from the
capture's `open/` directory to their manifest paths. Copy `catalog.sqlite` and
`manifest.json` to the destination root and create an empty `catalog.lock`.

Run `PYTHONPATH=scripts python3 -m pipeline.compaction.verify_capture ROOT` to
validate the SQLite file, manifest records and every payload's size and SHA-256.
Run the same check after unpacking the Drive backup into a separate directory.
Use a read-only snapshot for these imported catalogs. The catalogs also contain
source-host retirement records, which belong to the original writer.

To inventory the accepted pre-compaction market backfill, run
`PYTHONPATH=scripts python3 -m pipeline.compaction.historical_market` with the
source home as `--source`, the accepted sequential extraction plan as
`--plan`, and a separate `--output` directory. It records the encoded and
decoded files, the per-block state and its final checkpoint, and the original
acceptance receipts. It checkpoints one manifest per contract and carries
explicit empty event coverage. The migration receiver must verify these hashes
against the copied bytes before publishing an import.

On Node, `backup_historical_market` follows that published import inventory.
Run it with the pipeline scripts and the existing HF archive runtime on
`PYTHONPATH`.

```sh
python -m pipeline.compaction.backup_historical_market \
  --import-root /node/storage/imports/historical-market \
  --output /node/storage/imports/operations/historical-market \
  --remote dsc_hf:DSC_Research/imports/historical/markets --follow
```

Each contract has a Drive directory keyed by protocol, address and source
manifest SHA-256. It holds indexed uncompressed TAR packages, a pack index and
a `verified.json` catalog record. The catalog record is published after every
package passes the Drive size and MD5 check and one member of each present
artifact type is independently read back and SHA-256 verified. Restore uses the
catalog's `index_sha256` with the existing indexed TAR reader
`scripts/user-history/releases/read_archive.py`. Local `progress.json` lists
verified contracts and the active contract. The backup worker preserves Node
and helper originals.
