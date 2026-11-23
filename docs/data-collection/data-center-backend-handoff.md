# DSC data center backend handoff

## Start here

The research store contains Ethereum mainnet data (`chain_id = 1`). Its canonical path on Node is `/mnt/storage/dsc-data` on `/dev/vdb1`. The alias `/home/node/dsc-data` resolves to the same storage. The backend needs private, read-only access to the datasets it serves. The frontend should query the backend over HTTP. The private pipeline monitor serves operational status.

Read `metadata/dataset-index.json` for dataset discovery, then follow the appropriate acceptance records below for current coverage. Its format is `dsc-research-dataset-index-v1`. Each `releases[]` entry has `id`, `layers`, `layout`, `node`, `drive`, and `evidence[]`. Evidence records identify catalogs, schemas, provenance, and Node or Drive acceptance. `drive: null` means the complete release has no registered Drive acceptance. Node and Drive coverage can advance independently.

## File tree

```text
/mnt/storage/dsc-data/
├── dex/
│   ├── uniswapV2/
│   └── uniswapV3/
├── lending/
│   ├── aaveV2/
│   └── aaveV3/
│       └── health-factor/
│           ├── historical/
│           │   ├── catalog/{CANONICAL_JOBS.csv,wallet-coverage.json}
│           │   ├── wallets/<batch>/{pack-index.json,packs/pack-*.tar}
│           │   └── cohorts/<archive>/{pack-index.json,packs/pack-*.tar}
│           ├── local-accepted/<first>-<last>/
│           │   ├── local-placement.json
│           │   └── result/
│           │       ├── result.json
│           │       ├── wallets.json
│           │       ├── wallets-<first>-<last>-<wallet-start>.parquet
│           │       └── aggregates-<first>-<last>.parquet
│           └── compute/progress.json
├── revert/
│   ├── receipts/<first>-<last>.jsonl.gz
│   ├── traces/<first>-<last>.jsonl.gz
│   └── selected/<first>-<last>.jsonl.gz
└── metadata/
    ├── dataset-index.json
    ├── publication.json
    ├── encoded-publication.json
    ├── global-revert-publication.json
    ├── protocol-revert-publication.json
    ├── accepted-sources/
    └── replay-inputs/encoded/<protocol>/<address>/[<schema>/]<first>-<last>.parquet
```

Each of the four protocol roots has the following structure.

```text
<protocol-root>/
├── events/<contract-address>/<abi-schema>/<event-name>/<first>-<last>.parquet
├── block-state/<contract-address>/<first>-<last>.parquet
└── reverted-tx/<first>-<last>.jsonl.gz
```

| API protocol key | Physical path | Address filter |
| --- | --- | --- |
| `uniswap-v2` | `dex/uniswapV2` | Pair address |
| `uniswap-v3` | `dex/uniswapV3` | Pool address |
| `aave-v2` | `lending/aaveV2` | Pool proxy address |
| `aave-v3` | `lending/aaveV3` | Pool proxy address |

Use the [tracked-contract registry](../../contracts/ethereum/pools.csv) to populate the protocol and address selectors. Its creation blocks establish the earliest meaningful block per contract. Read current accepted bounds from Node metadata. Normalize Ethereum addresses to lowercase for directory lookup. Treat schema folder names as opaque ABI identifiers.

## Coverage and partition rules

- On disk, `first-last` includes both ends. Translate it to `[first, last + 1)` before intersecting it with API ranges. In manifests, `start` and `stop` already mean inclusive start and exclusive stop. Progress fields named `through` are inclusive.
- Use complete publication markers and their evidence to select readable partitions. A range with a valid acceptance record can contain zero events or zero selected reverts. A range outside accepted coverage has unknown results. Return these cases separately.
- Use the current catalog snapshot to resolve files before querying. Read only overlapping partitions, filter rows to the requested range, and paginate. Historical and incremental data can have different partition boundaries.
- Index paths, maintenance folders, pipeline inputs, and credentials belong on the server. Use allow-listed protocol names, validated addresses and event names, and server-side path resolution.

| Product | Current publication record |
| --- | --- |
| Decoded events and block state | `metadata/publication.json`, `stage = complete`, `market_through`. Use indexed historical evidence and registry start blocks to resolve lower bounds and accepted coverage. |
| Encoded logs | `metadata/encoded-publication.json`, `stage = complete`, `market_through`. |
| Global receipts, traces and selections | `metadata/global-revert-publication.json`, `stage = complete`, `start`, `through`. |
| Protocol-matched reverts | `metadata/protocol-revert-publication.json`, `stage = complete`, `start`, `through`. |
| Historical wallet HF | `historical/catalog/CANONICAL_JOBS.csv` maps each job interval to `canonical_cloud_batch`. Use the matching local batch and its accepted manifest and pack index. Use `wallet-coverage.json` for the historical bounds. |
| Historical aggregate HF | The cohort archive's `accepted.json`, `manifest.json` and `pack-index.json`. The packed `metadata/dataset.json` and `metadata/catalog.sqlite` carry dataset and job provenance. |
| Incremental HF | `compute/progress.json` gives the continuous accepted end as `next_block - 1`. For each window, use `local-accepted/<first>-<last>/local-placement.json` and `result/result.json`. |

## Physical schemas

### Decoded events

One row per log. Multiple rows can share the same block. Common columns include `protocol:string`, `chain_id:uint64`, `address:string`, `block_number:uint64`, `name:string`, `block_hash:string`, `transaction_hash:string`, `transaction_index:uint64`, `log_index:uint64`, `topic0:string`, and `source_file:string`. Some profiles add `schema_version:uint32`. Event arguments are `arg__<name>` columns with types defined by the file's ABI profile. Inspect the Parquet schema when projecting across versions. Preserve large integers as strings in API JSON. Use `(block_number, transaction_index, log_index)` for ordering and cursors. Keep `source_file` out of user-facing payloads.

### Every-block state

One row per accepted block per tracked contract. Columns are `protocol:string`, `address:string`, `block_number:uint64`, and `state_json:string`. Parse the JSON before serving it.

| Protocol | Observed core state fields |
| --- | --- |
| Uniswap V2 | `reserve0`, `reserve1` |
| Uniswap V3 | `liquidity`, `sqrtPriceX96`, `tick` |
| Aave V2/V3 | An object keyed by underlying reserve address. Per-reserve fields include `currentLiquidityRate`, `currentStableBorrowRate`, `currentVariableBorrowRate`, `liquidityIndex`, and `variableBorrowIndex`. |

Values in the sampled state objects are strings. These are reconstructed end-of-block core values. When serving human units, apply the correct token decimals and protocol scaling.

### Revert

Read gzip-compressed JSON Lines by accepted block range. `revert/receipts` contains block summaries with `block_number`, `block_hash`, `transaction_count`, `failed_transaction_count`, `failed_index_sum`, and `failed_receipts[]`. `revert/traces` carries the block in `receipt_summary`, failed calls in `failed[]`, and trace provenance. Trace rows can be sparse. Use the receipt summary and coverage evidence for block completeness.

`revert/selected` and each protocol's `reverted-tx` contain block records with `schema`, `block_number`, `block_hash`, and `failed[]`. A sampled failed entry contains `provider`, `receipt`, `trace`, and `matched_contracts[]`. A transaction can match multiple contracts or protocols. Deduplicate by transaction hash when computing cross-protocol transaction totals. Archive scope is mined transactions with failed receipts.

### Aave V3 health factor

**Wallets.** The Parquet row has `wallet_id:uint32`, `first_block_index:uint32`, and `hf_vector:list<float64>`. Historical batches contain a packed `metadata/users.parquet` dictionary. For incremental data, the wallet ID is the zero-based row in the matching `result/wallets.json`, which contains `[address, first_seen_block]` pairs. Public queries should identify wallets by address. Use the dictionary bound to the selected batch or window for internal IDs.

Read the Parquet footer keys `block_origin`, `chunk_start`, and `chunk_stop`. For zero-based vector offset `i`, the block is:

```text
max(chunk_start, block_origin + first_block_index) + i
```

Wallet HF has one vector per address for a covered interval. Historical data starts at block `16,291,127` and ends at `26,407,881`. Incremental windows start at `26,407,882`. Positive infinity is the analytical no-debt/excluded sentinel. JSON cannot represent infinity. Return a null value plus an explicit status to distinguish it from missing coverage. Wallet vectors do not carry an eMode flag or per-wallet debt values.

**Reserve and protocol aggregates.** Observed historical and incremental cohort files share the following columns.

| Field | Physical type |
| --- | --- |
| `block` | `uint32` |
| `scope_id` | `uint16` |
| `cohort` | `string` with `all`, `emode`, `non_emode` |
| `numerator`, `debt_base`, `hf` | `float64` |
| `borrower_count`, `excluded_dust_users` | `uint32` |
| `excluded_dust_debt_base` | `float64` |

Key by `(block, scope, cohort)`. Resolve historical scope IDs from the packed `metadata/reserves.json` dictionary. For incremental files, resolve reserve IDs from the zero-based `result.json.aggregates.reserves` array and the protocol ID from the Parquet `protocol_scope_id` metadata. Use reserve addresses and a separate `protocol` namespace in the API.

`emode` means the account's eMode category is nonzero at block end. `non_emode` means it is zero. The `all` cohort includes both groups under the same analytical debt rule. HF equals `numerator / debt_base` when debt is positive. Reserve aggregates weight account HF by debt owed in that reserve. Protocol aggregates weight it by total debt. Use the published `all` row or recombine the two cohorts by summing their numerators and denominators before dividing. A scope with no included debt has an undefined aggregate HF. The footer gives the Aave base-currency scale. The two dust-exclusion fields are populated for the protocol-wide `all` row.

**Historical packs.** After resolving a job or dictionary member, look up its relative path in `pack-index.json.members`. Entries contain `pack`, `offset`, `bytes`, and `sha256`. Read or cache that exact member from the uncompressed TAR. Read the member as Parquet. Avoid unpacking every archive per request.

## Backend query contract

The following routes are proposed for the backend. They give the frontend stable filters over the physical layouts above.

| Route | Typical query |
| --- | --- |
| `GET /v1/catalog` | Protocols, tracked addresses, datasets, event names, accepted ranges and validation status |
| `GET /v1/events` | `protocol`, `address`, optional `event`, `start_block`, `stop_block`, `cursor`, `limit` |
| `GET /v1/block-state` | `protocol`, `address`, bounded block range or point lookup |
| `GET /v1/reverted-transactions` | `protocol`, optional matched `address`, block range, cursor |
| `GET /v1/aave-v3/hf/wallet` | `address` and a bounded range. Return a series plus status where HF is undefined or coverage is absent. |
| `GET /v1/aave-v3/hf/aggregate` | `scope=protocol` or a reserve address, `cohort=all\|emode\|non_emode`, bounded range |

Return `accepted_through` and the requested and effective block range with each result. Report coverage gaps explicitly so the frontend can distinguish a genuine empty result from unpublished data. Use parameterized reads, bounded ranges and per-request limits. For wallet history, resolve the wallet ID from the dictionary and retrieve only the overlapping vector tiles before decoding values.

## Verified point lookup

This example was run against Node with PyArrow. It returned one block-state row containing `reserve0` and `reserve1`.

```python
import json
from pathlib import Path
import pyarrow.parquet as pq

root = Path("/mnt/storage/dsc-data")
path = root / (
    "dex/uniswapV2/block-state/"
    "0x43ae24960e5534731fc831386c07755a2dc33d47/"
    "11601387-11611386.parquet"
)
table = pq.read_table(
    path,
    columns=["block_number", "state_json"],
    filters=[("block_number", "=", 11601387)],
)
row = table.to_pylist()[0]
state = json.loads(row["state_json"])
```
