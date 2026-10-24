# Protocol event and state extraction

Shared runners support Uniswap V2/V3 and Aave V2/V3 reserve data. Contract addresses and production
block ranges are **not selected yet**. Earlier V3 pilot files remain historical
test data; no launcher defaults to that address.

## Script organization

```text
scripts/
  extraction/                 # dsc-node: Cryo event/state runners
    extract_pool_events.sh
    extract_pool_state.sh
    extract_reserve_state.py  # per-block discovery and reserve calls
    check_deployment.py       # proxy and ABI verification
    pool_common.sh
  decoding/                   # dsc-data: offline ABI decoding
    decode_pool_data.py        # CLI and batch orchestration
    pool_decode.py            # event and state decoding
    abi_fields.py            # lossless scalar/list/struct Arrow mapping
    decode_storage.py         # Parquet output and integrity checks
  validation/
    reserve_state.py          # discovery and per-reserve coverage
    verify_pool_state.py      # encoded state coverage checks
  reverted/                   # failed-transaction pipeline
    extract_reverted.py       # dsc-node: native tracing
    pull_reverted.sh          # dsc-revert: archive transfer
    select_reverted.py        # dsc-revert: contract filtering
  protocols/
    protocol_config.py        # discovers protocol configurations
    deployment.py             # historical proxy/ABI bounds
    dex/uniswap/v2/           # official ABI, config, static getter catalog
    dex/uniswap/v3/
    lending/aave/v2/          # V2 LendingPool ABI and reserve configuration
    lending/aave/v3/          # official ABI, config, reserve call definitions
  requirements/
    abi.txt                   # decoding dependencies
    rpc.txt                   # node tracing dependencies
```

| Protocol | End-of-block getters |
| --- | --- |
| `uniswap-v2` | getReserves, totalSupply, price0CumulativeLast, price1CumulativeLast, kLast |
| `uniswap-v3` | slot0, liquidity, feeGrowthGlobal0X128, feeGrowthGlobal1X128, protocolFees |
| `aave-v2` | getReservesList, then getReserveData for each asset (V2 schema) |
| `aave-v3` | getReservesList, then getReserveData for each asset (V3 schema) |

All event types are extracted as encoded logs in one pass. V2 decoding includes
Mint, Burn, Swap, Sync, Transfer, and Approval. V3 uses its own event ABI.
State is read directly, never reconstructed from events. V2 reserves may differ
from actual token balances, and its stored cumulative prices are not extrapolated
to the block timestamp. Neither getter set is a complete contract-state snapshot.

## Server locations

- Script/config/ABI copies: `/home/dsc-data/scripts/`.
- Current runners: `/home/node/dsc-extraction/scripts/`.
- Default node event/state output:
  `/home/node/dsc-extraction/data/<protocol>/<address>/events-raw/` or `state-raw/`.
- Decoder environment: `/home/dsc-data/scripts/.venv-abi/`.
- Reverted archive/selection scripts: `/home/dsc-revert/scripts/`.

The earlier `/home/node/dsc-pool-events-pilot/scripts/` runner copies are also
updated to require explicit protocol/address arguments. Prior data is retained.

## Extract on dsc-node

Production coverage is each tracked address from its verified deployment block
through the latest finalized block, with state at every block. For a fixed run,
set `START` to the deployment block (or the next missing block on resume) and
`END` to the captured finalized height + 1. The contract list and tracker remain pending.
Set `PROTOCOL`, `ADDRESS`, `START`, and `END`.
`PROTOCOL` is `uniswap-v2`, `uniswap-v3`, `aave-v2`, or `aave-v3`. Aave requires the profile described below. END is exclusive.

```bash
bash /home/node/dsc-extraction/scripts/extraction/extract_pool_events.sh \
  "$PROTOCOL" "$ADDRESS" "$START:$END"

bash /home/node/dsc-extraction/scripts/extraction/extract_pool_state.sh \
  "$PROTOCOL" "$ADDRESS" "$START:$END"
```

Production state uses `START:END` (step 1). Single blocks and `START:END:STEP`
remain available for explicit tests; do not downsample production coverage. The configured getter
results are required per selected block (five for each Uniswap version). Validation rejects missing, duplicate,
null, wrong-chain/address, and incorrectly sized results.

Settings are shared and overridable:

| Environment variable | Default |
| --- | --- |
| `ETH_RPC_URL` | `http://127.0.0.1:8545` |
| `CRYO_BIN` | `/home/node/.cargo/bin/cryo` |
| `PYTHON_BIN` | `python3` (PyArrow needed for state validation) |
| `CHUNK_SIZE` | 1000 |
| `MAX_CONCURRENT_REQUESTS` | 8 |
| `MAX_CONCURRENT_CHUNKS` | 8 |
| `BLOCKS_PER_REQUEST` | 1000; events only |
| `OUTPUT_DIR` | protocol/address/dataset directory above |

The installed Cryo 0.3.2 enforces request concurrency despite its misleading
"unlimited" display. Use finalized historical ranges, stable chunk boundaries,
one writer per directory, and separate directories for changed sampling/schema.
Event/state launchers do not enforce finality; state output lacks block hashes.
Inspect extraction reports: Cryo file skipping is not proof of full coverage.

## Transfer to dsc-data

The existing dedicated node SSH key permits rsync writes only under
`/home/dsc-data/data/incoming/`. Pre-create a protocol/address/batch directory on
dsc-data, then run on dsc-node after extraction finishes:

```bash
rsync -rtc --checksum \
  -e 'ssh -i /home/node/.ssh/id_ed25519_dsc_data_ingest -o IdentitiesOnly=yes -o BatchMode=yes -o UserKnownHostsFile=/home/node/.ssh/known_hosts_dsc_data -o StrictHostKeyChecking=yes' \
  "$COMPLETED_BATCH_DIR/" \
  "dsc-data@100.79.189.8:$PROTOCOL/$ADDRESS/$DATASET/$BATCH/"
```

Remote paths are relative to `data/incoming/`. Keep event and state batches
separate. Verify completed delivery before decoding; do not decode a directory
that is still receiving files. No scheduler, completion tracker or cleanup is enabled.

## Decode on dsc-data

Set KIND to `events` or `state`; INPUT and OUTPUT must be separate batch directories.
The chosen protocol selects the official ABI and state getters automatically.

```bash
/home/dsc-data/scripts/.venv-abi/bin/python /home/dsc-data/scripts/decoding/decode_pool_data.py \
  --protocol "$PROTOCOL" --address "$ADDRESS" --kind "$KIND" \
  --input "$INPUT" --output "$OUTPUT"
```

The CLI requires `--protocol`; `--abi` is an explicit override for an independently
verified compatible ABI. Protocol definitions select supported getters and ABI revisions. Decoding supports
ABI scalars, arrays, and nested tuples; a new protocol still needs a reviewed configuration. It uses Web3.py `get_event_by_topic`/`process_log` for events and eth-abi for
getter outputs, and writes separate
Parquet tables by event/getter name. Rows and manifests identify the protocol;
keep V2/V3 in separate output directories because matching event names have different
schemas. Integers are exact decimal strings, booleans remain booleans, and addresses
are strings. Arrays remain Arrow lists and tuples become named Arrow structs. Token amounts are in base units. Unnamed outputs use `arg__output_N`.

Null/malformed payloads, unknown selectors, duplicates and wrong chain/address
fail the batch. Encoded inputs remain. Output is published atomically with source,
ABI and output checksums, protocol/getter configuration and library versions.
Identical reruns verify checksums and skip. Changed inputs/configuration require a
new output directory. Decoder manifest version is now 3 (includes Web3.py version); old version-1/2 decoded
outputs are retained and must not be overwritten. Use bounded batches because
record keys are held in memory for duplicate detection.

## Reverted transactions

[reverted-extraction.md](reverted-extraction.md) describes the protocol-independent
archive. Extract once per block/configuration; reuse it for both V2 and V3 address
lists. RPC transport uses Web3.py 8.0.0 in
`/home/node/dsc-extraction/.venv-web3/` (install `scripts/requirements/rpc.txt`).
Full transaction objects are not fetched. Contract filtering runs only on
dsc-revert. The unified tracker remains planned in [plan.md](plan.md).

## Verification

The 47 tests cover Uniswap getter catalogs and decoding, Aave discovery and
structured decoding, proxy/ABI bounds, coverage/provenance, CLI arguments,
malformed inputs, resume, and adaptive tracing.
Run with dependencies from `scripts/requirements/abi.txt`:

```bash
python -m unittest discover -s tests -v
bash -n scripts/extraction/extract_pool_events.sh scripts/extraction/extract_pool_state.sh scripts/extraction/pool_common.sh
```

Existing V3 live pilot: 112 events and 15 state calls, validated previously.
Reverted live pilot: 64 failed transactions across ten blocks. Uniswap V2 live
pilot: 63 events across 100 blocks and 10 state results across two blocks; all
decoded event arguments and state values matched direct RPC queries.
Aave V3 historical pilot: 26 state results across two blocks and 11 Pool events
across 100 blocks, transferred to dsc-data and decoded; reserve values matched
direct historical calls. Aave V2 pilot: 58 state results across two blocks and
14 events across 100 blocks; all 56 reserve records matched direct calls.
See [extraction-rule.md](extraction-rule.md).

## Aave V2/V3 reserve extraction

Official ABIs are bundled. Historical deployment/ABI matching is operator setup,
not something the user needs to supply. Prepare and verify the profile after the
production contract and block range are selected. Existing pilot profiles have
been verified for their recorded test intervals.

Scope: V2 LendingPool / V3 Pool events plus `getReservesList()` and `getReserveData(asset)` at each
requested block. No user positions, aToken/debt-token events, or complete balance
sheet is implied. State calls use Cryo; only discovery is interpreted to plan
subsequent calls. ABI validation does not replace the offline decoded dataset.
The prototype runs one Cryo reserve batch per sampled block; benchmark before
large dense historical runs.

Pinned ABIs: `@aave/protocol-v2@1.0.1` for V2 LendingPool and
`@aave/core-v3@1.19.3` for V3 Pool. V2 emits `Deposit` and has its own reserve
tuple; do not decode it with the V3 ABI. These pins do **not** imply compatibility
with every Aave upgrade. Select a deployment and reviewed historical interval
before production extraction. Store its profile outside the repository
(the example uses V3; set `protocol` to `aave-v2` for V2):

```json
{
  "protocol": "aave-v3",
  "chain_id": 1,
  "address": "<Pool proxy address>",
  "from_block": 0,
  "to_block": 1,
  "implementation": "<verified implementation address>",
  "implementation_code_sha256": "<SHA-256 of runtime bytecode bytes>",
  "abi_sha256": "<SHA-256 of the pinned Pool.json file>"
}
```

The numbers above are placeholders; `to_block` is exclusive. Confirm the ABI
against official deployment/version artifacts, then record its supported block
interval and implementation bytecode hash. A profile is an explicit reviewed
input, not an automatic claim that a supplied ABI matches a deployment.
The runner checks chain, proxy implementation, runtime bytecode, and upgrade logs.
Upgrade blocks and ranges crossing upgrades are rejected; use separately reviewed
profiles/ABIs for different implementations. No current-mainnet fallback exists.

Aave runners need `scripts/requirements/abi.txt` on dsc-node as well as dsc-data.
Set `PYTHON_BIN` to that environment and `PROTOCOL` to `aave-v2` or `aave-v3`. Keep the existing three positional arguments:

```bash
PYTHON_BIN=/home/node/dsc-extraction/.venv-abi/bin/python \
OUTPUT_DIR="$BATCH/state-raw" \
bash scripts/extraction/extract_pool_state.sh "$PROTOCOL" "$ADDRESS" "$BLOCKS" \
  --deployment "$PROFILE"

PYTHON_BIN=/home/node/dsc-extraction/.venv-abi/bin/python \
OUTPUT_DIR="$BATCH/events-raw" \
bash scripts/extraction/extract_pool_events.sh "$PROTOCOL" "$ADDRESS" "$START:$END" \
  --deployment "$PROFILE"
```

Transfer the **whole batch directory**, including `deployment.json`,
`extraction.json` (state), `discovery/`, and `reserves/`. Decode on dsc-data using
the existing CLI with `--protocol "$PROTOCOL"`; it reads the copied deployment profile.
State decoding validates discovery and every expected reserve call before publishing.
The `getReserveData` table identifies the asset in `arg__input_asset`, and keeps
its result as a struct in `arg__output_0`. Aave rows carry `schema_version`;
Uniswap retains its existing schemas and manifest version 3 for compatibility.
Aave uses manifest version 4 with deployment metadata and relative source paths.

Protocol/version tables remain separate. ABI fields define the table shape;
`schema_version` in each configuration identifies that shape. Shared storage and
tracing stay independent of DEX versus lending categories. The future completion
tracker must identify the extraction configuration as well as contract and block.
