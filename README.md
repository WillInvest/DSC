# Single-pool Uniswap V3 event extraction

The example is Ethereum mainnet USDC/WETH, fee tier 3000 (0.30%):
`0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8`.
Uniswap uses this pool in its [query examples](https://developers.uniswap.org/docs/ecosystem/subgraphs/guides/v3-query-examples).
The extractor checks its factory, tokens, fee, and factory registration on-chain.

Run extraction on **dsc-node**. Python 3.9+ standard library is sufficient on Linux/macOS;
no package installation is needed. RETH HTTP RPC was verified at
`http://127.0.0.1:8545` on dsc-node. This is not a database endpoint.

```bash
python3 scripts/pool_events.py \
  --address 0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8 \
  --from-block 13000000 --to-block 13000999 --chunk-size 250 \
  --output output/usdc-weth-3000
```

This inclusive 1,000-block pilot produced **112 events**: 82 Swap, 8 Mint,
13 Burn, and 9 Collect. It is a sample, not the pool's full history.
The example JSON records these inputs/results for reference; the CLI takes explicit flags.

All logs emitted by the address are fetched with `eth_getLogs`, without a topic filter.
The nine events from the [official pool interface](https://github.com/Uniswap/v3-core/blob/main/contracts/interfaces/pool/IUniswapV3PoolEvents.sol)
are decoded: Initialize, Mint, Collect, Burn, Swap, Flash,
IncreaseObservationCardinalityNext, SetFeeProtocol, and CollectProtocol.
Unknown event signatures remain in the output with null `event` and `args`.
No state sampling, transaction replay, reverted-transaction extraction, or scheduling
is performed by these scripts. Reverted-transaction routing remains separate.

## Output and resume

Each batch produces `FROM-TO.jsonl.gz` and updates `manifest.json` only after the
batch is durable. Each JSON line includes the original RPC log, decoded event and
arguments, block hash/number/timestamp, transaction hash/index, log index, address,
and chain ID. ABI integers are decimal **strings** to preserve all 256 bits;
amounts are raw token units, not decimal-adjusted amounts. Block numbers, timestamps,
and indexes are JSON numbers. Deduplicate downstream by `(chain_id, block_hash, log_index)`.

The manifest records pool metadata, covered ranges (including empty batches), row
counts, end-block hashes, and SHA-256 checksums of compressed files. Consumers must
read only files referenced by the manifest; an interrupted write may leave an
unreferenced batch or `.tmp` file, which the next attempt can overwrite.

Rerun the same command to resume; change only `--to-block` to extend coverage.
`--to-block finalized` snapshots the node's finalized head at startup.
Address, starting block, chain, and schema must match the existing output.
A file lock prevents simultaneous extraction or transfer from the same directory.
Transport failures retry four times; RPC errors stop without advancing the current
batch. For a provider range limit, rerun with a smaller `--chunk-size`.

Only finalized blocks are accepted. Resume verifies chunk checksums and the last
stored block hash; extraction checks log block hashes and the batch boundary.
A changed finalized hash stops extraction for investigation; there is no automatic
rollback of published data. Do not mix different runs in one output directory.

## Direct transfer to dsc-data

After extraction, run **on dsc-node**:

```bash
python3 scripts/send_events.py \
  --source output/usdc-weth-3000 \
  --destination-dir /home/dsc-data/dsc-events/usdc-weth-3000
```

This requires `rsync` and working, verified noninteractive SSH login from dsc-node
to `dsc-data@100.79.189.8`. Use a dedicated destination directory for each extraction
configuration and a single sender per destination. The script verifies local checksums,
locks the source, transfers data files with rsync checksum comparison, then publishes
the manifest last. Retry the same command after a failed transfer. It does not delete
remote files. Consumers must treat the manifest as the committed coverage.
Credentials belong outside this repository.

**Current transfer limitation:** direct node-to-data SSH was not configured at the
pilot check (host verification failed and no standard public-key files were found
in the node user's `.ssh` directory). The destination's ED25519 fingerprint was
cross-checked through the existing trusted connection, but authentication setup
was not changed. Transfer and PostgreSQL ingestion have not been run. These scripts
land files on dsc-data; a database ingestion adapter is a later step, not implicit
in a successful file transfer.

## Verification and pilot location

```bash
python3 -m unittest discover -s tests -v
python3 -m py_compile scripts/pool_events.py scripts/send_events.py
```

The live pilot ran on dsc-node under `/home/node/dsc-pool-events-pilot/`, with data
under `output/usdc-weth-3000/`. All 112 stored raw logs matched an independent
full-range RPC query, every event decoded, and a second run made no new batches.
No existing extraction services were changed and no recurring job was installed.

For another Ethereum Uniswap V3 pool, supply its address, start block and a separate
output directory. Before a full backfill, establish the pool creation block and
coverage target. Scaling to many pools should add orchestration and database ingestion
while keeping extraction on dsc-node and application storage on dsc-data.

Validation notes: four automated tests pass, plus the live RPC comparison and resume
check above. Simplification review completed. Code review: skipped (ce-code-review
unavailable) — its scope resolution could not establish a base in this newly
initialized repository, which had no HEAD or remote. A manual source review was
performed instead. Live SSH transfer is still unverified.
