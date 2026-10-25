## Direct reconstruction revision, 2026-10-25

The user explicitly requested straightforward event reconstruction without the
recurring node checks. This supersedes the 1,000-block comparison requirement
below for the active automatic event pipeline. Replay decoded events on dsc-data
into every-block core state, without per-block headers or periodic getter
comparisons. Preserve the checked mode and its outputs separately. Label direct
outputs event-derived and unchecked; do not claim node-verified state coverage.
Aave boundary initial conditions and configurator/provider events remain required
inputs, collected once rather than as recurring validation. Decode-ready work
has priority; otherwise continue direct replay without per-chunk scheduler waits.

# Event extraction and core market reconstruction

## User revision, 2026-10-24

The user selected **core market fields only** and authorized event-driven
end-of-block reconstruction with periodic node comparisons. This supersedes the
older prohibition on reconstruction and the older full-getter production scope
below. The full getter mode remains available for independent validation.

- Retain all Pool events. Cryo extracts encoded logs on dsc-node; dsc-data decodes
  them using the existing official ABI/Web3.py path and replays core state there.
- Core fields: Uniswap V2 reserve0/reserve1; Uniswap V3 sqrtPriceX96, tick and active
  liquidity; Aave V2/V3 stored liquidity/variable-borrow indices and liquidity,
  variable-borrow and stable-borrow rates for each active reserve.
- Supplement Aave Pool logs with canonical configurator initialization/drop logs
  and provider routing evidence. New idle reserves must not be omitted.
- Record a core state for **every block**, comparing exact integers with node
  getters at intervals of at most **1,000 blocks**, including final partial
  intervals. Creation/upgrade boundaries require explicit bootstrap evidence and
  the reviewed historical ABI. Aave indices here are stored getter values, not
  time-accrued normalized-income/debt views.
- Mismatches leave the interval unaccepted and preserve evidence; never replace a
  failed endpoint and claim the preceding interval was reproduced correctly.
- Use a separate core-state identity; preserve accepted events, full getter data
  and the independent reverted archive. Keep 32 pool workers × 2 Cryo requests.
- Node checkpoint calls use Web3.py's ABI-aware contract API. This replaces bulk
  per-block Cryo state calls in core mode; Cryo remains the bulk event extractor.

See [the implementation contract](scripts/reconstruction/README.md) for field
exclusions, exact data representation, provenance, routes and acceptance.

## Sequential production order, 2026-10-24

Finish all events for all tracked addresses and reviewed profiles first, including
Aave configurator/provider logs. Then extract sparse state checkpoints on the node
and reconstruct intermediate every-block core state on dsc-data. Only after that
phase is accepted may failed-transaction tracing/archive/filter work begin.
Each phase requires complete configuration-scoped, verified destination evidence.
Preserve already accepted full state, core state and reverted data for reuse.
The 32×2 event setting does not imply 64 native trace workers; trace concurrency
stays unchanged pending a measured tracing benchmark.

## Earlier full getter rule (retained for legacy RPC mode)

Status: **Original full getter decision, 2026-10-23; superseded for core-mode production by the revision above.**
This replaces the previous decision to decode events inside Cryo. Change it only
with explicit user direction.

1. **dsc-node extracts; dsc-data decodes.** Use Cryo to extract encoded event logs
   and direct end-of-block state-call results. Transfer completed Parquet files
   directly to dsc-data. Do not reconstruct state from events.
2. **Use maintained ABI libraries for both datasets.** Decode events on dsc-data
   with Web3.py `get_event_by_topic` and `process_log`; decode getter outputs with
   `eth-abi`. Use `eth-utils` for getter selectors and PyArrow for Parquet. Keep custom code limited to file handling, field mapping,
   provenance, and project validation. Use the official version-pinned protocol ABI.
3. **Events:** one Cryo `logs` job per emitting contract/range, with no `--event-signature` and
   no event-specific `--topic0` filter. Preserve all raw topics and data, block
   number/hash, transaction hash/index, log index, address, and chain ID. Decode
   each log according to topic0 on dsc-data; unknown or malformed logs fail the
   decoding batch rather than being silently dropped.
4. **Block-state:** For V3, Cryo `eth_calls` reads the five agreed getters directly at
   each requested block: `slot0()`, `liquidity()`, both global fee-growth getters,
   and `protocolFees()`. Save selectors and encoded return bytes. These are core
   pool snapshots, not complete ticks/positions. Reject missing, duplicate, null,
   or malformed results before accepting coverage. Decode outputs with the selected official ABI.
   For V2 pairs, use `getReserves()`, `totalSupply()`, `price0CumulativeLast()`,
   `price1CumulativeLast()`, and `kLast()`. These are stored values; reserves are not
   guaranteed to equal actual token balances. Cumulative prices are not extrapolated.
   For Aave V2/V3, discover reserves with `getReservesList()` at each requested block,
   then extract `getReserveData(asset)` for each returned asset. Decode the discovery
   list on dsc-node only to construct calls; publish decoded datasets on dsc-data.
   This covers reserve-level Pool data, not user positions or token-contract events.
5. **ABI:** standard Uniswap V3 pools use official `@uniswap/v3-core@1.0.1` in
   `scripts/protocols/dex/uniswap/v3/UniswapV3Pool.json`; provenance is in `scripts/protocols/README.md`.
      V2 uses official `@uniswap/v2-core@1.0.1` in `scripts/protocols/dex/uniswap/v2/UniswapV2Pair.json`.
   Protocol configurations and getter catalogs live under `scripts/protocols/`.
   Explicit protocol and address arguments are required; there is no default pool.
   Aave V3 uses the pinned official Pool ABI under `scripts/protocols/lending/aave/v3/`.
   V2 uses `@aave/protocol-v2@1.0.1` under `scripts/protocols/lending/aave/v2/`.
   Each version has its own event/reserve schema. The historical deployment profile bounds supported blocks and pins ABI/runtime
   bytecode; proxy upgrades require separate reviewed support. Other protocols or
   modified pools require the corresponding official ABI.
6. **Decoded output:** write separate Parquet tables by event/getter name. Preserve
   protocol and identifying metadata and map outputs to ABI field names. Store integers as exact
   decimal strings, booleans as booleans, and addresses as strings. Amounts remain
   in token base units. Preserve arrays as lists and tuples as structs. Record source checksums, ABI hash, decoder/library versions,
   row counts, and output checksums in the batch manifest.
7. **Retention and cleanup:** encoded files are staging, not a mandatory permanent
   dataset. Retain them until transfer, decoding, coverage validation, durable storage,
   and the applicable backup policy are satisfied. Preserve failed inputs for retry.
   Delete node staging only after verified delivery and a durable completion record.
   This implementation does not enable automatic deletion or invent a backup policy.
8. **Progress:** use the combined completion design in `plan.md`. A decoding manifest
   proves the supplied records decoded, not that the original block range was fully
   extracted or delivered. Do not mark completion based only on file existence or
   successful decoding. Completion tracking and automated coverage reconciliation
   remain to be implemented.
9. **Execution settings:** explicit finalized historical block selections; Cryo range
   ends are exclusive. Events use 1,000 blocks per output chunk and per log request.
   Both launchers use up to eight concurrent chunks/requests. State calls are separate
   per getter/block; log request sizing does not batch `eth_call`. These starting
   settings can be tuned. Use stable boundaries, dedicated dataset/schema directories,
   and one writer. The launchers do not yet enforce finality or maintain block hashes
   for the `eth_calls` dataset.
10. **Server roles:** scripts live under `/home/dsc-data/scripts/`, data under
    `/home/dsc-data/data/`, with runner copies on dsc-node. Failed-transaction replay
    remains on dsc-node, full trace retention/filtering on dsc-revert, and selected
    reverted-data delivery to dsc-data. Event/state staging cleanup never deletes
    the full failed-transaction archive on dsc-revert.

## Production coverage (user decision, 2026-10-23)

- For every address in the tracked list, start at its verified deployment block
  (inclusive), not Ethereum genesis or the first observed transaction in a sample.
- Collect all events and the configured end-of-block state at **every block**;
  the production state step is 1, with no downsampling. Aave user-position
  snapshots remain excluded.
- Interpret current as the latest finalized block when a run is planned. Record
  that block number/hash as the fixed high-water mark; Cryo's exclusive END is
  that block number + 1. Subsequent catch-up runs extend coverage as finality advances.
- Resolve creation/initialization and upgrade boundaries explicitly. Cover the
  full requested history with appropriate historical ABIs; do not silently skip
  blocks rejected by the current deployment-profile guard. If a getter genuinely
  does not exist yet, record its applicability separately from missing work.
- The contract list and completion tracker remain pending. This records the
  coverage policy; it does not start a backfill or enable an automatic schedule.

## Verified pilot

- Raw single-pass event extraction: 112 logs, blocks 13,000,000–13,000,999.
- Direct transfer to dsc-data: source/destination SHA-256 checksums matched.
- Offline event decoding: 82 Swap, 8 Mint, 13 Burn, 9 Collect; every argument matched
  the earlier validated sample.
- Offline state decoding: 15 getter results across blocks 13,000,997–13,000,999.
- All encoded inputs and previous pilot outputs are retained.

References: [Cryo](https://github.com/paradigmxyz/cryo),
[eth-abi decoding](https://eth-abi.readthedocs.io/en/stable/decoding.html),
[Uniswap V3 core](https://github.com/Uniswap/v3-core).

## Aave compatibility pilot (2026-10-23)

- Historical Ethereum Pool reference: `0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2`;
  this is a validation sample, not a production contract selection.
- State blocks 17,000,000–17,000,001: 2 reserve lists and 24 reserve records,
  all reserve fields matched direct historical Web3 contract calls.
- Events blocks 17,000,000–17,000,099: 11 Pool logs, matching the direct RPC count.
- Cryo ran on dsc-node, files transferred directly to dsc-data, and decoded there.
- Uniswap compatibility: 112-event and 15-state pilot outputs and manifests
  remained byte-for-byte identical after the refactor.

## Aave V2 compatibility pilot (2026-10-23)

- Historical Ethereum LendingPool reference: `0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9`;
  validation only, not production contract selection.
- State blocks 13,000,000–13,000,001: 2 reserve lists and 56 reserve records
  (28 reserves per block); every reserve field matched direct historical calls.
- Event blocks 13,000,000–13,000,099: 14 logs extracted and decoded, matching RPC.
- Shared Cryo runners and ABI decoder required no code changes; only the pinned
  V2 ABI/configuration, tests, and documentation were added.

## Uniswap V2 live validation (2026-10-23)

- Example USDC/WETH pair: `0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc`.
  Factory and token identities were checked on-chain; this is a validation choice,
  not a production contract selection.
- Events: blocks 13,000,000–13,000,099, 63 logs; every decoded argument matched
  a direct RPC log parsed with the official ABI.
- State: blocks 13,000,000–13,000,001, 10 getter results; all values matched
  direct historical contract calls.
- Extraction ran on dsc-node, transferred directly to dsc-data, and decoded there.
- Node artifacts: `/home/node/dsc-extraction/data/validation/uniswap-v2-pilot/`.
  Data artifacts: `/home/dsc-data/data/incoming/uniswap-v2-pilot/` and
  `/home/dsc-data/data/decoded/uniswap-v2-pilot/`.
- Aave V2 and V3 saved profiles were rechecked against the node for their respective
  `13000000:13000100` and `17000000:17000100` test intervals. This does not claim
  ABI compatibility for all historical upgrades; matching additional intervals is
  part of operator setup once the production range is selected.
