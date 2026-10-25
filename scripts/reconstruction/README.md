# Direct core-state reconstruction

The active runner is automation/reconstruct.py. It reads decoded Parquet on
dsc-data and writes one end-of-block state row for every block. Within a block,
events are ordered by transaction/log index; only the final state is written.
Blocks without relevant events carry forward the previous state.

| Protocol | Fields and transitions |
|---|---|
| Uniswap V2 | Sync replaces reserve0/reserve1 |
| Uniswap V3 | Initialize/Swap set sqrtPriceX96 and tick; Swap sets liquidity; Mint/Burn adjust active liquidity for tickLower <= tick < tickUpper |
| Aave V2/V3 | ReserveDataUpdated replaces stored indices and rates for each reserve; configurator initialization/drop events update active reserves |

Rules are adapted from pinned official subgraph handlers recorded in
subgraph-sources.json. Raw integers are preserved as decimal strings, without
token-decimal analytics rounding. Aave uses actual initial indices and reviewed
boundary snapshots rather than subgraph placeholder zeros. Dropped reserves are
removed from the active-reserve projection. No hosted Graph data is consumed.

The user removed recurring state comparisons and header downloads. Active output
is event-derived, unchecked, under reconstructed/<pool>/event-derived-unchecked-v1.
Each 10,000-block chunk is published atomically; progress retains its final state
and resume cursor. There is no per-chunk scheduler sleep. Earlier checked output
and its checked-progress.json remain as historical data, not active code.

Aave's one-time input collector runs on dsc-node via automation/aave_inputs.py.
It obtains creation/upgrade seeds with bootstrap.snapshot, follows provider
routing, and extracts supplementary raw logs with Cryo. These are inputs, not
periodic node checks. Missing reserves or input coverage fail visibly.

Files: core.py supplies field handlers; direct.py reads/replays/writes Arrow
state tables; bootstrap.py reads initial Aave state; routing.py interprets
provider proxy changes. ABI sources live under abis/. The old coordinator-based
capture, acceptance and replay adapters have been removed.

This schema excludes user positions, full fee/oracle accounting, Aave normalized
(time-accrued) views, and complete EVM storage. No node-verified completion or
legacy tracker state bits are claimed by direct replay.
