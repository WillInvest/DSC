# Active DSC scripts

The current entrypoint is `main.py`, backed by `pipeline/`: bounded incremental
cycles and continuous finalized catch-up across events, local core state,
receipts, hosted/RETH traces, and accepted selected data. See `pipeline/README.md`.
It reuses the fast workers below. The previous fixed-target services are retained
for rollback but disabled once the continuous coordinator is enabled.

Retired standalone extraction/selection and comparison entrypoints live in
`../obsolete-scripts/`, with a recovery map. Shared decoders and the preserved
full getter compatibility path remain here because they are still dependencies.

| Folder | Responsibility |
|---|---|
| event-cryo-extraction | Sequential Cryo extraction on dsc-node, transfer queue on dsc-data, private web monitor |
| decoding | Existing Web3.py/eth-abi decoder and parallel Parquet file runner |
| reconstruction | Official-subgraph-derived core field updates and direct every-block replay |
| automation | Decode-first scheduling, one-time Aave inputs, watchdogs, terminal status |
| protocols | Official ABIs, historical profiles' ABI sources, protocol configuration |
| registry | Contract-list and historical ABI profile preparation tools |
| reverted | Receipt scan, receipt acceptance, dRPC/RETH tracing, archive selection and delivery; invoked by pipeline |
| shared | Existing JSON/receipt/file helpers reused by active scripts |
| validation | Input/state validators required by the retained ABI decoder |
| requirements | Pinned Python dependencies |
| monitor/export.py | Compatibility entrypoint for the existing restricted SSH monitor key |

The worker entrypoints are event-cryo-extraction/run.py and the offline
decode/reconstruction runners. `main.py` coordinates them; there is no retired
top-level scripts/run.py or legacy orchestration tree.
The monitor lives on dsc-web; its deploy location remains
/home/dsc-web/sequential-events. Native Cryo monitoring remains in cryo-events.

See automation/README.md for scheduling and recovery, decoding/README.md for
Parquet decoding, and reconstruction/README.md for state semantics.
Virtual environments, data, run receipts, credentials and runtime logs are not
source code. Keep them out of Git. Tests live at the repository root in tests/.
