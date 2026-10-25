# Active DSC scripts

| Folder | Responsibility |
|---|---|
| event-cryo-extraction | Sequential Cryo extraction on dsc-node, transfer queue on dsc-data, private web monitor |
| decoding | Existing Web3.py/eth-abi decoder and parallel Parquet file runner |
| reconstruction | Official-subgraph-derived core field updates and direct every-block replay |
| automation | Decode-first scheduling, one-time Aave inputs, watchdogs, terminal status |
| protocols | Official ABIs, historical profiles' ABI sources, protocol configuration |
| registry | Contract-list and historical ABI profile preparation tools |
| reverted | Existing failed-transaction extraction, archive transfer and selection; unchanged and not launched by this pipeline |
| shared | Existing JSON/receipt/file helpers reused by active scripts |
| validation | Input/state validators required by the retained ABI decoder |
| requirements | Pinned Python dependencies |
| monitor/export.py | Compatibility entrypoint for the existing restricted SSH monitor key |

The active data-side entrypoints are event-cryo-extraction/run.py and
automation/run.py. There is no top-level scripts/run.py or legacy coordinator.
The monitor lives on dsc-web; its deploy location remains
/home/dsc-web/sequential-events. Native Cryo monitoring remains in cryo-events.

See automation/README.md for scheduling and recovery, decoding/README.md for
Parquet decoding, and reconstruction/README.md for state semantics.
Virtual environments, data, run receipts, credentials and runtime logs are not
source code. Keep them out of Git. Tests live at the repository root in tests/.
