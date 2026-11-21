# DSC Node pipeline

`/home/node/dsc-scripts` contains a versioned copy of the pipeline.

| Folder | Responsibility |
| --- | --- |
| `events-extraction/` | Cryo extraction, ABI decoding, protocol profiles, and the tracked registry |
| `state-replication/` | Market state replay and the supplementary Aave inputs |
| `revert-extraction/` | Failed receipts, dRPC trace workers, and tracked-contract filtering |
| `hf-computation/` | Aave wallet and cohort HF extraction, replay, and validation |
| `engine/` | The tested runtime paths, shared modules, catalog writers, and contracts |
| `main.py` | Mount check and one pipeline pass |

The four stage folders expose source from the same Git release. This keeps
imports and worker entrypoints consistent when browsing or deploying a revision.

`python3 /home/node/dsc-scripts/main.py check` verifies the data mount and
prints the planned production output paths. Future scheduling can invoke
`python3 /home/node/dsc-scripts/main.py run --config CONFIG` after the
Node production handoff. A pass commits the finalized market cycle, publishes
its protocol directories, accepts one HF window when available, and advances
each Drive backup. The launcher checks the mounted data disk before writing.

The configuration supplies `market_baseline`, `market_profiles`,
`market_verification`, `production_fence` and `revert_ssh_config` as local
paths. Supply `hf_opening`, `hf_history`, `cryo` and `hf_helpers` for the
accepted HF checkpoint, frozen inputs, Cryo executable and helper configuration.
`market_drive` and `hf_drive` identify the approved Drive roots.
`hf_backup_start` is the first inclusive block for the configured HF archive.
The Node market ledger and canonical catalog live under
`/mnt/storage/dsc-data/metadata/market`. The HF window, work and archive
cursors live under `/mnt/storage/dsc-data/lending/aaveV3/health-factor`.
