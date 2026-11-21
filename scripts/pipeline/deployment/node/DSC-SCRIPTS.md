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

Use `python3 /mnt/storage/dsc-scripts/main.py run --local-only --config CONFIG`
to finish after publishing the local market and HF outputs. The HF cursor
advances only after the accepted wallet and cohort window has been placed
under `lending/aaveV3/health-factor/local-accepted/`.
Historical maintenance can call `pipeline.hf_storage.place_local` with a
`link_source` under `/mnt/storage` when the inventory records the original
bind-mounted path. Both paths must identify the same window directory.

For the consolidated Node runtime, set `hf_input_image` to the verified
image on `vdb1` and `hf_python_overlay` to the deployed NumPy overlay on the
same disk. Mount the image read-only at `/home/dsc-data` in the process
namespace so the pinned HF receipts resolve their original paths.
Run a single local pass as the Node user with:

```sh
sudo systemd-run --wait --pipe --uid=node \
  --property="BindReadOnlyPaths=${HF_INPUT_IMAGE}:/home/dsc-data" \
  /usr/bin/python3 /mnt/storage/dsc-scripts/main.py run --local-only --config "$CONFIG"
```

The configuration supplies `market_baseline`, `market_profiles`,
`market_verification`, `production_fence` and `revert_ssh_config` as local
paths. Supply `hf_opening`, `hf_history`, `cryo` and `hf_helpers` for the
accepted HF checkpoint, frozen inputs, Cryo executable and helper configuration.
The HF checkpoint must start at the block after the market baseline and
carry its exact predecessor hash before the launcher opens a writer.
For passes that include backup, `market_drive` and `hf_drive` identify the
approved Drive roots. `hf_backup_start` is the first inclusive block for the
configured HF archive.
The Node market ledger and canonical catalog live under
`/mnt/storage/dsc-data/metadata/market`. The HF window, work and archive
cursors live under `/mnt/storage/dsc-data/lending/aaveV3/health-factor`.
