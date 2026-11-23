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
The local market, Revert and raw-event publication records must already
cover the same sealed baseline.
For passes that include backup, `market_drive` and `hf_drive` identify the
approved Drive roots. `hf_backup_start` is the first inclusive block for the
configured HF archive.
The Node market ledger and canonical catalog live under
`/mnt/storage/dsc-data/metadata/market`. The HF window, work and archive
cursors live under `/mnt/storage/dsc-data/lending/aaveV3/health-factor`.

## Finalized-block schedule

`dsc-node-research-pass.timer` triggers `dsc-node-research-pass.service`
every five minutes. The service runs the local-only launcher as `node`,
reads `/etc/dsc/research-pass.json`, and binds the image referenced by
`/etc/dsc/research-hf-input` read-only at `/home/dsc-data` in its own
mount namespace. The deployment config and image link must point to the
same verified HF input. systemd leaves an active pass running when the
timer elapses, and the launcher also takes the shared scheduled-pass lock.
Drive uploads use separate jobs.

Check the next trigger with `systemctl list-timers dsc-node-research-pass.timer`.
Use `journalctl -u dsc-node-research-pass.service` and the market ledger's
`progress.json` to see the running interval and last accepted block.
Verify HF independently using its local accepted windows.

The read-only `dsc-node-research-monitor.service` starts at boot. Its Node
config is `/etc/dsc/research-monitor.json`, its code resolves through
`/mnt/storage/dsc-monitor-releases/current`, and its latest local snapshot is
`/mnt/storage/dsc-data/metadata/node-monitor-progress.json`. The monitor probes
the configured worker SSH endpoints on the same LAN routes the coordinator
uses. A reachable endpoint confirms a TCP connection. An assigned task or
fallback indicator comes from the coordinator and HF worker receipts.
The existing fixed-command SSH receiver publishes the filtered snapshot to
the CU1 private monitor. Check `/api/progress` on the monitor for the sample age,
accepted watermarks, and Drive progress.

## Compute unit names

| Operator label | Current host | Node connection |
| --- | --- | --- |
| CU1 | `dsc-compute` and `dsc-web` on the same host | `dsc-hf-compute` |
| CU2 | `dsc-data` | `dsc-hf-data` |
| CU3 | `dsc-revert` | `dsc-revert-pipeline` |

The labels identify the existing workers. CU1 also serves the private DSC
monitor through the `dsc-compute` and `dsc-web` tailnet names. Preserve its
monitor service and endpoints during the transition. Retire legacy research
copies after Node acceptance and the matching Drive restore verification.
Before changing a worker connection, check `hf_helpers` or `revert_ssh_config`
and verify the worker can accept a Node task and return its result.

## Release recovery

Use the source tar and Git bundle under
`/mnt/storage/dsc-script-releases/source-recovery/<commit>/`. After extracting
the source into an isolated directory, run `layout-release.sh` from
`scripts/pipeline/deployment/node/` with that directory as its argument.
Extract `runtime-supplement/hf-generated-runtime.tar.gz` into the resulting
`engine/` directory. Its `restore-validation.json` records the expected hashes
and tested runtime versions. The supplement provides the GHO and math kernels,
the lockfile-pinned JavaScript packages and the Python dependency overlay.
The launcher still needs the Node Python environment and the verified HF input
image described above.
