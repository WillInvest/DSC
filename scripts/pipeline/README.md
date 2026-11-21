# Continuous finalized pipeline

Entry point on dsc-data:

```sh
/home/dsc-data/scripts/.venv-abi/bin/python /home/dsc-data/scripts/main.py once --end-block 26300000
/home/dsc-data/scripts/.venv-abi/bin/python /home/dsc-data/scripts/main.py continuous
/home/dsc-data/scripts/.venv-abi/bin/python /home/dsc-data/scripts/main.py status
```

`once` without an ending block captures the current finalized block. `continuous`
finishes/resumes the current cycle, captures finalized again, and repeats. When
there are no new finalized blocks it waits 30 seconds. It never follows a moving
target inside a cycle or uses an unfinalized latest block.

Order: Aave ABI/profile preparation → Cryo event extraction and acceptance → ABI
decode → local every-block state → block receipt scan → receipt acceptance on
revert → dRPC/RETH traces and contract selection → selected acceptance on data.
State remains `event-derived-unchecked-v1`; no recurring state comparisons.

The coordinator owns `/home/dsc-data/runs/pipeline/tracker.sqlite`. `cycles` pins
the start, ending block and hash; `stages` records acceptance evidence and retries;
`position` advances transactionally only after all stages commit. `progress.json`
is the monitor view, not the source of acceptance. A flock permits one coordinator.
Systemd restarts failed runs after 60 seconds; the same target/stage resumes.

After each completed cycle, the coordinator publishes backfill-aligned canonical
files and retires verified cycle data fragments. Events/state use the original
pool-specific 10,000-block grid; receipts/traces use the original 1,000-block grid.
The newest partial interval grows across cycles. See [compaction](compaction/README.md)
for roots, schema separation, publication, recovery and canonical reading.
Worker recovery retains the prior one-hour no-progress limit; a fresh coordinator
heartbeat alone does not establish worker progress. The service's ExecStartPre
requires the exact 100-block replay verification to have passed.

The historical baseline is preserved at `runs/sequential-events`, `runs/event-pipeline`,
the protocol dataset directories, and reverted `historical-v1`/`drpc-v1` archives.
It is adopted only after existing state checkpoints and reverted counters are complete.

New cycle paths:

| Host | Root |
|---|---|
| data: plans, profiles, stage logs | `/home/dsc-data/runs/pipeline/cycles/START-END/` |
| data: encoded, decoded, state, selected | `/home/dsc-data/data/incremental/START-END/` |
| node: event output and receipts | `/home/node/dsc-extraction/cycles/START-END/` |
| revert: receipts, complete traces, selected | `/home/dsc-revert/data/cycles/START-END/` |

Each cycle has new worker identities and does not rewrite prior partial chunks.
For each address, the state checkpoint must join the new start exactly; its hash
is pinned in the new replay identity. After compaction, read the canonical catalog
and the preserved historical prefix as described in the compaction guide. Cycle
metadata remains in the ledger; its retired dataset fragments are no longer the
read path.

Aave profiles extend only after Cryo bulk upgrade-log extraction and native RPC
checks of implementation storage slots and pinned runtime code. Upgrade-log
collection reuses the event worker's chunking, retries and empty-result evidence.
An upgrade invokes the existing verified
Blockscout/Sourcify profile builder; new schema handlers still fail visibly if
unsupported. Supplemental provider/configurator events are collected for the new
interval. Existing state carries forward; node seeds occur at reviewed boundaries.

Before enabling production, repeat the final accepted 100 blocks:

```sh
/home/dsc-data/scripts/.venv-abi/bin/python /home/dsc-data/scripts/pipeline/verify.py --blocks 100
```

The replay uses a separate ledger, progress pointer and datasets under
`runs/pipeline/verification/isolated/START-END`. Node and Revert use separate
verification directories keyed by the local run root. The archival Aave ABI
profiles must cover the interval before the replay starts. The replay audits them
on Node and uses the existing ABI files for offline decoding.

The 100-block run publishes the `verification.json` gate at the verification
root. A shorter diagnostic run leaves the gate in place.
`verification.json` records per-address event/decoded/state matches, exact failed
receipts, trace comparisons, and selected transaction/contract matches. All call
structure, addresses, gas, values and raw input/output must match. Human-readable
provider error/revert strings must also match exactly. Any provider formatting
difference fails the gate and is recorded for inspection. No production coverage moves.

The isolated Node candidate passed this gate for blocks 26,266,452 through
26,266,551, covering the 102 registered contracts and 294 complete failed
transaction traces. The verification ledger records the exact event, decoded
event, state, receipt, trace and contract-selection comparisons.

For a candidate installed on Node, set `DSC_PIPELINE_SITE=node` and
`DSC_PIPELINE_SCRIPTS` to the candidate's `scripts` directory. The candidate
loads the pool registry from `contracts/ethereum/pools.csv` alongside that
directory. Run `pipeline/verify.py` with explicit `--root`, `--baseline-plan`,
`--profiles` and `--revert-baseline` paths on Node. Its Revert baseline points
to the Node copy of the historical `drpc-v1` archive root.
RETH, Cryo, receipt collection and Aave auxiliary collection run locally.

Set `DSC_PIPELINE_REVERT_SSH_CONFIG` to the dedicated SSH client configuration
with the alias `dsc-revert-pipeline`. Its public key on Revert has a Node LAN
source restriction and the forced `scripts/reverted/node_worker.py` command.
The worker pulls receipts from the matching Node verification scope, starts the
existing tracing script with local credentials, and serves only redacted
progress and read-only rsync downloads under the worker's data root. Node checks
full archives against the same sealed batch receipts as the selected output.
Reuse the deployed Python environments.

The normal `once` and `continuous` commands also run on Node with an explicit
coordinator root, accepted baseline plan, reviewed profile directory and
verification directory. The baseline must include end-of-block state
checkpoints for the 102 tracked contracts, and the Node replay gate must
validate all three market layers, failed receipts, full traces and selection.

```sh
DSC_PIPELINE_SITE=node python scripts/main.py once \
  --root /node/coordinator --baseline-plan /node/accepted/events/plan.json \
  --profiles /node/accepted/profiles --node-verification /node/verified-replay \
  --end-block 26266552
```

The Node coordinator writes a unified Parquet and JSONL catalog under
`<root>/canonical`. The historical catalogs keep their existing partitions.
The new catalog records its predecessor and anchors its 10,000-block market
and 1,000-block receipt/trace partitions at the first block of the new storage
epoch. It holds encoded events, decoded events, every-block state, failed
receipts, full traces and selected traces. Accepted stage inputs stay in
place during the candidate run. If the catalog commit is interrupted after
extraction acceptance, rerunning the command resumes publication without
repeating the accepted extraction stages. `compaction.json` records the
independent publication watermark.

For a handoff from verified Node imports, `pipeline.node_handoff` reads the
accepted Data and Revert catalogs at their common ending block. It checks
every state payload it uses against its catalog SHA-256 and reads the single
terminal Parquet row for each of the 102 contracts. It independently compares
failed receipts with RETH and checks the full traces and both copies of the
selection. The resulting directory contains sealed checkpoints, `plan.json`
for the normal Node entrypoint, and `handoff.json` with the exact source
identities. The entrypoint binds that receipt when it initializes the Node
ledger. The source catalogs remain readable and can continue to serve the
historical data.

For shadow verification, run `pipeline.catalog_probe probe CATALOG BLOCK`
on Node, Data and Revert for the same accepted block. The command
`pipeline.catalog_probe compare NODE_JSON DATA_JSON REVERT_JSON` checks the
encoded events, decoded events and all 102 state rows. It also checks the
failed receipts, complete call trees and contract matches. The fingerprints
compare values across the different physical partition layouts.

During shadow operation, `pipeline.node_status aggregate CONFIG --follow
--output REPORT` publishes a read-only combined view. The configuration points
to the Node coordinator and its comparison, handoff, HF and backup progress
files. It also specifies the existing Node-only Data SSH alias, whose fixed
`dsc-pipeline-status-v1` command returns the legacy producer's selected fields.
The report keeps the calculation and Drive watermarks separate, includes
source file timestamps, and preserves Node status when Data cannot be reached.
Run `scripts/main.py status --status-config CONFIG` for an on-demand report.
The existing status command without that option still prints the coordinator's
own progress file.

To show Node status at the existing private monitor URL, the Node status
configuration can include `publish_monitor` with an existing restricted SSH
`host` and `ssh_config`. That alias must route the exact
`dsc-node-monitor-v1` command to `pipeline.node_status receive-monitor`
with a fixed output path. Keep the receiver's directory writable only by
the receiver and readable by the web service. The monitor's
`DSC_NODE_MONITOR_CACHE` path defaults to
`/var/lib/dsc-pipeline-monitor/node-progress.json`. The receiver accepts a
bounded JSON payload and stores only block watermarks, counts and status
labels. The private page shows this report independently of the legacy Data
connection and flags stale reports.

### Node production services

`deployment/node/dsc-node-pipeline.target` groups the market coordinator, HF
computation, independent Drive backups and the read-only status publisher.
Start and stop the full set through the target. Each service retries
independently using its own lock and cursor. The HF worker reads committed
market blocks from the Node catalog and the HF backup reads accepted windows.

Set the following fields in the root-managed `/etc/dsc/node-pipeline.env`.

| Fields | Value |
| --- | --- |
| `DSC_PIPELINE_SCRIPTS`, `PYTHONPATH` | Versioned scripts directory and its runtime module paths, including `scripts/user-history` and the deployed Drive modules |
| `DSC_PIPELINE_REVERT_SSH_CONFIG` | Existing restricted Node-to-Revert SSH client configuration |
| `MARKET_ROOT`, `MARKET_BASELINE`, `MARKET_PROFILES`, `MARKET_VERIFICATION` | Node coordinator directory, sealed handoff plan, reviewed profiles and accepted replay report directory |
| `MARKET_BACKUP_WORK`, `MARKET_DRIVE` | Market Drive cursor directory and remote root |
| `HF_OPENING`, `HF_HISTORY`, `CRYO`, `HF_WINDOWS`, `HF_WORK`, `HF_HELPERS` | Accepted HF successor at the market epoch boundary, frozen HF inputs, Cryo executable, accepted window directory, new epoch work directory and helper configuration |
| `HF_BACKUP_WORK`, `HF_DRIVE`, `HF_BACKUP_START` | Existing HF archive cursor directory, matching Drive remote and original first block |
| `STATUS_CONFIG`, `STATUS_REPORT` | Absolute paths to the combined read-only source configuration and report |

Before enabling the target, finish the last Data-owned cycle, fence
`dsc-pipeline.service` on Data and verify it is inactive and masked.
Capture the accepted boundary and confirm the Node and Drive restores. Seal
the Node handoff at that block. The root-managed
`/etc/dsc/node-production-fence.json` must contain `format`
(`dsc-node-production-fence-v1`), `legacy_writer` (`dsc-pipeline.service`),
`legacy_state` (`masked`), the resolved `coordinator_root`, the inclusive
`through` and the `handoff_sha256` of `handoff.json`. The public Node
continuous command validates this file against the chosen baseline and root
before opening the coordinator. Bounded shadow runs use the existing
`once` command.

Close the old HF follower at the same boundary using the accepted legacy
catalog and point `HF_OPENING` to its successor checkpoint. Stop the
temporary backup and status units before the production target takes over
their work directories. After installation and `daemon-reload`, enable and
start `dsc-node-pipeline.target`. Verify the first committed Node cycle and
its Drive recovery. Before a rollback, stop the target, reconcile accepted
Node coverage to the Data ledger and restore the single Data writer.

### Legacy Data services

Deployment: disable the old extraction/offline watchdog and delivery timers before
enabling `dsc-pipeline.service`; keep `dsc-pipeline-monitor.timer` active. The monitor
uses `current.json` to follow the active cycle and shows all eight stages. It is
view-only. `systemctl stop dsc-pipeline` intentionally pauses the coordinator; it
does not have a competing watchdog. Start the unit to resume. Old historical worker
services are retained but disabled for rollback; completed data remains in place.

Initial source rollback copies (no runtime datasets inside):
`/home/dsc-data/runs/pipeline-deploy-20261030/scripts-before.tar.gz`,
`/home/node/dsc-extraction/pipeline-deploy-20261030/scripts-before.tar.gz`, and
`/home/dsc-revert/runs/pipeline-deploy-20261030/scripts-before.tar.gz`.
Each host also retains a hash-verified move map under its `obsolete-scripts/`.
Pause the new service before restoring source and the recorded previous unit
enablement. Never restore source archives over runtime data or credentials.
