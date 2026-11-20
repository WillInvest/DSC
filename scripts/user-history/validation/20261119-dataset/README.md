# Organized dataset recovery and publication, 2026-11-19

These recipes run on Node with the existing frozen replay environment and
operational helper PYTHONPATH; they are not collectors or numerical producers.
They contain no credentials and do not delete source files or change sharing.

- `restore_small_primary.py`: preserve the two early raw archives from exact
  retained source manifests and recover the four-job raw cloud archive. Reuses
  immutable copy/check and the existing full-SHA verifier. Completion receipts
  are under each Node primary archive `.primary-restore/complete.json`.
- `build_entrypoint.py`: build Node's readable catalog from explicit acceptance
  receipts. Pending cloud components remain pending. It does not accept data.
- `publish_metadata.py`: update only an explicit set of human/agent documents,
  dictionaries, canonical job catalog and provenance via rclone copy/check,
  then full remote SHA readbacks. Scientific archive bytes are not selected.
  Resume is safe for these mutable metadata files. Rebuild the catalog and
  export the authority job catalog again after final cloud ledger acceptance.
  The final publisher uses recursive listing, checks every selected Drive
  object, and checkpoints exact SHA readbacks. Unchanged files can reuse
  their SHA proof after the fresh Drive check.
- `match_data_archive_members.py`: reconcile Data's sealed cleanup inventory
  to Node's exact archive member names, sizes, SHA-256 values and TAR offsets.
  Its private evidence is under Node's `.data-cleanup-member-proof-20261119`.
  It can inspect the last batch's local index during upload. Treat that batch
  as cloud-pending until the authoritative audit pins the same index SHA.

The authority catalog export is `releases/export_job_catalog.py`, which uses
`hf_flow_queue.read_snapshot` and validates all92,294 disjoint contiguous jobs
and910,310,934,885 logical values. On Compute's ordinary SSH shell, add the
existing frozen image `scripts` directory to PYTHONPATH for `shared.artifacts`;
the live control service supplies that package via its read-only bind mount.

After the final batch is committed on Compute, export a fresh catalog and
snapshot, install both at the Node entrypoint, and run
`audit_job_archives.py` with the entrypoint and primary archive roots. It
checks continuous canonical job coverage against all six archive receipts,
the pinned archive indices and manifests, local package sizes and raw bytes.
It deliberately refuses a catalog with any pending cloud assignment.
Then rebuild DATA_CATALOG.csv and rerun the existing metadata publication
service to publish the final catalog and current validation receipts.

`finish_release.sh` is the bounded operator-side continuation for this fixed
release. It waits for the Compute authority receipt, exports under the shared
ledger lock, audits in a private Node staging directory, then publishes after
the initial metadata publisher exits. It does not authorize retirement.

The helper retirement gate in `retire_legacy_helpers.py` now checks the
published catalog, audit and restored replay proof. Per-file Node verification
and the restricted retirement helper remain required. The gate is staged on
Compute while publication continues.

The Compute and Revert source plans are prepared in each host's
`~/hf-legacy-retirement-20261118`. Their corresponding Node proofs are under
`/home/node/hf-centralized/20261118-cleanup/{web,revert}`. The existing
retirement controller reuses these exact plans and proofs when the final
dataset gate accepts.

The final cloud worker uses `95-final-drive-query-controls.conf` after the
2026-11-19 Drive rate-limit recovery. The file sets 128 MiB upload chunks,
recursive listing and two requests per second. The immutable batch path,
package index and authority ledger are retained across the restart.

`resume_sealed_final_archive.py` handles this fixed batch after an interrupted
cloud copy. Its read-only preflight checks the earlier independent full-index
SHA, exact member manifest and all 238 package certificates and file identities.
It uses the original batch lock, immutable copy/check and restore probes. Only
the normal uploader records the canonical commit and retires loose replicas.
Run the resume with the normal cloud worker stopped to avoid duplicate work.

Data's four grouped source certificates are staged on Node under
`.data-cleanup-groups-20261119`. Once the gate accepts the complete release,
run `prepare_data_retirement.py` with that directory and
`.data-cleanup-member-proof-20261119`. It binds the matched member hashes to the
final cloud archive indices and obtains an exact Drive range readback from
each archive used by a batch. It creates ordinary `hf_flow_io.retire_local`
payloads and a pinned inventory. It does not retire data. Before applying each
payload on Data, recheck the published acceptance SHA, source HOLD and payload
SHA. `apply_data_retirement.py` does that for the fixed Data output root.
Run it first without `--apply` to validate the pinned manifests and producer
hold. With `--apply`, it calls the existing restricted retirement helper and
checks both exact path absence and retired ledger records.

`finish_retirement.sh` is the one-shot operator continuation. It waits for
the final metadata receipt with the canonical archive audit, then runs the
Node proof preparation and the Data dry preflight. Only after both succeed
does it start the existing Compute/Revert retirement unit and the approved
Data retirement. Its wait expires without cleanup if final publication has
not been accepted.

Node entrypoint: `/home/node/aave-research-dataset`.
Pending final work is tracked in the active goal, DATASET-RETENTION.md,
collection events.jsonl and Node validation receipts. A metadata publication
receipt alone does not prove full scientific coverage or authorize cleanup.
