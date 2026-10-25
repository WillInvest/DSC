# Offline event decoding

The original `PoolDecoder`, Web3.py `process_log`, strict ABI checks, Arrow type
conversion, `BatchWriter` and checksummed `publish` remain unchanged.
`decode_event_files.py` is a file-parallel adapter for full-history Cryo folders;
it uses Python's ProcessPoolExecutor, not a new event parser or scheduler service.

Example on dsc-data:

```sh
/home/dsc-data/scripts/.venv-abi/bin/python /home/dsc-data/scripts/decoding/decode_event_files.py \
  --protocol uniswap-v2 --workers 16 \
  --input /home/dsc-data/data/dex/uniswap/v2/encoded/WETH-USDT-3000 \
  --output /home/dsc-data/data/dex/uniswap/v2/decoded/WETH-USDT-3000
```

For Aave use `--protocol aave-v3` and `--profiles` pointing to the reviewed
historical deployment profiles. Profiles must cover all event rows without gaps
or overlap; they are pinned to historical ABIs. Upgrade-block ABIs in the existing
profiles include reviewed boundary events. Do not simply use the newest ABI for
the whole history.

Each input file produces `decoded/<pool>/<input-stem>/schema-vN/<Event>.parquet`,
with existing decoder manifests and a chunk receipt. Aave schema versions remain
separate. The pool's `metadata.json` is retained. `decode-progress.json` reports
completed files and rows; `state: complete` means the selected inputs all decoded.
`--limit` is only for a bounded pilot and its progress applies only to that subset.

Reruns reuse completed shards only when source hash, ABI/profile identities and
decoded file checksums match. Atomic publication prevents incomplete shards from
appearing complete. Input file ranges must not overlap and each row must fall
within its file's range; the existing duplicate check applies within each shard.
This bounds duplicate-tracking memory without losing cross-file disjointness.

Full source/output row counts must match. Decode errors are not skipped. Encoded
inputs are preserved. The old single-profile CLI remains available for its original
event/state batch workflow.
