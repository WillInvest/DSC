# Failed-transaction extraction

User decisions, 2026-10-23: use native RETH RPC, archive failed receipts and complete
call-level traces, and omit full transaction objects. Build the unified tracker later.
Prestate extraction is not enabled: it was discussed but not added to this scope.

## Tools and behavior

`extract_reverted.py` runs on dsc-node. It uses Web3.py 8.0.0 HTTPProvider for RPC transport and RETH's
native `eth_getBlockReceipts`, `debug_traceTransaction` and
`debug_traceBlockByNumber` with `callTracer`. Custom code handles project-specific
strategy selection, validation, file checkpoints and concurrency. Existing tools
were compared first: Cryo and Ethereum ETL transform receipts/traces; Cast is an
RPC CLI, not an archive manager; Dshackle Archive requires an additional service.
No custom EVM, tracer or ABI decoder is implemented.

For N receipts, let S be the sum of `(transactionIndex + 1)` for status `0x0`:

- No failed transactions: no debug tracing.
- S > multiplier × N: trace the whole block; retain only failed-transaction traces.
- Otherwise: trace failed transactions individually.

The default multiplier is 1. This is an uncalibrated execution-count heuristic,
not a speed guarantee. The CLI permits changing it; the archive records the choice
and elapsed time. Defaults: two concurrent blocks, at most two simultaneous RPCs,
120-second tracer timeout, 10-second connection timeout, 150-second read timeout,
and at most three attempts on transport failures. Web3.py handles backoff and
connection reuse; the explicit retry allowlist includes block receipts and both
debug methods. JSON-RPC errors and null results fail immediately, preserving
checkpoints for a later rerun.
Timeouts are errors, never empty successful results. A request retry may repeat
server computation if its response was lost; exactly-once execution cannot be
promised over RPC. Successful checkpoints avoid repeat tracing after interruption.

`eth_getBlockByNumber(..., false)` supplies hashes and header metadata only, not
full transaction objects. It validates receipt completeness and canonical block
hashes before/after tracing and on resume. Ranges must be below the finalized head;
chain ID defaults to Ethereum mainnet 1. Receipts must have a status (post-Byzantium).
The archive records the RETH version. Trace options include all child calls, with
call logs disabled; this is not an opcode trace or prestate snapshot.

## Files and durability

One checksummed, atomically written gzip JSON envelope per block:
`blocks/<block_number // 1000 padded to 9 digits>/<block_number>.json.gz`.
The envelope contains `payload` and its SHA-256. Payload contains header metadata,
all block receipts (including successful receipts for coverage evidence), full
unmodified callTracer result objects for failed transactions, and extraction metadata.
No successful trace is retained in the completed archive. `.work/` holds temporary
trace checkpoints; successful block commit removes them. Node archives remain after
transfer; there is no automatic deletion. RPC errors or incomplete results fail the
run. Completed blocks/checkpoints survive a retry; corrupt files or changed block
hashes fail closed and require investigation. A single-writer lock protects each
output directory. Use one output directory per chain/tracer configuration.

These files record local extraction evidence, not application delivery or the
future unified status matrix. Changing only the multiplier reuses completed valid
blocks; use a separate directory for controlled benchmark runs.

## Deployed scripts and commands

Runner copies: `/home/node/dsc-extraction/scripts/` (also updated in the old pilot path).
Canonical server script copies: `/home/dsc-data/scripts/`.
Archive verification/selection copies: `/home/dsc-revert/scripts/`.
Extraction requires Python 3.10+ and `scripts/requirements/rpc.txt` (Web3.py 8.0.0).
The node environment is `/home/node/dsc-extraction/.venv-web3/`.
Offline selection/verification on dsc-revert still uses standard Python only.
Transfer uses rsync/SSH.

On **dsc-node** (range end is exclusive):

```bash
/home/node/dsc-extraction/.venv-web3/bin/python /home/node/dsc-extraction/scripts/reverted/extract_reverted.py \
  13000000:13000010 \
  --output /home/node/dsc-reverted/output/pilot-v1 \
  --multiplier 1 --workers 2

```

`ETH_RPC_URL` defaults to `http://127.0.0.1:8545`; credentials must stay out of files
and documentation. Direct node-to-revert SSH initiation timed out during testing;
reverse SSH works using the existing server credentials. The receiver therefore
pulls data directly from dsc-node; no local-machine relay or network policy change
is needed. Run only after extraction succeeds. The pull script transfers only
completed `blocks/`, uses checksum comparison, and validates the requested coverage.
It does not delete sources. Archive names contain letters, numbers, underscores or
hyphens; the same name identifies `/home/node/dsc-reverted/output/<name>` and the
receiver's archive directory. Never reuse a name for a different chain/configuration.

On **dsc-revert**, pull and validate:

```bash
bash /home/dsc-revert/scripts/reverted/pull_reverted.sh pilot-v1 13000000:13000010
```

On **dsc-revert**, validate the requested coverage after transfer:

```bash
python3 /home/dsc-revert/scripts/reverted/select_reverted.py \
  --archive /home/dsc-revert/data/archive/reverted/pilot-v1 \
  --blocks 13000000:13000010
```

To select for the application, supply a file containing one tracked address per line:

```bash
python3 /home/dsc-revert/scripts/reverted/select_reverted.py \
  --archive /home/dsc-revert/data/archive/reverted/pilot-v1 \
  --blocks 13000000:13000010 \
  --contracts /home/dsc-revert/data/contracts.txt \
  --output /home/dsc-revert/data/selected/pilot-v1
```

Selection matches every call destination, including successful child calls inside
a failed transaction. A transaction retains its complete call tree and a list of
all matched contracts. Explicit empty files cover blocks without matches. Use a
separate output per tracked-address list. Selection does not delete the full archive.
Selected data delivery to dsc-data, scheduling, database ingestion and the unified
tracker are not automated by these scripts.

## Verification

Ten focused tests cover skip/individual/block paths, incomplete receipts,
checkpoint recovery, changed hashes, corrupt outputs, RPC trace errors, wrong trace identities, and successful child matching.
Live pilot: blocks 13,000,000 through 13,000,009; 64 failed transactions;
one skipped block, two individual-mode blocks, seven whole-block-mode blocks.
Full callTracer JSON matched between individual and whole-block strategies for four
failed transactions in two sample blocks. Resume reused all ten completed blocks.
The threshold remains unbenchmarked; this pilot checks correctness and connectivity.

References: [RETH debug API](https://reth.rs/jsonrpc/debug/),
[callTracer](https://geth.ethereum.org/docs/developers/evm-tracing/built-in-tracers),
[Ethereum ETL](https://github.com/blockchain-etl/ethereum-etl),
[Dshackle Archive](https://github.com/emeraldpay/dshackle-archive),
[Cast RPC](https://getfoundry.sh/cast/reference/rpc).

Web3.py migration verified: all 34 tests passed; five live blocks reproduced the
original receipts and full trace results, and the existing ten-block archive resumed.
