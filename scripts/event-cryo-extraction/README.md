# Sequential encoded-event extraction

This is the user-requested replacement for the historical event coordinator and
private monitor. It extracts **events only** for the registry's 50 Uniswap V2,
50 Uniswap V3, and 2 Aave market addresses. State, decoding, and reverted traces
are not part of this queue. The previous coordinator remains disabled.

`run.py` runs on dsc-data. It calls `node.py` over LAN SSH on dsc-node for one
address at a time. Cryo writes 10,000-block Parquet chunks, with 32 concurrent
chunks/requests and normally 1,000 blocks per RPC request. It then transfers that
address's files via rsync to its existing `encoded` folder on dsc-data. Extraction
of the next address starts only after transfer and destination checks.

The fixed target is deployment through finalized Ethereum block **26,266,551**,
hash `0x31a1209f0a733f1da049a77eb6c1d909ec982591c95e922ff8f3a26ad882c92d`.
This reuses the WETH/USDT and Aave V3 pilots' exact ranges and completed output.
Every destination is resolved from existing metadata and checked against the
102-address CSV; the source registry's older selection cutoff is not the extraction cutoff.

## Retry and completion

Cryo skips existing files. Before resuming, PyArrow checks file footers and moves
interrupted/invalid files into an operational quarantine directory. Failed chunks
retry with inner request sizes **100, 10, then 1**; output chunk boundaries stay
unchanged. This handles RETH's log-response cap without changing the node during
extraction. The user subsequently raised that cap from 20,000 to 100,000 on
2026-10-24; smaller-range retries are still needed for responses above the new cap.
If even single-block requests fail, the address is visibly failed and the queue
continues. No failed range is silently marked complete.

Completion requires exact expected chunk coverage, including Cryo's final-report
evidence for empty chunks, an extraction identity match, successful rsync, and
matching destination file names, sizes and Parquet row counts. There is no per-block
header scan. This is an RPC extraction/transfer check, not independent proof of
the node's historical correctness. Raw output is retained on the node for resume.

Operational files live at `/home/dsc-data/runs/sequential-events/`: immutable
`plan.json`, atomic `progress.json`, `jobs.json`, logs and accepted `receipts/`.
Restarting `dsc-sequential-events.service` resumes its plan and retries failed
addresses; completed destinations are checked before skipping them. The service
does not automatically restart a failed process in a tight loop.

## Monitor

The existing `dsc-monitor.service` on dsc-web serves `monitor.py` using its existing
FastAPI/Uvicorn environment and private Tailscale URL. Every ten seconds it reads
one JSON snapshot through the existing restricted fixed-command SSH key. The
export command can only read this snapshot. It cannot launch extraction.

The main number and bar count **addresses received on dsc-data**. The current
address bar counts files written on dsc-node, so it is provisional and may omit
empty chunks until final verification. Received contract-block totals are separate.
The page marks stale snapshots and SSH failures explicitly. No ETA is promised.

Live private page: https://dsc-web.tail5aad6c.ts.net/

Cryo's native current-contract display runs in a persistent tmux session on
dsc-node (not dsc-data). It updates automatically when the queue changes contracts:

```
ssh -t node@100.108.171.14 'tmux attach -r -t cryo-events'
```

Detach with Ctrl+B, then D. The native ETA covers only the current Cryo pass;
the web page counts the entire queue. Transfer progress is recorded in per-address
logs on dsc-data. The native terminal can display the last finished pass during a
transfer; use the web page for the current queue phase.

## Commands on dsc-data

```
/home/dsc-data/scripts/.venv-abi/bin/python /home/dsc-data/scripts/event-cryo-extraction/run.py prepare
sudo systemctl start dsc-sequential-events.service
sudo systemctl stop dsc-sequential-events.service
journalctl -u dsc-sequential-events.service -f
```

Prepare only once, then copy `plan.json` to
`/home/node/dsc-extraction/sequential-events/plan.json` before starting. Node execution
uses its existing ABI Python environment (Web3.py and PyArrow); no new dependency
is installed. The standalone tmux pilot is superseded by this service.

Tests: `python -m unittest discover -s tests/sequential_events -v` with PyArrow and
Web3.py available. The implementation reuses Cryo, rsync, PyArrow metadata APIs,
Web3.py, Python subprocess/file locking, and FastAPI; custom code is limited to
registry adaptation, sequential scheduling, receipts and progress presentation.
