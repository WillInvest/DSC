# DSC extraction pipeline

Extract Ethereum events with Cryo on dsc-node, transfer completed datasets to
dsc-data, decode with official Uniswap/Aave ABIs, then reconstruct selected core
state locally from decoded events. The private monitor runs on dsc-web.

The active fixed run covers 102 tracked addresses from deployment through block
26,266,551. It processes one contract's extraction at a time. Fully received
encoded data gets decoding priority; direct reconstruction runs when that backlog
is clear. Watchdogs resume interrupted work. See [scripts](scripts/README.md)
and [automation](scripts/automation/README.md).

Reconstructed state contains one end-of-block row for every block. It is explicitly
**event-derived and unchecked**: there are no recurring RETH state comparisons or
per-block header downloads. Aave uses one-time reviewed-boundary initial states
and configurator/provider event inputs. This is selected core state, not all EVM
storage or user positions. See [reconstruction](scripts/reconstruction/README.md).

The existing reverted-transaction scripts remain unchanged for later work.
Execution/tracing belongs on dsc-node, complete reverted archives and contract
filtering on dsc-revert, application data and orchestration on dsc-data, and
backend/frontend hosting on dsc-web. See [reverted extraction](reverted-extraction.md).

Monitor: https://dsc-web.tail5aad6c.ts.net/

```bash
ssh -t node@100.108.171.14 'tmux attach -r -t cryo-events'
```

Source is deployed under /home/dsc-data/scripts; data under /home/dsc-data/data.
The Python environment is scripts/.venv-abi. Run tests with:

```bash
PYTHONPATH=scripts scripts/.venv-abi/bin/python -m unittest discover -s tests
PYTHONPATH=scripts scripts/.venv-abi/bin/python -m unittest discover -s tests/sequential_events
```

Obsolete coordinators, experimental queues, old extraction launchers and old
monitor UI have been removed from the active source tree. Git history and a
private pre-cleanup server backup retain the older implementation. Data and
accepted receipts are not removed by source cleanup.
