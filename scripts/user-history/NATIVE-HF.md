# Native historical Aave V3 account calculation

The production `dsc-aave-v3-user-hf.service` on dsc-data now runs
`native_materialize.py --config /home/dsc-data/hf-distributed/cluster.json`.
Its `native.conf` systemd override follows the earlier `distributed.conf`.
The existing four-machine Dask deployment and frozen input images are retained.

`native_hf.py` adapts integer operations from the twelve reviewed historical
Aave source bundles in `artifacts/aave-account-kernels-20261115/`. It pins the
reviewed manifest, preserves historical rounding/eMode/interest differences,
and uses Python integer arithmetic rather than a local EVM. Source licenses
remain in the retained bundles. Historical GHO discounts and borrowing flags
remain separate inputs. Stable-debt activity without a verified adapter fails.

`native_batch.py` seeds sparse user positions and settings from read-only
SQLite, applies subsequent events in block order, and normalizes each reserve
once per block. Its `account(..., include_balances=True)` also calculates actual
token balances. It reuses existing price-file verification and reserve bootstrap
lookup from `accounting_query.py`; those helpers perform no EVM execution.

`native_materialize.py` reuses the original materializer's Parquet writer,
owner universe, capacity guard, node comparisons and receipt acceptance. The
small calculation modules are distributed using Dask `Client.upload_file` into
worker scratch space, without rewriting frozen input images. Only dsc-data
publishes accepted receipts. Every discovered user is retained from first
appearance, including closed positions and no-debt accounts. HF uint256 maximum
is kept exactly. This numerical export contains the six account metrics; it is
not an expanded token-by-token nominal-balance table.

Jobs cover every block, at most 10,000 blocks per job, with splits immediately
before and at Pool upgrades and at the SVB priority window. The retained
materializer samples debt/no-debt accounts at each job's start, middle and end
against the node, including block identity. A mismatch prevents acceptance.
SVB runs first, then the remaining history. A 32-worker online fleet does not
mean every worker is busy during a priority stage with fewer jobs.

The new calculation has its own generation in `hf-progress.json` and
`hf-numeric/<generation>/calculation.json`. Old EVM generations and incomplete
outputs are retained. Full input readiness, computed rows, in-flight rows and
accepted rows are separate concepts. Estimated full-history physical output
size and completion time require sustained measurements; a single fast block
is insufficient. The 50 GiB free-space reserve remains enforced.

Verification and source provenance are recorded in
`docs/data-collection/aave-v3/evidence/native-compute-20261115/` and the append-only
research log. Tests: `test_native_hf.py`, `test_native_materialize.py`, plus the
existing materializer and distributed-adapter tests. The original Aave EVM
workers remain independent references, outside the production calculation path.
