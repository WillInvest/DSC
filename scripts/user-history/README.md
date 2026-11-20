# Aave V3 all-user history

Scope: Ethereum V3 core Pool, every historical reserve and all users, using only
our RETH node. The user authorized beginning the full workflow and hourly
supervision on 2026-11-13. This work has separate output identities and does not
modify the existing production Pool/state pipeline or its accepted coverage.

Research provenance and append-only issue/decision records are maintained in
`docs/data-collection/aave-v3/README.md` and `events.jsonl`. Preserve source
responses, receipts, failed-attempt evidence, version/source hashes and dated
audit reports. Record source gaps separately from implementation errors, pending
work and deliberate scope deferrals; do not infer missing historical boundaries.

## Current all-reserve scope and direct oracle collection (2026-11-14)

The user removed the four-asset restriction after the direct-node oracle bulk
benchmark. `priority.json` now selects all 172 historical token contracts across
68 reserves, reusing existing receipts. Raw input, decode and scaled-position
services have resumed under the same immutable cutoff 26,407,881. Whole-account
nominal balances and HF remain separate, incomplete stages.

`dsc-aave-v3-oracle.service` on dsc-data runs `run_oracle.py`. It uses Cryo on
dsc-node with 32 concurrent requests to call the original historical
`AaveOracle.getAssetsPrices` at every block end. The node has 32 logical CPUs
(16 physical cores); concurrency is not a claim that all CPUs are saturated.
`prepare_oracle.py` reuses retained provider/configurator events, completes the
routing-event tail with Cryo, and checks actual node reserve lists and Oracle
routing at historical changes. The immutable plan has 62 intervals spanning
16,291,127–26,407,881 inclusive. The first 205,665 blocks have no reserves and
are recorded as empty-market coverage, not price observations.

Oracle plans/status/receipts live under `/home/dsc-data/runs/aave-oracle-v3/`;
data artifacts under `/home/dsc-data/data/aave-oracle-v3/`; node extraction under
`/home/node/dsc-extraction/aave-oracle-v3/`. Raw calls and decoded price vectors
remain staging artifacts with explicit interval receipts, outside canonical
primary catalogs. The interval plan supplies asset ordering and quote units.
Node verifies block coverage, ABI outputs, readback and sampled direct getters;
data verifies hashes, rows and decoded block/vector values before acceptance.
Returned zero prices are retained and counted as quality defects, never filled
with a fabricated quote. Original accepted event/state grids are untouched.

The existing private monitor has a separate Oracle card showing node interval
progress and data acceptance. This historical backfill is independent of the
continuous primary extraction ledger; displaying it does not advance that
ledger or claim incremental Oracle coverage beyond the fixed cutoff. Hourly
read-only supervision now follows all assets and the Oracle run.

The recent 10,000-block/68-asset benchmark extracted in 6.01 seconds and finished
setup/decode/checks in 10.16 seconds. A separate 10,000-block/12-asset SVB pilot
and an interrupted-Parquet recovery check passed. These are bounded tests, not
a guarantee that full-history extraction finishes in three hours.

## Earlier four-asset priority (superseded)

The user subsequently prioritized ETH (WETH), USDC, USDT and Bitcoin (WBTC).
The active run's `priority.json` selects those assets and defers other assets,
including Lido-related work. It is scheduling policy only: the immutable original
extraction plan, target hash and all accepted/checkpoint data remain intact.
The queue reloads this policy before submitting each job. Progress reports both
the four-asset denominator (12 token contracts) and the original 172-contract
inventory. `priority_inputs_collected` means only focused raw token inputs are
ready; it does not mean positions, prices or HF have been reconstructed.

All three WETH, USDC and WBTC token inputs were already accepted when priority
changed. USDT's three tokens are the remaining first-priority input work. Other
in-flight supplementary workers were stopped with their artifacts preserved.
No new Lido/rETH source collection is required for four-asset nominal positions.

Four-asset user positions can be reconstructed before historical prices are
complete. Valuation requires historical Aave price sources; WBTC's SVB adapter
uses WBTC/BTC and BTC/USD feeds. True whole-account HF additionally requires
every collateral/debt asset held by that account, collateral flags, eMode and
historical risk parameters. Accounts with other assets must be marked outside
the four-asset-only HF scope, never silently given a whole-account HF.

## Current deployed stage

`prepare.py` on dsc-data reuses retained full-history configurator logs, fetches
the missing ReserveInitialized tail from node in bounded ranges, verifies that
all current reserves are present, and discovers every historical aToken,
variable debt token and nonzero stable debt token. It reuses the registry's
node-side deployment-boundary verifier. Each token's 10,000-block grid starts
at its verified deployment, and each run pins a finalized end block/hash.

`run.py` on dsc-data coordinates two `node_worker.py` processes. Workers use
Cryo with two requests each and separate roots; they reuse the active queue's
chunk and empty-result checks without acquiring its global lock or touching its
tmux pane. Node validates log scope/duplicates and hashes each file. Data pulls
the files directly and verifies hashes and row counts before a raw-input receipt.
Failed jobs resume the same immutable scope through systemd on-failure restart.

These newly delivered files are staging inputs with explicit receipts, not
published canonical datasets. Canonical publication must use the existing
compaction/catalog machinery after the complete input stage is verified. Do not
read existing canonical primary files by globs; use the shared-lock snapshot API.
The retained one-time auxiliary log directories are separate from that catalog.

State locations:

- `/home/dsc-data/runs/aave-user-history-v3/current.json`
- `/home/dsc-data/runs/aave-user-history-v3/<cutoff>/plan.json`
- same run directory: `progress.json`, `jobs.json`, `receipts/`, per-address logs
- staging data: `/home/dsc-data/data/aave-user-history-v3/<cutoff>/staging/`
- node extraction: `/home/node/dsc-extraction/aave-user-history-v3/<cutoff>/`
- service: `dsc-aave-v3-user-inputs.service` on dsc-data

## Remaining workflow

### User event decoding and scaled-position recovery

`dsc-aave-v3-user-decode.service` processes the 12 prioritized token histories,
using explicit raw receipts, ABI-aware existing PoolDecoder/Web3.py parsing,
and separate Parquet schemas for historical implementations. Upgrade logs and
implementation bytecode are verified on node; Sourcify supplies source ABIs.
Where Sourcify returns 404, verified Blockscout ABI records are permitted only
when their deployed bytecode exactly matches the historical node runtime.
Partial source verification remains explicitly recorded.

`position-owners.sqlite` stores deposit, borrow and transfer participant addresses;
`decoded-staging` and its per-file receipts retain all decoded events. Discovery
counts are partial until every token history is processed, and do not establish
that an address currently has a nonzero balance.

`dsc-aave-v3-user-positions.service` then reconstructs scaled token balances from
Mint/Burn/BalanceTransfer, without counting the accompanying Transfer twice.
It executes the original Aave WadRayMath and TokenMath in EthereumJS via a thin
event-inversion facade. Versions with nominal mint inputs use the historical
half-up rule; versions with scaled mint inputs use their original floor/ceiling
balance rules. Sources, keccak checks, compilation and the deployed math kernel
are reproducible under `artifacts/aave-four-asset-positions-2026-11-13/`.
Deploy that artifact's `math-kernel.json` beside the worker and install the pinned
runtime with `npm ci --ignore-scripts --no-audit --no-fund`.

`scaled-positions.sqlite` and sparse `positions-staging` change tables are
resumable replay outputs. `validate_positions.py` independently compares their
scaledBalanceOf to node every 100 input chunks, at upgrade-crossing chunks, and
at token completion, retaining separate reports. The initial WETH 10,000-block
prefix through 16,506,791 matched all 104 recovered owners exactly. Stable debt
with any balance-changing event stops replay until a separately verified adapter
is supplied; histories without activity additionally require node totalSupply=0.

Zero-address `BalanceTransfer` is not automatically a burn: the historical WETH
implementation at block 24,944,649 credited its scaled balance storage at zero.
Replay retains that ledger balance and hydrates it across input chunks; the
separate owner-discovery table continues to exclude zero from research users.
`test_position_replay.py` checks this real event, share conservation, ordinary
transfers, negative-balance rejection, and cross-chunk zero-balance hydration.
New position receipts record the Python/JavaScript worker hashes as well as the
unchanged original Aave math-kernel hash. Older receipts remain intact.

Read `decode-progress.json`, `decode-jobs.json`, `position-progress.json`,
`position-jobs.json` and `position-validations/` for these stages. A completed
scaled replay and sampled node matches are not nominal every-block balances,
historical valuations or complete HF acceptance. Interest indices, collateral
flags, risk settings and prices remain separate required inputs.

Token input backfill is the first stage, not a completed user dataset. Required
subsequent stages are historical oracle/rate/configuration source discovery and
collection; ABI/version-aware position replay; original Aave oracle/HF execution;
and stratified independent node checks. Stable debt, GHO, upgrades and all eMode
versions require explicit historical support. Never label unsupported types,
unprocessed periods or raw-input receipts as verified user/HF coverage.

Accepted Pool events are reused. `pool-reuse.json` records a shared-lock canonical
inventory and the original baseline plan; build a complete view using catalog
historical_before boundaries plus baseline receipt inventories, without duplicate
partial tails. The main pipeline continues to advance independently.

## Oracle evidence

`artifacts/aave-oracle-event-replay-svb-2026-11-13/` contains a seeded SVB pilot
from blocks 16,800,000–16,807,200 inclusive. Five Chainlink feeds were reconstructed
from 113 AnswerUpdated events. Lido pooled ETH and shares were reconstructed from
Submitted, mint TransferShares, SharesBurnt and old oracle PostTotalShares events;
Rocket Pool used BalancesUpdated at the mined block, not its older reported block.

Original, source/runtime-matched historical Aave adapters, Lido getter and rETH
getter bytecode execute locally in EthereumJS with input fixtures. Across every
one of the 7,201 blocks, all 28,804 prices, 14,402 exchange rates and 28,804
underlying-total comparisons matched node exactly. Direct node references were
comparison-only. Local replay took approximately 1.34 seconds; collecting the
all-block reference data took approximately 259 seconds.

In this particular window each exchange rate changed only once. A control using
only Lido's report also matched the ratio at 1e18 precision in this window;
there is no evidence here that daily-only filling fails. Full share/pooled-ETH
events were nevertheless independently reconstructed and verified, and other
versions/windows must be tested before applying a daily-only shortcut.

The experiment is not a deployment-to-present rate/oracle profile and crosses
no Lido, Rocket Pool or Aave adapter upgrade. It does not establish event-only
reconstructability for all modern price adapters.

## Hourly supervision

`dsc-aave-v3-user-audit.timer` on dsc-data runs hourly even when the desktop is
asleep. `audit.py` reads status and receipts, verifies selected staging hashes,
and compares complete sampled block logs against node. It writes only separate
reports to `audits/` and `audit-latest.json`; it never modifies input data,
configuration, checkpoints, acceptance receipts or catalog state. The first
active-stage audit correctly reported that no full token scope was accepted yet.
The same audit path passed the delivered 200-block aUSDC pilot against node.
The audit follows four-asset priority and rotates token types per asset. It now
samples empty as well as nonempty files; empty files are checked against node
instead of being silently skipped, addressing the previously reported gap.

Codex automation `aave-v3` is ACTIVE and runs hourly in this chat to read and
report progress/audit evidence. It cannot restart services, change configuration,
advance acceptance or repair data. Automatic approval rejected those broader
privileges; supervision was narrowed to read-only checks and reports. No
email/Slack messages are sent. Chat follow-up requires the desktop runtime;
server extraction and server-side hourly audits are independent of it.

## Reused components

Cryo 0.3.2, Web3.py ABI-aware decoding/get_logs, PyArrow Dataset filters, existing
deployment_block, EventDecoder, shared atomic JSON/checksum helpers and the
catalog snapshot reader. Custom code is limited to reserve/token discovery,
separate queue orchestration, delivery acceptance and validation. The old event
runner cannot be called unchanged because it owns the production extraction lock
and tmux session; its inspection/helpers are reused instead.

## Nominal balances and whole-account HF (2026-11-14)

`dsc-aave-v3-user-accounting.service` on dsc-data now prepares and indexes
queryable, end-of-block account histories. It leaves the existing input/decode/
scaled services running and uses only our RETH node. Its state is separate from
primary canonical acceptance:

- `nominal-progress.json`, `hf-input-progress.json`, `hf-progress.json` under the existing fixed-cutoff user
  run report preparation, reconstructable block coverage, actual numerical rows,
  node sample counts, errors and waiting reasons separately.
- `accounting.sqlite` indexes exact block timestamps/hashes, reused Pool and
  configurator events, boundary/risk/eMode snapshots and explicit share-history
  receipts. `accounting-header-receipt.json`, `nominal-receipts/`, `hf-receipts/`
  and timestamped `accounting-validations/` retain provenance and comparisons.
- Cryo extracts all block timestamps on dsc-node with32 requests, into a separate
  `accounting-headers` staging root, then transfers directly to dsc-data. The
  node's `progress.json` counts complete Parquet rows. The data receipt checks
  continuous every-block coverage and the fixed ending hash before use.
- Pool canonical tails are read while holding the shared catalog snapshot lock;
  preserved baseline files strictly precede the canonical boundary. Baseline
  receipts lacking SHA-256 are checked against their recorded size/row count and
  a new accounting input hash is recorded; this is not an invented old hash.
- Risk/eMode values are node snapshots at asset initialization, configuration
  event blocks and reviewed Pool upgrades. This captures boundary migrations
  rather than assuming all state changes were emitted as setter events. User
  collateral flags and mode selections come from retained Pool events; debt
  ownership and shares come from token histories.

Balances and HF are stored in **factorized form**: sparse shares/rate/configuration
histories plus exact block times and every-block Oracle vectors. The original
historical Aave code evaluates a requested user/block. We do not store the enormous
user×block Cartesian product or call a small numerical sample that full product.
`factorized_history_ready` is distinct from dense numerical materialization;
`completed_blocks` means complete reconstructable input coverage across all
assets at those blocks. `numeric_rows_materialized` counts actual computed sample
rows only. A missing asset's token history blocks whole-account coverage from
that asset's first block onward.

Query a covered account on dsc-data:

```
/home/dsc-data/scripts/.venv-abi/bin/python \
  /home/dsc-data/scripts/user-history/accounting_query.py \
  --user 0xUSER --block BLOCK
```

The JSON contains exact token amounts (underlying token decimals), normalized
indices, collateral/debt values in Oracle base units and all six
`Pool.getUserAccountData` fields including18-decimal HF. No-debt HF retains the
contract's uint256 maximum; do not treat it as an ordinary finite observation.
Queries require indexed and nominal-validated token factors and reject incomplete
account inputs. They do not fill missing balances/prices with fabricated zeros.

Calculation reuse:

- `interest_worker.mjs` executes hash-verified original `MathUtils`, `WadRayMath`
  and normalization entrypoints in EthereumJS, selected by historical block.
  There are two historical compounded-interest implementations; one formula
  must not be used indiscriminately across all versions.
- `account_worker.mjs` executes original historical `GenericLogic` and
  `ReserveLogic` with thin input fixtures. Modern collateral/borrowable/ltvzero
  bitmaps and isolated eMode semantics are explicit, required inputs.
- Existing `math_worker.mjs` executes original Aave token balance rounding for
  nominal amounts; no alternate floating-point HF calculation is introduced.
- The new workers use a pinned isolated `accounting-runtime/` npm installation,
  so installing the added EthereumJS block dependency does not replace the
  running scaled-replay runtime.

A missing Pool upgrade was discovered while validating the new stages:
**block26,392,162**, implementation `0x4295fe69b3e978eb511e4558b4407ad871612fcb`.
Its source/runtime and exact activation were verified on node. Accepted canonical
Pool events confirm it is the only additional Upgraded event after25,199,939
through the fixed cutoff. Supplemental source profiles live under
`artifacts/aave-interest-2026-11-14/`; accepted historical primary profiles were
not rewritten. The computation kernels now cover12 implementations through
26,407,881. This does not extend the main pipeline's acceptance identity.

Validation before launch:13 real normalization getter fixtures across12versions;
57 retained SVB complete-account fixtures;14 fresh node-seeded nonzero-debt
account cases across12versions, including historical/current eModes; all compared
fields exact. These establish kernel behavior, **not synthesized full-history
correctness**. Production then compares synthesized nominal balances at early,
middle, cutoff and token-upgrade checkpoints; whole-account six-field samples
include SVB and Pool upgrade boundaries when complete inputs exist. Reports and
failed comparisons persist; failed results cannot be reused as successful on
restart. Code/kernel generation identities invalidate stale validation pointers.
Historical stable debt activity remains unsupported until its own adapter passes;
GHO and other special token semantics must pass historical balance checks rather
than being assumed correct from a cutoff-only comparison.

The private monitor has separate nominal and HF cards. Hourly automation now
prioritizes new outputs, independently compares them with own-node getters and
records `continue`, `repair_verified`, or `await_user_decision`. The user authorized
clear, reproducible code fixes with tests and node rechecks; ambiguous causes,
research-method changes and rewrites of accepted/canonical data require a user
decision. The fixed-cutoff Oracle collection itself completed62/62 intervals,
10,116,755 coverage blocks (205,665 empty-market blocks),400,931,872 prices,
zero returned zero-price values. Whole-account HF remains a separate stage.

### Numerical all-user HF generation

`dsc-aave-v3-user-hf.service` runs `materialize_accounts.py --workers32` on
dsc-data. It waits for the checked generation and receipt published in
`hf-input-progress.json`; it owns `hf-progress.json`. The monitor labels their
coverage separately: factor completeness does not advance numerical completion.

The full numerical plan partitions16,291,127–26,407,881 without overlaps or gaps.
It prioritizes the seeded-study SVB period16,800,000–16,807,200 with a one-block
calibration job, then processes all remaining historical blocks in ≤100-block
staging jobs. These are auxiliary user-data jobs, not replacements for original
10,000-block primary grids. Each job includes every discovered owner from its
first appearance, including zero-debt/closed accounts; only the zero address is
excluded. It preserves all six raw account integers and the contract's maximum
HF sentinel. There is no silent four-asset, borrower-only or temporal filter.

Parquet rows are streamed, output-readback rows are checked against own-node
`getUserAccountData`, and only then is a checksummed generation-bound receipt
published. Accepted numerical intervals and rows have separate counters from
in-flight rows. Outputs live under the user's data output `accounting/hf-numeric/`;
receipts under run-root `hf-numeric/`. Restart checks source generation, owner
first-seen identities, file SHA/rows and successful validation; failed comparisons
remain evidence and never become accepted through a restart. The producer keeps
50GiB free for the primary pipeline and pauses as `awaiting_user_decision` if the
measured next batch would cross that reserve. Dense all-user history can be very
large; completed Oracle timings do not estimate its cost. Rate/projection values
must come from actual numerical jobs. Factorized account queries remain useful
before the complete numerical export finishes.

### GHO historical debt adapter

Legacy GHO debt used per-user discount percentages and previous interest indices.
Generic debt Mint/Burn inversion can miss discount-driven share burns. The exact
GHO variable-debt address dispatches to `gho_replay.py`; generic replay is forbidden.
The adapter executes original GHO `_mintScaled`, `_burnScaled`, rebalance and
`balanceOf` code in EthereumJS. `DiscountPercentUpdated` supplies the observed
old/new percentages; the original action uses the old percentage and persists
its new state at the associated Mint/Burn. No external stkAAVE history is guessed.

Per-user previous index and discount percent persist transactionally across
chunks. Explicit `gho_history` receipt files feed `accounting.sqlite.gho_states`.
The adapter verifies the full retained implementation profile against its six
source/runtime-reviewed implementations and rejects unknown routes. Historical
GHO nominal queries use the original discounted `balanceOf`; full HF retains
GenericLogic's actual historical branch. Some profiles read scaled debt and the
index; others read `balanceOf` for non-virtual reserves such as GHO. The fixture
must expose both views separately so the unchanged protocol code chooses its
own basis. Original discounted
balances and38 initial historical events (8 users) matched node exactly; all
8worker/persistence tests passed in the deployed PyArrow/SQLite runtime. This is
bounded evidence, not acceptance of the full GHO historical replay.

Positive GHO scaled debt alone does not prove that the Pool borrowing bit is on.
Original historical full-repayment code can leave rounding dust while clearing
that bit. A source-executed counterexample demonstrates the distinction; bounded
historical node probes did not find an observed occurrence. Production therefore
collects actual Pool user configuration at distinct GHO action owner/block points
and reviewed Pool upgrades. These are supplementary production inputs, recorded
with block hashes, rather than independent comparisons or inferred debt flags.

The numerical producer preserves a lone unaccepted Parquet/sidecar file after an
interrupted publication before recomputing; accepted files require inspection if
their identity is incomplete. Python's ProcessPoolExecutor surfaces abrupt worker
death as BrokenProcessPool, so the service reports failure and resumes verified
receipts rather than keeping a fresh heartbeat while waiting for a lost result.

### Separate nominal and scaled fixture views (2026-11-15)

Whole-account validation at block21,917,056 exposed a real adapter defect: its
token fixture returned scaled shares from both `scaledBalanceOf` and `balanceOf`.
The original GenericLogic's non-virtual GHO branch then omitted accrued interest.
The failing account's reconstructed token balances were individually correct;
its total debt/HF were not. The failed report remains retained and unaccepted.

New computation artifacts live in `artifacts/aave-account-kernels-20261115/`;
the 20261114 artifacts are preserved. Their thin fixtures store separate nominal
and scaled views. Ordinary nominal balances use the same original ReserveLogic
and WadRayMath; GHO's explicit nominal view comes from its version-aware original
balance adapter. The original historical GenericLogic still selects its branch;
the reconstruction does not substitute a separate HF formula.

Five kernel tests pass locally and on dsc-data, including all12versions, eMode,
57SVB accounts and a regression separating the GHO nominal/virtual branches.
All39retained full-account comparisons passed with the candidate;5fresh six-field
node checks include the failed user, controls/eMode and2early discounted GHO
users. These are bounded regressions, not full numerical acceptance. Auxiliary
regression evaluators skipped only the new-generation readiness markers while
reading already validated retained histories; no marker/data was written. The
production runner revalidates every nominal factor under the new generation
before accepting full-account coverage or starting numerical exports.


## Four-machine numerical execution (2026-11-15)

All172 token factors are ready under calculation generation
`5197dae89b140ccbed5090f2f3e6ff39dde3eba5c737bb97b0ec320663532004`.
The1154 nominal samples and72 whole-account six-field checks passed. These are
validation samples and reconstructable input coverage, not dense numerical rows.
The first production numerical block16,800,000 contains2721 owners, including1097
with priced debt. Subsequent numerical coverage must come from explicit receipts.

The user authorized computation on data, revert, web and node. `distributed_hf.py`
uses Dask2025.12.0's Client executor to run the existing `materialize_accounts.py`
job function. Data retains its single producer lock and alone publishes numerical
receipts. The32 compute processes are distributed as data4, revert8, web6, node14;
a separate data worker owns node comparison calls. These process counts are not
claims of dedicated CPU cores or measured speedup. The continuous primary pipeline,
RETH, website and reverted archives remain independent.

Worker images contain SQLite online backups (target journal mode DELETE), explicit
factor/token/profile/Oracle receipt inputs, original Aave artifacts and the pinned
Python/Node runtimes. systemd BindReadOnlyPaths maps the immutable image to original
absolute paths; only separate job output and worker-state paths are writable.
Workers attest the snapshot SHA and factor generation before production resumes.
Remote images use manifest-hash directories; interrupted transfers remain staging
until a successful verification seal. Old snapshots and failed reports are retained.

`dsc-aave-v3-user-hf.service` now starts the distributed adapter. Additional services
are `dsc-aave-v3-hf-scheduler.service` and `dsc-aave-v3-hf-validator.service` on data,
and `dsc-aave-v3-hf-workers.service` on all four machines. Config/rollout evidence
is under `/home/dsc-data/hf-distributed/`; current config is `cluster.json`.
Each machine generates its own leaf private key locally; the data-local CA signs
public CSRs. Current credentials stay outside the repository. The web host uses
existing operator SSH for deployment (`deploy_hf_cluster.py --external-host web`),
not new data-to-web shell authorization. External hosts must be stopped by their
operator before replacing their worker unit/image. All services must reconnect
after scheduler restart, including normal worker shutdowns, so Dask services use
Restart=always. Administrative systemctl stop still pauses them.

fsspec's Dask filesystem streams returned files over the authenticated Dask channel;
use its default client (`client=None`) rather than serializing a live TLS Client.
Original receipt validation checks the received bytes, owner universe, factor and
node comparison report before atomic exclusive receipt publication on data. No
remote receipt advances data's production coverage by itself. `scheduler_info`
must request `n_workers=-1`; this Dask release otherwise reports only five workers.

The original single-host32-process producer ran out of memory. A256MiB V8 cap alone
also failed the full-block rollout pilot; those attempts remain unaccepted.
The adapter now rebuilds the original Evaluator after500 accounts, releasing its
local EVM caches while retaining the same immutable inputs and calculation generation.
The official Aave code and original materializer are unchanged. Per-host cgroup
memory/CPU limits and the existing50GiB output reserve protect coexisting services.
Bounded SVB rollout checks do not prove resource behavior at every later version.

The monitor's HF card displays configured/online workers per host and in-flight
rows separately from accepted rows. Whole-account numerical pilots live outside
the production receipt inventory and never advance coverage. Full-history dense
output remains about910310934885 user-block rows for258131 discovered owners;
first-block compression or Oracle throughput cannot supply a reliable full-run ETA.


The distributed coordinator uses a rolling queue through the Dask executor and
concurrent.futures.wait(FIRST_COMPLETED), rather than the original same-host batch
barrier. Fast workers receive another job while slower hosts remain busy. The
original layout, factor checks, generation calculation, row-count checks and
receipt validator are reused unchanged. A fresh generation still calibrates one
nonempty job before parallel size projections. SVB jobs are a separate first
phase, and every historical job remains in the plan. Coordinator adapter SHA is
recorded separately from the unchanged numerical generation and worker image SHA.

API references: https://docs.dask.org/en/stable/futures.html ;
https://filesystem-spec.readthedocs.io/en/latest/_modules/fsspec/implementations/dask.html ;
https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.backup ;
https://www.freedesktop.org/software/systemd/man/latest/systemd.exec.html .

## Native bulk account calculation (2026-11-15)

The numerical HF service now uses the local integer batch engine. See [NATIVE-HF.md](NATIVE-HF.md) for execution, validation, version identity, and output scope. Existing factor inputs and historical reference outputs are preserved.

## Independent execution while data is offline (2026-11-15)

The user authorized continuing Aave HF on node/revert/web without dsc-data.
The current temporary services, identities, recovered-output accounting, monitor
and reconciliation guard are documented in [FAILOVER.md](FAILOVER.md).
The unavailable general data coordinator/database has not been migrated.


## HF vector format decision (2026-11-16; not yet production)

User explicitly chose to materialize only exact HF vectors, with full factorized history retained for all other quantities and later weights. This supersedes the earlier requirement that all six account metrics must be repeated in every output row; it does not authorize discarding the historical position/price/rate/config/asset attribution inputs or accepted old results. Logical representation is entity ID + first block offset + consecutive HF values. Physical files use checked segments of at most4096 blocks, with up to1M HF values per encode part as the tested working cap. Namespace and common block origin/cutoff are metadata; address mappings are stored once with append-only stable IDs.

Chosen sample schema: entity_id uint32, start_offset uint32, hf_values list<binary>. A full uint256 HF is stored as its minimal1..32-byte unsigned big-endian integer, zero=00; raw scale remains1e18 and contract uint256MAX is preserved. Ordinary Parquet/Zstd9 stores the lists. Original14-field files and producers are unchanged. The three sample vector files are2.22MB/1M HF,1.82MB/1M HF and1.82MB/870576 HF, plus a shared8.53MB full258131-user dictionary once. All30sample roundtrips across2,870,576 distinct HF values recovered exact user/block/HF keys and integers. These sizes are for the newly authorized HF-only scope, not lossless packing of all14 columns. No all-history size guarantee follows.

Full format and constraints: docs/data-collection/aave-v3/evidence/hf-vectors-20261116/format-v1.json; evidence and prototype: benchmark_hf_vectors.py and that directory's summary.json. Before adoption, require a new acceptance identity, complete production-job bounds/owner-first-seen/flattened-value coverage, strict codec/hash/continuity validation, existing own-node sample verification and host-wide memory protection. Physical segment rows are not logical user-block HF counts. Reserve/protocol datasets share the container but need explicit weighting/metric definitions before any aggregate is produced. No special-case missing interval may be filled with zero or no-debtMAX.

## Float64 vectors and weighted indicators (2026-11-16)

The user authorized the reviewed float64 plan and selected an exclusive USD0.01
total-account-debt cutoff. The new generation runs locally in NumPy, retaining
all raw/exact inputs and jointly accepting user vectors and debt-weighted
reserve/protocol indicators. The old exact generation stays paused. Definitions,
validated performance, paths, recovery, archive and four-hour audits are in
[FLOAT64-ROLLOUT.md](FLOAT64-ROLLOUT.md). Full-history computation has started;
this does not mean the910310934885-value target is complete.

## Market HF cohort replay (2026-11-20)

The new [modular replay package](hf_market_replay/README.md) retains three
every-block reserve/protocol cohorts: all borrowers, category-zero accounts,
and nonzero eMode accounts. Weighted numerators and debt denominators remain
available for subsequent analysis. [COHORT-ROLLOUT.md](COHORT-ROLLOUT.md) records
the separate Node authority, four-host TLS execution, offline recovery test,
archive stage and scoped retirement rules. This run does not replace the
original complete wallet HF release or its reconstruction inputs.
