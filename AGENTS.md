# DSC project

## Purpose

Extract events, state, and failed-transaction call traces on dsc-node; archive and filter reverted transactions by contract address on dsc-revert; store the application database and run the extraction coordinator on dsc-data; and run both the backend and frontend on dsc-web.

## Required reuse check before implementation

Apply this to every new or substantially changed script/component, regardless of
length. Existing custom code is not evidence that more custom code is necessary.

1. **Search before writing.** Inspect the project and search current primary docs
   for maintained, widely used tools and higher-level library APIs that perform
   the requested operation. Check individual responsibilities, not just whether
   one tool implements our entire pipeline. Reuse verified research from this
   session when it covers the same operation and version.
2. **State the decision before editing.** Briefly identify the chosen tool/API,
   link its documentation, and name the specific gaps requiring custom code.
   “No tool does our whole workflow” is not a sufficient justification.
3. **Use the highest-level suitable API.** Do not manually implement behavior
   already provided by a suitable maintained library. Calling a low-level library
   while recreating its available higher-level helpers does not satisfy this rule.
   Example: use Cryo for bulk extraction and an ABI-aware event parser such as
   Web3.py's `process_log` for event decoding, rather than manually splitting and
   mapping event fields. Verify compatibility before adoption.
4. **Keep custom code limited to demonstrated gaps.** Project-specific file
   adaptation, validation and orchestration may remain. Explain any rejection of
   an existing API using concrete compatibility, correctness or measured performance
   evidence; do not assume a custom implementation is faster or simpler.
5. **Review reuse before declaring completion.** Check for duplicated library
   functionality and remove avoidable duplication. Splitting a long script into
   files is not a substitute. Report the reused tools and remaining custom scope.

This check does not require an additional user approval step. Proceed within the
requested scope after establishing the evidence. If research is unavailable, say
so explicitly rather than claiming no suitable tool exists.

## Servers

| Role | SSH destination | Purpose |
| --- | --- | --- |
| dsc-node (RETH node) | `node@100.108.171.14` | Run extraction and debug tracing. Send events/state to dsc-data and failed-transaction trace results to dsc-revert. |
| dsc-revert | `dsc-revert@100.91.215.76` | Archive all extracted failed receipts and complete call-level traces; select relevant reverted data by contract address and send it to dsc-data. |
| Data and orchestration | `dsc-data@100.79.189.8` | Application database, extraction coordinator and tracker. |
| Backend and frontend | `dsc-web@100.119.128.31` | Application backend, frontend hosting, and web testing. |

Connect with `ssh <destination>` using the destinations above. These addresses are SSH hosts; do not assume they are also application or database endpoints.

`dsc-node` is the logical role name for the existing RETH machine (`fscresearchvm89`); its SSH username remains `node`.

## Governing architecture for all subsequent work

This is the user-established architecture for all future design, implementation, deployment, and maintenance. Preserve these responsibilities and data routes unless the user explicitly changes them.

1. **dsc-node:** Run extraction and EVM replay/debug tracing. Send extracted event and state data directly to dsc-data. Send all extracted failed-transaction trace results, with receipt context and transaction hashes, to dsc-revert.
2. **dsc-revert:** Retain the complete extracted failed-transaction archive for reuse. Match calls against the tracked contract address list, select the reverted data needed by the application, and send that subset to dsc-data. Adding contracts or protocols should reuse archived traces where coverage exists.
3. **dsc-data:** Own the application database. Receive events/state from dsc-node and selected reverted data from dsc-revert. This machine also runs `scripts/run.py`, owns the SQLite completion tracker, and controls extraction/archive workers through SSH, as explicitly requested by the user.
4. **dsc-web:** Host both the application backend and frontend. The backend accesses the database on dsc-data.

Contract-address filtering of reverted transactions belongs on **dsc-revert**, not dsc-node or dsc-data.

## Required data flow

```text
dsc-node -- events/state --> dsc-data

dsc-node -- failed receipts/debug traces --> dsc-revert
                                                          |
                                                archive all traces
                                                filter by contract
                                                          |
dsc-data <------------------------ selected reverted data --+

dsc-data database --> dsc-web backend API --> dsc-web frontend
```

## Working guidance

- Uniswap V2/V3 and Aave V2/V3 reserve extraction share event/state launchers and the offline ABI decoder; protocol-specific configuration and ABIs live under `scripts/protocols/<category>/<protocol>/<version>/`. Require explicit protocol and contract address. Contract selection is pending; do not treat the earlier V3 pilot as the production list. Reverted archives are protocol-independent.
- Follow the finalized [extraction rule](extraction-rule.md): Cryo extracts encoded events and end-of-block state on dsc-node; transfer them directly to dsc-data and decode there with Web3.py for events and eth-abi for getter results and the official pinned protocol ABI.
- Preserve the server roles above when designing or deploying components.
- Trace each failed transaction once per extraction configuration and reuse the result across tracked contracts. On dsc-revert, associate a reverted transaction with every tracked contract called in its trace, regardless of whether that individual call failed; retain call errors for later classification.
- Reverted extraction uses native RETH RPC via Web3.py HTTPProvider. Fetch receipts, skip blocks without failures, and choose whole-block tracing when the sum of failed transaction indices plus one exceeds `multiplier * transaction_count` (initial multiplier 1). Otherwise trace individually. The user explicitly removed full transaction fetching; retain receipts and complete callTracer results. Fetch hash-only block metadata for validation. See `reverted-extraction.md`.
- The failed-transaction archive currently covers mined transactions with receipt status `0x0`. Caught internal failures in successful transactions are outside this scope. Complete call-level traces do not mean opcode-level traces.
- Inspect existing services, code, and configuration before selecting technologies or changing remote infrastructure.
- Discover and verify remote paths, service names, ports, database engine, RETH interfaces, and deployment procedures; they are not yet documented.
- Extraction and debug-tracing runners belong on dsc-node. Coverage is deployment to a captured finalized cutoff, state every block; scheduling remains manual until the bounded runner is approved for historical operation.
- Keep credentials and secrets out of this repository and documentation.
- Prefer incremental, resumable extraction, with explicit progress tracking and safe handling of retries and chain reorganizations where relevant.

## Initial repository state

The workspace was empty at initialization. No application code, build commands, or test commands have been established in this repository yet.

## Verified infrastructure (2026-10-23)

- SSH access to all four servers has been verified.
- dsc-revert is machine `HFSL-HANLON2-34`, with campus address `10.246.103.26`. It has joined the existing Tailscale network as `dsc-revert`; use its Tailscale SSH destination above.
- SSH public-key login and passwordless sudo work for dsc-revert. Tailscale connectivity from it to RETH and dsc-data has been verified; application-level access between servers still needs verification.
- dsc-revert had approximately 782 GiB available disk space and 31 GiB total RAM at inspection. Recheck capacity before deployment.
- The legacy PostgreSQL 14 cluster on dsc-data was removed on 2026-10-23. PostgreSQL packages remain installed; a replacement application database has not been initialized. `dsc-backend.service` now runs on dsc-web.
- Reverted extraction/selection scripts and the unified tracker/coordinator are deployed; no scheduled extraction services are enabled. Pilot and usage are recorded in `reverted-extraction.md`.

- Upgradeable protocols require a reviewed deployment/ABI profile with explicit historical bounds. Keep protocol-specific schemas and configuration identities; do not equate state completion across different getter sets.

- Handle example-contract selection, live validation, and historical ABI matching autonomously within the requested scope. Validation samples do not select the production contract list.

- Production coverage: every tracked address from its verified deployment block through a captured latest-finalized high-water mark; collect configured state at every block (step 1). Do not downsample or silently omit initialization/upgrade boundaries. User-position snapshots remain excluded.

- Current DEX selection rule: user accepted The Graph lifetime `txCount` as a practical pool-activity proxy, replacing the strict Dune swap-only ranking. Select 50 Ethereum Uniswap V2 pairs and 50 V3 pools separately through a fixed finalized block, descending txCount with ascending address for ties, and no TVL filter. This metric includes swaps and liquidity operations; do not label it swap count or distinct transactions. Preserve Graph queries/responses and verify canonical factory membership and deployment blocks on dsc-node. Aave market selection remains separate.

## Database-only transition (2026-10-23)

- The user changed the architecture: dsc-data is database-only; dsc-web hosts both backend and frontend. Keep dsc-data as the sole project login on dsc-data; required OS/service accounts are separate. Old scripts, legacy PostgreSQL data, and unrelated users are obsolete by user decision. Preserve the new extraction project under `/home/dsc-data/scripts` and `/home/dsc-data/data`.
- The useful backend for `dscbeta.duckdns.org` now runs on **dsc-web** as `fao` via `dsc-backend.service`, from `/home/fao/dsc-scripts/app`, listening on localhost:18080. It is enabled at boot.
- On dsc-web, `dsc-test-domain.service` serves HTTPS and forwards to `dsc-test-frontend.service`; its API traffic goes to the local backend. The former `dsc-backend-tunnel.service` is disabled. The frontend service requires the backend service.
- The preserved beta backend uses local `.runtime` SQLite databases for accounts/research/requests, DuckDB user data, exports, and a private signing key. Its chat auth snapshot is `.runtime/backend-auth.json`; never print or commit these secrets.
- The beta catalog reads `/home/fao/dsc-test-baseline-20260909/.runtime/data-release.json` and datasets under `/home/fao/dsc/data/catalog-releases` on dsc-web. This preserves the existing beta application; connecting it to the future dsc-data database remains separate work.
- The old backend and data-sync services on dsc-data have been retired. No backend dependency on its old PostgreSQL cluster was found; the cluster has been removed. Migration checks include dataset hashes, SQLite integrity, agent readiness, dataset preview, HTTPS responses, and rejection of unauthenticated catalog access.

## Protected Savva transfer on dsc-node

- Preserve `/home/node/transferduck.py`, its navigation symlink under `node-operations/transfers`, `/home/node/transfer.log`, and the `node` crontab entry (Monday 01:30 server-local time, working directory `/home/node`).
- It uses `/usr/bin/python3` and `requests` to read `/home/node/DSC-Data/accepted-compacted/`, compare against `http://155.246.103.59:8080/catalouge`, and upload missing files to `/update`. The destination belongs to Savva (`spetrov1@155.246.103.59`). The current script excludes paths containing `state`.
- Keep `DSC-Data` out of wholesale cleanup: its accepted data is an active transfer dependency. Preserve the transfer and required data unless the user explicitly changes that requirement. Do not run the transfer or modify the destination backend merely to inspect it.
- Authorized node cleanup completed on 2026-10-23: only `/home/node/eth_warehouse_v2` and `/home/node/eth_warehouse_data` were deleted, after retiring their old jobs. The live node watchdog now writes status to `/home/node/reth-data/node_status.json`; its service remains active. Savva's transfer script and accepted-data manifest hashes were verified unchanged. Other node folders remain outside the approved deletion scope.

- dsc-data cleanup completed: only the `dsc-data` human login and home remain; required OS/service accounts are retained. The retired `dsc-link` account is removed. New scripts, environments, pilot data, source checkout, and Git bundle are preserved. Root usage is approximately 37 GiB with 854 GiB free. See [server migration](server-migration.md). The `fao` account on **dsc-web** must remain: the migrated application uses it.

## Extraction coordinator (2026-10-23)

- dsc-data is the orchestrator; run `scripts/run.py` there. dsc-web continues to host the backend/frontend. This supersedes the earlier database-only restriction for orchestration.
- Use private LAN addresses for server-to-server SSH/transfers: dsc-node `node@10.246.103.160`, dsc-revert `dsc-revert@10.246.103.26`, dsc-data `dsc-data@10.246.103.151`. These identities were verified against host keys through the existing connections. Tailscale destinations remain available for operator access; the runner does not depend on them.
- The coordinator stores SQLite on dsc-data, with one combined logical event/state/reverted bitmask (1/2/4), compact interval coverage and explicit empty-result evidence. Set flags only after destination acceptance. See `scripts/orchestration/README.md` for CLI, deployment and current limits.
- No full historical extraction or scheduled runner is enabled by deployment. Full Aave historical ABI profiles remain a prerequisite.

## Private monitor

- View-only monitor: `https://dsc-web.tail5aad6c.ts.net/`, served by `dsc-monitor.service` on dsc-web via Tailscale Serve. No public Funnel exposure; existing beta services remain separate.
- Monitor reports are collected every 30 seconds over LAN SSH from dsc-web (10.246.103.143) to dsc-data (10.246.103.151), using a dedicated restricted fixed-command key. SQLite is opened read-only; monitor requests cannot control extraction or write tracker state.
- Production and pilot coverage are separate. Use immutable recorded run targets/configs when calculating progress; never infer completion percentages from unscoped legacy coverage. See `scripts/monitor/README.md`.

## Historical production run (2026-10-23)

- This supersedes the earlier notes that profiles and a production service are missing. Fifteen Aave profiles (4 V2, 11 V3) cover deployment through finalized block 26,259,811 inclusive; source ABIs, interval evidence, and real Cryo/decode pilot results are in `contracts/ethereum/aave-profiles/` and protocol `history/` directories.
- `dsc-extraction.service` on dsc-data runs the 102-address registry, state every block, with that fixed cutoff and `--cleanup`. It is enabled at boot and restarts failed coordinator processes after 60 seconds using the existing tracker. It does not automatically extend into unreviewed future ABI intervals. See `scripts/orchestration/production-run.md`.
- Historical upgrade discovery uses Blockscout's paginated index; every returned boundary is checked against RETH. Sourcify verified ABI runtime bytecode is matched to RETH. Each extraction batch still rejects unrecorded upgrades. Profiles retain ABI-specific schemas rather than silently normalizing changed fields.
