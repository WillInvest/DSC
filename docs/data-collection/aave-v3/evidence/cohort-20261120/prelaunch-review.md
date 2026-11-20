# Cohort replay prelaunch review

Read-only review of `scripts/user-history/hf_market_replay` against `docs/plans/2026-11-20-1230-feat-node-non-emode-hf-plan.md`, 2026-11-20. No production, Git, authentication, service, or archive mutation was performed.

## Findings

### P2 — Bind host-local completion state to the plan generation

`ledger.py:116–124` creates `LocalResults` with only a path and returns all saved job IDs. `runner.py:128–129` trusts those IDs as completed for whichever plan is currently loaded, and `runner.py:160` skips them without opening their receipts. `dispatch.py:19–30` reads the current plan and overwrites the reported generation before returning the old completion rows.

If a worker root is reused for a canary-to-production promotion or a later generation with overlapping job IDs, the worker skips required computation and publishes old descriptors under the new plan's reported generation. The node then rejects their receipt identity and stops. This does not silently accept incorrect numerical data, but it defeats restart/reuse and can block the entire run.

The smallest correction is to bind `LocalResults` to generation and scope on open, as the node `Ledger` already does, and reject a mismatched root before `completed()`. Test reopening an existing local ledger with a different generation. Proven distinct worker roots for every generation avoid this trigger in a single deployment, but the module currently does not enforce that contract.

### P2 — Pin physical encoding parameters shared by duplicate attempts

`runner.py:80–83` reads `part_blocks` from host-local configuration. `runner.py:33–38` defines the supposedly comparable payload identity using physical part names, ranges, bytes and SHA hashes. `coordinator.py:100–101` stops the entire coordinator if a late duplicate differs in that identity.

Two valid attempts at the same leased job can therefore compute the same scientific series but use different allowed encoding windows, such as 128 blocks on one helper and the default 256 on another. Their part ranges and hashes differ, so a late result following Data's offline reassignment is treated as a conflicting numerical completion. Both outputs pass the current result validator.

Pin `part_blocks` in the plan's identity and require every role configuration to match it. This is smaller than introducing a new canonical value digest, and a universal 256-block window matches the current canary. Add a test that a host-local encoding override incompatible with the plan is rejected before work starts.

## Reviewed behavior with no identified blocker

- `calculation.py:141–156` evaluates the unchanged wallet model before filtering. `cohorts.py:46–55` applies the wallet's end-of-block category consistently to all positive debt edges and retains the account-level dust policy from the existing evaluator.
- Reserve and protocol N/W/HF, null denominator behavior, cohort partition arithmetic, borrower counts and start/middle/end independent reductions are checked by `validation.py`.
- The new node ledger commits acceptance before advancing each remote cursor. Restart can repeat that receipt without advancing coverage twice.
- Connectivity failures and stale heartbeats stop renewal for the unavailable role, allowing finite leases to expire and be assigned to online roles. Local workers retain completed receipts through controller/network absence.
- `retirement.py:23–60` selects only pinned old-generation result Parquets or packs from accepted old archive indices, and its current command only verifies remote/local bytes. Reconstruction inputs and standalone metadata are outside the selected payload set.

## Coverage and remaining execution evidence

The parent supplied a passing two-block real-input canary, six own-node HF/debt/category samples using 54 RPC, exact agreement of the unfiltered protocol sample with the prior release, and a passing 256-block late-history benchmark. I inspected the six local execution tests and seven numerical tests. I did not rerun the numerical tests in this read-only pass because the local numerical dependencies are absent.

Real worker CLI publication, node receipt transfer/acceptance, a four-host Data-outage/late-duplicate pilot and any future privileged archive-retirement policy change still require parent-side verification. Those operations were not represented as completed by this review. No scientific cohort-selection defect was found in the reviewed code.
