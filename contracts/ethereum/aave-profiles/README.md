# Aave Ethereum historical profiles

The JSON files in this directory cover the two Pool addresses in `../pools.csv`.
Intervals are `[from_block, to_block)` and include creation and upgrade blocks.
The final interval ends at the fixed finalized cutoff in
`evidence/validation.json`; it must be reviewed/refreshed before advancing past
that block. These are reserve-level profiles, not user-position snapshots.

Profiles pin the implementation address, runtime bytecode SHA-256, extraction
ABI SHA-256, schema version, previous implementation, and exact boundary upgrade
logs. `scripts/registry/build_aave_profiles.py` discovers the chronology through
[Blockscout's indexed log API](https://docs.blockscout.com/api-reference/get-address-logs),
following every pagination cursor and retaining responses. Every returned upgrade
block is checked against RETH's native log/storage APIs using Web3.py. It obtains verified implementation
and proxy ABIs from [Sourcify API v2](https://docs.sourcify.dev/docs/api/) and checks
the returned on-chain runtime bytecode against RETH before using them.

Verified source records live under each protocol's `history/sources/` directory.
The extraction ABI in `history/<start>.json` retains the current implementation's
functions and outputs, and combines its events with the proxy events and events
from implementations active earlier in the upgrade block. Conflicting event
layouts fail generation. This composition is derived from verified ABIs; it is
not an unmodified official package artifact.

Index completeness is a discovery dependency: a missing upgrade blocks extraction
when the native verifier reaches it, rather than silently satisfying coverage.
The runtime verifier checks the previous/end-of-block implementation and exact
reviewed upgrade logs. End-of-block state uses the new implementation on an
upgrade block; events from before the upgrade remain decodable. No initialization
or upgrade block is silently skipped. Each profile is validated at its first
and last block: reserve discovery, every returned reserve's configured getter,
and every Pool-address event emitted in those sampled blocks. The evidence file
records sample counts and canonical block hashes. This is boundary validation,
not a claim to have extracted every historical block already.

Schemas follow the verified ABI, including output field names, and are recorded
per profile. In newer V3 revisions `getReserveData` is a legacy compatibility
getter; deprecated fields can return zero. Retain the values returned by the
contract, without interpreting those zeros as complete current accounting.
See [Aave's current types](https://github.com/aave-dao/aave-v3-origin/blob/main/src/contracts/protocol/libraries/types/DataTypes.sol).

Generate into a fresh staging directory using the ABI Python environment on
dsc-node (the script also writes ABI artifacts relative to its own code tree):

```bash
python scripts/registry/build_aave_profiles.py \
  --contracts contracts/ethereum/pools.csv \
  --output contracts/ethereum/aave-profiles \
  --to-block FINALIZED_CUTOFF
```

Review validation, deploy identical protocol ABI files to node and data, and copy
the profile JSON files to `/home/dsc-data/data/deployment-profiles/`. Start the
coordinator with `--profiles` pointing there and `--to-block` set to the validated
cutoff. Generate into a fresh directory when extending coverage, so obsolete
intervals cannot overlap newer ones. Existing tracker completion uses the
profile identity and cannot be silently relabelled with a changed ABI.

## Validated release

15 profiles (4 V2, 11 V3) cover deployment through block 26,259,811 inclusive.
All 30 interval endpoints passed getter/event checks. Eight real Cryo-to-decoder
cases passed, including old implementations, latest upgrade blocks, empty event
results, and the V3 legacy-output naming change. See `evidence/cryo-pilot.json`.
