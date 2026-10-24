# Aave V3 Pool (reserve scope)

Official ABI: `@aave/core-v3@1.19.3`, artifact
`artifacts/contracts/protocol/pool/Pool.sol/Pool.json`.
Source: https://registry.npmjs.org/@aave/core-v3/-/core-v3-1.19.3.tgz
The checked-in ABI is the complete, unmodified artifact ABI.

ABI SHA-256: `5cfd3a18823c74e60553b84db8d444d66c8b51347454385bc306ea19733b2abe`.

This is a pinned ABI revision, not a claim that every Aave V3 deployment or
historical implementation uses it. A reviewed deployment profile is required;
see the project README. No production deployment or range is selected.

State: `getReservesList()` at each requested end-of-block state, then
`getReserveData(asset)` for every returned reserve. This includes configuration,
indices, rates, token addresses, and accounting values exposed by that getter;
it is not a complete balance sheet or user-position snapshot.

Historical production extraction selects a profile-specific ABI under `history/`;
the package ABI above remains the default for earlier isolated pilots. Verified
source records and derived extraction ABIs are kept separately. See
[historical profiles](../../../../../contracts/ethereum/aave-profiles/README.md).
