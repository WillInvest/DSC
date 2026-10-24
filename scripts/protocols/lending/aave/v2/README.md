# Aave V2 LendingPool (reserve scope)

Official package: `@aave/protocol-v2@1.0.1`.
Source: https://registry.npmjs.org/@aave/protocol-v2/-/protocol-v2-1.0.1.tgz
Artifact: `artifacts/contracts/protocol/lendingpool/LendingPool.sol/LendingPool.json`.
Artifact SHA-256: `474ee717601e66bc16de9d166b4a2f68e6ec59170d3ae6666b52bfa9832591dc`.
`LendingPool.json` is the complete, unmodified ABI array from this artifact.
ABI SHA-256: `e7b3ed15803a206ceb9eff65f345fd19a64e6b2ad54a85fdb02e543da20926af`.

Extract all LendingPool events, including `Deposit` (V3 uses `Supply`).
State uses `getReservesList()` at each requested block followed by
`getReserveData(asset)` for every returned reserve. V2's reserve tuple has
12 components and different field ordering/types from V3; the ABI defines its
own schema. Do not decode V2 with the V3 ABI.

User-position getters and aToken/debt-token events are excluded.
A reviewed, bounded deployment/ABI profile is required for historical extraction;
no production address or block range is selected by this configuration.
