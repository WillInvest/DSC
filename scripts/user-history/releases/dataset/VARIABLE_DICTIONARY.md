# Variables and indexing

## Wallet vectors

| Field | Meaning |
|---|---|
| `wallet_id` | uint32 ID in `users.parquet`; stable within this dataset |
| `first_block_index` | Address's first block minus16291127; not necessarily this tile's first block |
| `hf_vector` | List of float64 analytical HF values for consecutive blocks |
| Parquet `chunk_start`, `chunk_stop` | Actual block bounds of the physical tile; stop is exclusive |

For element `i` in a wallet row, block number is
`max(chunk_start, 16291127 + first_block_index) + i`.
An address is retained after exiting; debt-free/excluded states have positive
infinity. Never treat infinity as zero or include it in a weighted mean.

## Reserve/protocol vectors

| Field | Meaning |
|---|---|
| `scope_id` | Lookup in `dataset.json.aggregate_scope_mapping` |
| `first_block_index` | Common origin metadata; actual segment begins at `chunk_start` |
| `hf_vector` | `numerator_vector / denominator_vector`; null where denominator is zero |
| `numerator_vector` | Sum of included user HF times that user's debt in this scope |
| `denominator_vector` | Sum of included debt, in Aave base currency units |
| `excluded_users_vector` | Protocol-only per-block count of debt-position owners excluded by the threshold |
| `excluded_debt_base_vector` | Protocol-only excluded debt value estimate in base units |

Reserve weighting uses debt owed in that reserve, while the user's HF incorporates
the user's entire collateral/debt account. Protocol weighting uses total debt.
The scope mapping determines the protocol row; it is not assumed to be zero.
Ethereum V3 base values use1USD=100000000 base units. Exclusion diagnostics are
float64 estimates, not exact integer accounting or proof of negligible effects.

## Events and Oracle prices

Raw event Parquets preserve block/transaction identity, log index, emitter,
topics and data. Event amounts are token units and require the historical token
decimals and ABI. aToken and debt-token contract addresses are distinct from
their underlying reserve address; use the token plan/profile mapping.

Oracle Parquets contain `block_number:uint32` and `prices:list<string>`.
The strings retain exact uint256 prices. Match list element order to the
corresponding interval's `assets` in the Oracle plan and divide by its `unit`
(100000000 in the nonempty Ethereum V3 intervals). Never assume an asset's list
position is fixed across intervals. Empty-market coverage has no price values.

## Reconstruction outputs

The example account reconstruction reports Aave's six raw account fields:
totalCollateralBase, totalDebtBase, availableBorrowsBase,
currentLiquidationThreshold, ltv and healthFactor. Currency uses base units;
threshold/LTV use basis points; raw finite HF uses1e18 scaling. Actual token
balances and scaled shares are exact integer strings with token decimals,
while prices, interest indices and configuration words are retained separately.
The analytical debt exclusion flag does not modify the reconstructed raw getter
values. Interest and version-dependent rounding must use the supplied evaluator.
