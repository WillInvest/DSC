# Selected Ethereum contracts

102 contracts: 50 Uniswap V2 pairs, 50 Uniswap V3 pools, and the Aave V2 main
and V3 Core market Pools. Full addresses and activity ranks are in
[selection.json](selection.json); on-chain verification is in [pools.csv](pools.csv).

## Selection

The user accepted The Graph lifetime `txCount` as a practical activity proxy.
Rank each Uniswap version separately at finalized block **26,259,428** and
select 50, without TVL, token, volume, age, or current-activity filters.
`txCount` includes swaps and liquidity operations (including V3 collections),
so this is **pool activity, not swap-only counts or distinct transactions**.
Old pools with substantial historical activity may be selected even if quiet today.
Token symbols are display labels; addresses identify tokens and pools.

[Graph evidence](graph/provenance.json) records subgraph IDs and checksums.
The saved queries and responses include the top 100 candidates per version;
local ordering uses descending integer txCount then ascending address. Counts
at ranks 50 and 51 differ for both versions, so boundary ties are absent.
Both subgraphs reported no indexing errors at the fixed snapshot. Their metadata
returned a null block hash; the canonical snapshot hash is recorded from RETH,
so no independent Graph block-hash comparison was possible.
This replaces the previous Dune plan and the manually curated nine-address sample.
`selection-evidence.csv` is legacy sample evidence and is not used for this ranking.

The node verifier checks token addresses, fees, canonical factory lookup and
first deployed code. Aave addresses are checked against the official address book.
[manifest.json](manifest.json) records verification and file hashes.
The node snapshot directory is `data/contract-registry/graph-26259428` under
`/home/node/dsc-extraction`.

Extraction coverage remains deployment through a captured finalized boundary,
with configured state at every block. Aave still requires historical ABI profiles.
User-position snapshots remain excluded. Selection does not start extraction;
the unified completion tracker remains pending.
