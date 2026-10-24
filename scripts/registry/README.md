# Curated pool registry

This verifies an explicitly selected list of large Ethereum pools. It does not
scan every factory pool or launch historical extraction. Edit
`contracts/ethereum/selection.json` to change the selection.

`sources.cjs` exports official Uniswap factory ABIs and the Aave address book.
`build_registry.py` uses Web3.py to check chain/finality, token identities, fee
tiers, canonical factory mappings, and deployment-code boundaries. Custom logic
only joins the selected addresses to that evidence and writes the registry.

Run on dsc-node with the existing ABI environment:

```bash
SOURCE_DIR=/home/node/dsc-extraction/data/contract-registry/sources
mkdir -p "$SOURCE_DIR"
cp scripts/registry/package{,-lock}.json "$SOURCE_DIR/"
npm ci --prefix "$SOURCE_DIR" --ignore-scripts --no-audit --no-fund
node scripts/registry/sources.cjs "$SOURCE_DIR/node_modules" > "$SOURCE_DIR/../selected-sources.json"
.venv-abi/bin/python scripts/registry/build_registry.py \
  --sources "$SOURCE_DIR/../selected-sources.json" \
  --selection contracts/ethereum/selection.json \
  --output "$SOURCE_DIR/../selected-26259428" --block 26259428
```

The block argument is inclusive. Use a fresh output directory per snapshot or
selection. Verify CSV, source, and selection hashes against `manifest.json`.
The binary search for deployment assumes persistent contract code, as applies
to these Uniswap pairs/pools and Aave Pool proxies. It is not a general detector
for contracts that have been destroyed and redeployed.

Sources:
- [Uniswap V2 core](https://github.com/Uniswap/v2-core), npm version 1.0.1
- [Uniswap V3 core](https://github.com/Uniswap/v3-core), npm version 1.0.1
- [Aave address book](https://github.com/aave-dao/aave-address-book), version 4.75.0
- [Web3.py contract API](https://web3py.readthedocs.io/en/stable/web3.contract.html)

## Graph activity ranking

The current selection uses The Graph `txCount`, accepted as an activity proxy
that includes swaps and liquidity operations. See `contracts/ethereum/graph`
for exact queries, original responses, subgraph IDs and checksums. Submit the
saved GraphQL queries to The Graph gateway using a private API token; never
save credentials here. Sort each response by descending integer txCount and
ascending address, then take 50 after checking the boundary is not tied.
The fixed query fetches 100 candidates so the 50/51 boundary can be checked.
If the boundary is tied, fetch all tied candidates before choosing by address.

Selected Uniswap entries require `token0`, `token1`, and `fee_pips`; token symbols
are labels, not validation inputs. The existing Web3.py verifier checks these
against the chain and official factory mappings. Retain the two Aave entries.
Use `graph-selection.json` and a fresh `graph-26259428` output directory for this
snapshot instead of the earlier sample paths in the example above.

`top_swapped_pools.sql` is the superseded, unexecuted Dune swap-only query.
