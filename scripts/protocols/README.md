# Official Uniswap ABIs

Source: https://unpkg.com/@uniswap/v3-core@1.0.1/artifacts/contracts/UniswapV3Pool.sol/UniswapV3Pool.json

Pinned official npm package: `@uniswap/v3-core@1.0.1`.
Artifact SHA-256: `204fb2fa7ad1a639b1df5601f035f9924b064b721b97adf60c7d5d4712643e34`.

`UniswapV3Pool.json` contains the unmodified ABI array from that artifact.
Events are decoded on dsc-data with Web3.py `process_log`; function returns use
`eth-abi`. Web3.py resolves event topics against this ABI; getter selectors use
`eth-utils`. No hand-maintained event signature catalog is needed.
Preserve this provenance when changing the ABI.

`dex/uniswap/v3/state.tsv` selects the five agreed no-argument view getters
from the same official ABI: function name, four-byte call selector, and expected
return length in bytes. Selectors are the first four bytes of RETH `web3_sha3`
of the canonical signature. All selected returns are static ABI types, so the
expected length is 32 bytes per output field. This catalog does not reconstruct
state or decode returned bytes.

## Uniswap V2 pair

Source: https://unpkg.com/@uniswap/v2-core@1.0.1/build/UniswapV2Pair.json

Pinned official npm package: `@uniswap/v2-core@1.0.1`.
Artifact SHA-256: `bb00cb1d64deea4159731b5957eeb8354531a110fb9120b9ef14edbc950e0736`.
`UniswapV2Pair.json` is the unmodified ABI array from this artifact.
`dex/uniswap/v2/state.tsv` contains the ABI-derived selectors and
static return lengths for getReserves, totalSupply, price0CumulativeLast,
price1CumulativeLast, and kLast. It was generated with eth-utils and is checked
against the pinned ABI by tests. No deployment addresses are configured here.
