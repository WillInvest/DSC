// Export main-contract metadata from official sources; no pool discovery.
const path = require('node:path');
const book = require(path.join(path.resolve(process.argv[2]), '@aave-dao/aave-address-book'));
const factory = file => require(path.join(path.resolve(process.argv[2]), file)).abi;
console.log(JSON.stringify({
  uniswap_v2: {
    address: '0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f',
    abi: factory('@uniswap/v2-core/build/UniswapV2Factory.json'),
    source: 'https://developers.uniswap.org/docs/protocols/v2/deployments',
  },
  uniswap_v3: {
    address: '0x1F98431c8aD98523631AE4a59f267346ea31F984',
    abi: factory('@uniswap/v3-core/artifacts/contracts/UniswapV3Factory.sol/UniswapV3Factory.json'),
    source: 'https://developers.uniswap.org/docs/sdks/v3/guides/managing-liquidity/position-minting',
  },
  ...Object.fromEntries([['aave_v2', 'AaveV2Ethereum'], ['aave_v3', 'AaveV3Ethereum']].map(([key, name]) => [key, {
    address: book[name].POOL, module: name,
    assets: Object.fromEntries(['USDC', 'WETH', 'USDT', 'DAI', 'WBTC'].map(symbol =>
      [symbol, book[name].ASSETS[symbol].UNDERLYING])),
    source: '@aave-dao/aave-address-book@4.75.0',
  }])),
}, null, 2));
