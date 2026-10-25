"""ABI-aware initial snapshots for Aave creation and upgrade boundaries only."""
import json
from web3 import Web3
from protocols.protocol_config import load_protocol
from reconstruction.core import AAVE_FIELDS


def snapshot(w3, protocol, address, block, profile=None):
    config = load_protocol(protocol, profile)
    contract = w3.eth.contract(address=Web3.to_checksum_address(address),
                              abi=json.loads(config['abi'].read_text()), decode_tuples=True)
    def call(name, *args):
        return getattr(contract.functions, name)(*args).call(block_identifier=block)
    if protocol == 'uniswap-v2':
        values=call('getReserves')
        return dict(reserve0=int(values[0]),reserve1=int(values[1]))
    if protocol == 'uniswap-v3':
        slot=call('slot0')
        return dict(sqrtPriceX96=int(slot[0]),tick=int(slot[1]),liquidity=int(call('liquidity')))
    result={}
    for asset in call('getReservesList'):
        values=call('getReserveData',asset)._asdict()
        result[asset.lower()]={field:int(values[field]) for field in AAVE_FIELDS}
    return result
