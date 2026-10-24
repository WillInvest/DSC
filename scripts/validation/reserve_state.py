import json
import pyarrow.parquet as pq
from web3 import Web3
from decoding.pool_decode import StateDecoder
from protocols.protocol_config import reserve_calls


def rows(directory):
    for path in sorted(directory.rglob('*.parquet')):
        for batch in pq.ParquetFile(path).iter_batches():
            yield from batch.to_pylist()


def validate_row(row, address, blocks):
    if row['chain_id'] != 1 or row['contract_address'].lower() != address.lower():
        raise ValueError('wrong chain or contract in state data')
    if row['block_number'] not in blocks:
        raise ValueError('state data contains an unrequested block')


def discover_reserves(directory, config, address, blocks):
    abi = json.loads(config['abi'].read_text())
    contract = Web3().eth.contract(abi=abi)
    call = contract.encode_abi(config['discovery_function']).lower()
    decoder = StateDecoder(abi, [config['discovery_function'].split('(')[0]])
    reserves = {}
    for row in rows(directory):
        validate_row(row, address, blocks)
        block = row['block_number']
        if row['call_data'].lower() != call or block in reserves:
            raise ValueError('duplicate or unexpected discovery call')
        assets = [asset.lower() for asset in decoder.decode(row)[2][0]]
        if len(assets) != len(set(assets)):
            raise ValueError('duplicate reserve in discovery result')
        reserves[block] = assets
    if len(reserves) != len(blocks):
        raise ValueError('incomplete reserve discovery coverage')
    return contract, reserves


def verify_reserves(directory, config, address, blocks):
    for path in directory.rglob('*.parquet'):
        if path.relative_to(directory).parts[0] not in {'discovery', 'reserves'}:
            raise ValueError(f'Parquet file outside validated coverage folders: {path}')
    contract, reserves = discover_reserves(directory / 'discovery', config, address, blocks)
    expected = {(block, call) for block, assets in reserves.items()
                for call in reserve_calls(config, contract, assets)}
    decoder = StateDecoder(json.loads(config['abi'].read_text()), config['state_getters'])
    seen = set()
    for row in rows(directory / 'reserves'):
        validate_row(row, address, blocks)
        key = decoder.decode(row)[3]
        if key not in expected or key in seen:
            raise ValueError('unexpected or duplicate reserve state call')
        seen.add(key)
    if seen != expected:
        raise ValueError(f'incomplete reserve state coverage: {len(seen)} of {len(expected)} calls')
    return len(seen) + len(reserves)
