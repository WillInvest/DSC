#!/usr/bin/env python3
"""Verify a curated Ethereum pool list using official ABIs and Web3.py."""
import argparse
import csv
import hashlib
import json
from pathlib import Path

from web3 import Web3


def deployment_block(w3, address, end):
    """Find first code for these persistent factories and Pool proxies."""
    if not w3.eth.get_code(address, end):
        raise ValueError(f'No deployed code at snapshot: {address}')
    lo, hi = 0, end
    while lo < hi:
        mid = (lo + hi) // 2
        if w3.eth.get_code(address, mid):
            hi = mid
        else:
            lo = mid + 1
    if lo == 0 or w3.eth.get_code(address, lo - 1) or not w3.eth.get_code(address, lo):
        raise ValueError(f'Invalid deployment boundary: {address}')
    return lo


def verify_pool(w3, item, sources, block):
    protocol, address = item['protocol'], Web3.to_checksum_address(item['address'])
    source = sources[protocol.replace('-', '_')]
    token0 = token1 = ''
    fee = item.get('fee_pips', '')
    if protocol.startswith('uniswap'):
        version = protocol.rsplit('-', 1)[1]
        abi_name = 'UniswapV2Pair.json' if version == 'v2' else 'UniswapV3Pool.json'
        abi_path = Path(__file__).resolve().parents[1] / 'protocols/dex/uniswap' / version / abi_name
        pool = w3.eth.contract(address=address, abi=json.loads(abi_path.read_text()))
        token0 = pool.functions.token0().call(block_identifier=block)
        token1 = pool.functions.token1().call(block_identifier=block)
        expected = {item['token0'].lower(), item['token1'].lower()}
        factory = w3.eth.contract(address=source['address'], abi=source['abi'])
        lookup = (factory.functions.getPair(token0, token1) if version == 'v2' else
                  factory.functions.getPool(token0, token1, fee))
        actual_fee = 3000 if version == 'v2' else pool.functions.fee().call(block_identifier=block)
        if ({token0.lower(), token1.lower()} != expected or actual_fee != fee or
                lookup.call(block_identifier=block).lower() != address.lower()):
            raise ValueError(f'Pool identity mismatch: {item}')
    elif address.lower() != source['address'].lower():
        raise ValueError('Aave Pool differs from the official address book')
    return dict(chain_id=1, protocol=protocol, name=item['name'], address=address,
                creation_block=deployment_block(w3, address, block), through_block=block,
                token0=token0, token1=token1, fee_pips=fee,
                activity_rank=item.get('activity_rank', ''), tx_count=item.get('tx_count', ''))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sources', type=Path, required=True)
    parser.add_argument('--selection', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--block', type=int, required=True, help='Fixed finalized block, inclusive')
    parser.add_argument('--rpc', default='http://127.0.0.1:8545')
    args = parser.parse_args()
    w3 = Web3(Web3.HTTPProvider(args.rpc, request_kwargs={'timeout': 120}))
    if w3.eth.chain_id != 1 or not 0 < args.block <= w3.eth.get_block('finalized')['number']:
        raise ValueError('Expected Ethereum mainnet and a finalized block')
    block_hash = Web3.to_hex(w3.eth.get_block(args.block)['hash'])
    sources = json.loads(args.sources.read_text())
    selected = json.loads(args.selection.read_text())
    if len({item['address'].lower() for item in selected}) != len(selected):
        raise ValueError('Duplicate selected address')
    rows = [verify_pool(w3, item, sources, args.block) for item in selected]
    if Web3.to_hex(w3.eth.get_block(args.block)['hash']) != block_hash:
        raise ValueError('Snapshot hash changed')
    args.output.mkdir(parents=True, exist_ok=True)
    path = args.output / 'pools.csv'
    with path.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    manifest = dict(chain_id=1, scope='curated large established pools; not an exhaustive list',
                    count=len(rows), verified_through_block=args.block, block_hash=block_hash,
                    selection_sha256=hashlib.sha256(args.selection.read_bytes()).hexdigest(),
                    sources_sha256=hashlib.sha256(args.sources.read_bytes()).hexdigest(),
                    files={path.name: hashlib.sha256(path.read_bytes()).hexdigest()})
    (args.output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print(json.dumps(rows, indent=2))


if __name__ == '__main__':
    main()
