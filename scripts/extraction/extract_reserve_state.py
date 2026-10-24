#!/usr/bin/env python3
"""Use Cryo for discovery and parameterized end-of-block reserve calls."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from web3 import Web3
from protocols.protocol_config import load_protocol, PROTOCOLS, reserve_calls
from protocols.deployment import block_range, load_deployment, verify_deployment, save_profile
from validation.reserve_state import discover_reserves, verify_reserves


def cryo_calls(address, calls, blocks, output, label):
    subprocess.run([
        os.environ.get('CRYO_BIN', '/home/node/.cargo/bin/cryo'), 'eth_calls',
        '--rpc', os.environ.get('ETH_RPC_URL', 'http://127.0.0.1:8545'),
        '--contract', address, '--call-data', *calls, '--blocks', blocks,
        '--output-dir', str(output), '--chunk-size', os.environ.get('CHUNK_SIZE', '1000'),
        '--max-concurrent-requests', os.environ.get('MAX_CONCURRENT_REQUESTS', '8'),
        '--max-concurrent-chunks', os.environ.get('MAX_CONCURRENT_CHUNKS', '8'),
        '--hex', '--label', label], check=True)


def extract(config, address, block_spec, output, profile, w3):
    blocks = block_range(block_spec)
    verify_deployment(w3, profile, blocks)
    save_profile(output, profile)
    settings = {'protocol': config['protocol'], 'blocks': block_spec, 'address': address.lower(),
                'state_calls': config['state_calls'], 'discovery_function': config['discovery_function'],
                'schema_version': config['schema_version']}
    metadata = output / 'extraction.json'
    if metadata.exists() and json.loads(metadata.read_text()) != settings:
        raise ValueError('output has different extraction settings; use a new batch directory')
    metadata.write_text(json.dumps(settings, indent=2) + '\n')
    contract = Web3().eth.contract(abi=json.loads(config['abi'].read_text()))
    discovery = contract.encode_abi(config['discovery_function'])
    label = config['protocol'] + '_' + address.lower()
    cryo_calls(address, [discovery], block_spec, output / 'discovery', label + '_discovery_v1')
    contract, reserves = discover_reserves(output / 'discovery', config, address, blocks)
    for block, assets in sorted(reserves.items()):
        calls = reserve_calls(config, contract, assets)
        if calls:
            cryo_calls(address, calls, str(block), output / 'reserves' / str(block), label + '_reserves_v1')
    count = verify_reserves(output, config, address, blocks)
    verify_deployment(w3, profile, blocks)
    print(f'Validated {count} discovery/reserve calls across {len(blocks)} blocks.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('protocol', choices=PROTOCOLS)
    parser.add_argument('address')
    parser.add_argument('blocks')
    parser.add_argument('--deployment', required=True, type=Path)
    args = parser.parse_args()
    config = load_protocol(args.protocol)
    if config['state_mode'] != 'reserves':
        parser.error('protocol does not use reserve discovery')
    profile = load_deployment(args.deployment, config, args.address)
    output = Path(os.environ.get('OUTPUT_DIR', f'/home/node/dsc-extraction/data/{args.protocol}/{args.address.lower()}/state-raw'))
    w3 = Web3(Web3.HTTPProvider(os.environ.get('ETH_RPC_URL', 'http://127.0.0.1:8545'), request_kwargs={'timeout': 60}))
    extract(config, args.address, args.blocks, output, profile, w3)


if __name__ == '__main__':
    main()
