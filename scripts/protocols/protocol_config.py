"""Load protocol definitions without putting protocol branches in shared runners."""
import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
CONFIGS = {json.loads(path.read_text())['protocol']: path for path in ROOT.glob('*/*/*/config.json')}
PROTOCOLS = tuple(sorted(CONFIGS))


def load_protocol(name):
    if name not in CONFIGS:
        raise ValueError('Use ' + ', '.join(PROTOCOLS))
    path = CONFIGS[name]
    config = json.loads(path.read_text())
    for key in ('abi', 'catalog'):
        if key in config:
            config[key] = path.parent / config[key]
    return config


def reserve_calls(config, contract, assets):
    from web3 import Web3
    return [contract.encode_abi(spec['function'], args=[Web3.to_checksum_address(asset) if arg == '$reserve' else arg
            for arg in spec['arguments']]).lower()
            for asset in assets for spec in config['state_calls']]


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('protocol', choices=PROTOCOLS)
    parser.add_argument('field', choices=['abi', 'catalog', 'state_mode', 'requires_deployment'])
    args = parser.parse_args()
    config = load_protocol(args.protocol)
    print(int(config.get(args.field, False)) if args.field == 'requires_deployment' else config[args.field])
