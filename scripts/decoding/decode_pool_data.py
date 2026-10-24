#!/usr/bin/env python3
"""Decode Cryo pool event/state Parquet offline with the official ABI and eth-abi."""
import argparse
import json
from importlib.metadata import version
from pathlib import Path
import re
import sys
from tempfile import TemporaryDirectory

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from decoding.decode_storage import checksum, inventory, verify_existing, write_batches, publish
from decoding.pool_decode import PoolDecoder
from protocols.protocol_config import load_protocol, PROTOCOLS
from protocols.deployment import load_deployment, block_range
from validation.reserve_state import verify_reserves

VERSION = 3


def run(kind, source, output, abi_path, address, protocol="uniswap-v3", deployment=None):
    protocol_config = load_protocol(protocol)
    source, output, abi_path = source.resolve(), output.resolve(), abi_path.resolve()
    address = address.lower()
    if not re.fullmatch(r'0x[0-9a-f]{40}', address):
        raise ValueError('invalid pool address')
    if source == output or source in output.parents or output in source.parents:
        raise ValueError('input and output directories must be separate, not nested')
    profile = None
    if protocol_config.get('requires_deployment'):
        default_abi = protocol_config['abi'].resolve()
        profile = load_deployment(deployment or source / 'deployment.json', protocol_config, address)
        protocol_config = load_protocol(protocol, profile)
        if abi_path not in (default_abi, protocol_config['abi'].resolve()):
            raise ValueError('deployment-pinned protocols do not allow ABI overrides')
        abi_path = protocol_config['abi'].resolve()
    inputs = inventory(source)
    if profile and kind == 'state':
        settings = json.loads((source / 'extraction.json').read_text())
        if settings['protocol'] != protocol or settings['address'].lower() != address:
            raise ValueError('extraction settings mismatch')
        blocks = block_range(settings['blocks'])
        verify_reserves(source, protocol_config, address, blocks)
    config = dict(
        version=VERSION, protocol=protocol, state_getters=protocol_config['state_getters'],
        kind=kind, address=address, abi_sha256=checksum(abi_path), inputs=inputs,
        libraries={name: version(name) for name in ['web3', 'eth-abi', 'eth-utils', 'pyarrow']})
    if profile:
        config.update(version=4, deployment=profile, schema_version=protocol_config['schema_version'])
    if verify_existing(output, config):
        return
    decoder = PoolDecoder(kind, abi_path, address, protocol_config['state_getters'],
                          schema_version=protocol_config['schema_version'] if profile else None,
                          deployment=profile)
    output.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix='.' + output.name + '-', dir=output.parent) as workspace:
        temporary = Path(workspace) / 'decoded'
        temporary.mkdir()
        counts = write_batches(source, temporary, config, decoder)
        if inventory(source) != config['inputs'] or checksum(abi_path) != config['abi_sha256']:
            raise ValueError('source files or ABI changed during decoding')
        publish(temporary, output, counts, config)
        print(f'Decoded {sum(counts.values())} {kind} rows into {output}; encoded inputs retained.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--kind', choices=['events', 'state'], required=True)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--address', required=True)
    parser.add_argument('--protocol', choices=PROTOCOLS, required=True)
    parser.add_argument('--deployment', type=Path, help='Reviewed deployment profile; defaults to INPUT/deployment.json')
    parser.add_argument('--abi', type=Path, help='Override the pinned protocol ABI explicitly')
    args = parser.parse_args()
    run(args.kind, args.input, args.output, args.abi or load_protocol(args.protocol)['abi'], args.address, args.protocol, args.deployment)


if __name__ == '__main__':
    main()
