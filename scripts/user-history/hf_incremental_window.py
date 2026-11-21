"""Pin a contiguous finalized Aave HF input window after a sealed boundary."""
import argparse
import json
from pathlib import Path
import re

from shared.artifacts import checksum, digest, write_json

FORMAT = 'aave-v3-hf-input-window-v1'


def block_hash(value):
    if not isinstance(value,str) or not re.fullmatch(r'0x[0-9a-fA-F]{64}',value):
        raise ValueError('Invalid Ethereum block hash')
    return value.lower()


def validate_window(window):
    scope = window['identity']
    headers = window['headers']
    if (scope['format'] != FORMAT or digest(scope) != window['window_sha256']
            or digest(headers) != scope['headers_sha256']
            or [h['block'] for h in headers] != list(range(scope['start'],scope['stop']))
            or not headers or headers[-1]['hash'] != scope['end_hash']):
        raise ValueError('Invalid incremental HF header evidence')
    return scope


def pin_window(rpc, start, stop, predecessor_hash, *, generation, checkpoint_sha256):
    if not 0 < start < stop:
        raise ValueError('Invalid HF window')
    if int(rpc('eth_chainId', []), 16) != 1:
        raise ValueError('HF window must use Ethereum mainnet')
    finalized = rpc('eth_getBlockByNumber', ['finalized',False])
    if stop-1 > int(finalized['number'],16):
        raise ValueError('Requested HF window is not finalized')
    parent = rpc('eth_getBlockByNumber', [hex(start-1),False])
    predecessor_hash = block_hash(predecessor_hash)
    if int(parent['number'],16) != start-1 or block_hash(parent['hash']) != predecessor_hash:
        raise ValueError('Accepted HF predecessor hash differs')
    headers = []
    previous = predecessor_hash
    for height in range(start,stop):
        raw = rpc('eth_getBlockByNumber', [hex(height),False])
        if int(raw['number'],16) != height or block_hash(raw['parentHash']) != previous:
            raise ValueError(f'HF header lineage breaks at block {height}')
        previous = block_hash(raw['hash'])
        headers.append(dict(block=height, hash=previous, timestamp=int(raw['timestamp'],16)))
    identity = dict(format=FORMAT, chain_id=1, start=start, stop=stop, end_hash=previous,
                    predecessor_hash=predecessor_hash, release_generation=generation,
                    checkpoint_sha256=checkpoint_sha256, headers_sha256=digest(headers))
    return dict(identity=identity, window_sha256=digest(identity), headers=headers)


def main():
    from hf_incremental_boundary import FORMAT as BOUNDARY_FORMAT
    from pipeline.runtime import rpc

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--boundary', required=True, type=Path)
    parser.add_argument('--end-block', required=True, type=int)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    boundary = json.loads(args.boundary.read_text())
    if boundary['format'] != BOUNDARY_FORMAT or checksum(args.boundary.parent/'state.json.gz') != boundary['checkpoint']['sha256']:
        raise ValueError('Invalid boundary evidence')
    window = pin_window(rpc,boundary['next_block'],args.end_block+1,boundary['predecessor_hash'],
                        generation=boundary['release_generation'],
                        checkpoint_sha256=boundary['checkpoint']['sha256'])
    if args.output.exists():
        if json.loads(args.output.read_text()) != window:
            raise ValueError('Refuse to change the pinned HF input window')
    else:
        write_json(args.output,window)
    print(json.dumps(window['identity']))


if __name__ == '__main__':
    main()
