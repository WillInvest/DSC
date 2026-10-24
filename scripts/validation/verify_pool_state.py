#!/usr/bin/env python3
"""Check Cryo state-call coverage and ABI output lengths; does not fetch or decode."""
import argparse
from pathlib import Path
import re
import sys

import pyarrow.parquet as pq


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--blocks', required=True)
    parser.add_argument('--address', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--catalog', type=Path, required=True)
    parser.add_argument('--preflight', action='store_true')
    args = parser.parse_args()
    if not re.fullmatch(r'[0-9]+(?::[0-9]+(?::[0-9]+)?)?', args.blocks):
        parser.error('use BLOCK or START:END_EXCLUSIVE[:STEP]')
    parts = [int(value) for value in args.blocks.split(':')]
    start = parts[0]
    stop = parts[1] if len(parts) > 1 else start + 1
    step = parts[2] if len(parts) > 2 else 1
    if stop <= start or step <= 0:
        parser.error('range must be nonempty and step positive')
    blocks = range(start, stop, step)
    if not re.fullmatch(r'0x[0-9a-fA-F]{40}', args.address):
        parser.error('invalid pool address')
    selectors = {}
    for line in args.catalog.read_text().splitlines():
        name, selector, length = line.split('\t')
        if selector in selectors or not re.fullmatch(r'0x[0-9a-f]{8}', selector):
            raise ValueError('duplicate or invalid function selector')
        selectors[selector] = (name, int(length))
    if not selectors:
        raise ValueError('expected configured state getters')
    if args.preflight:
        return
    seen = set()
    columns = ['block_number', 'contract_address', 'call_data', 'output_data', 'chain_id']
    for path in sorted(args.output.glob('*.parquet')):
        for batch in pq.ParquetFile(path).iter_batches(columns=columns):
            for row in batch.to_pylist():
                block = row['block_number']
                if block not in blocks:
                    continue
                if row['chain_id'] != 1 or row['contract_address'].lower() != args.address.lower():
                    raise ValueError(f'wrong chain or pool in {path}')
                selector = row['call_data']
                if selector not in selectors:
                    raise ValueError(f'unknown function selector in {path}: {selector}')
                name, length = selectors[selector]
                key = (block, selector)
                if key in seen:
                    raise ValueError(f'duplicate result: block {block}, {name}')
                output = row['output_data']
                if not isinstance(output, str) or not re.fullmatch(r'0x[0-9a-fA-F]+', output):
                    raise ValueError(f'null or invalid result: block {block}, {name}')
                if len(bytes.fromhex(output[2:])) != length:
                    raise ValueError(f'wrong ABI return length: block {block}, {name}')
                seen.add(key)
    expected = len(blocks) * len(selectors)
    if len(seen) != expected:
        raise ValueError(f'incomplete state coverage: {len(seen)} of {expected} calls')
    print(f'Validated {len(seen)} non-null calls across {len(blocks)} blocks; {len(selectors)} getters per block.')


if __name__ == '__main__':
    try:
        main()
    except (ValueError, OSError) as exc:
        print(f'error: {exc}', file=sys.stderr)
        sys.exit(1)
