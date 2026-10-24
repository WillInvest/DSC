#!/usr/bin/env python3
"""Run on dsc-revert: verify archive or select traces calling tracked addresses."""
import argparse
import re
from pathlib import Path
if __package__:
    from .extract_reverted import load, save, validate_archive
else:
    from extract_reverted import load, save, validate_archive


def called_addresses(trace):
    # Match every destination, including successful child calls in a failed tx.
    found = {trace['to'].lower()} if trace.get('to') else set()
    for child in trace.get('calls', []):
        found.update(called_addresses(child))
    return found


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive', type=Path, required=True)
    parser.add_argument('--blocks', required=True, help='START:END_EXCLUSIVE; missing blocks fail')
    parser.add_argument('--contracts', type=Path, help='One contract address per line; omit to verify only')
    parser.add_argument('--output', type=Path, help='Required with --contracts')
    args = parser.parse_args()
    start, end = map(int, args.blocks.split(':'))
    if not 0 <= start < end:
        parser.error('Invalid range')
    contracts = set()
    if args.contracts:
        contracts = {line.strip().lower() for line in args.contracts.read_text().splitlines() if line.strip()}
        if not contracts or any(not re.fullmatch(r'0x[0-9a-f]{40}', c) for c in contracts):
            parser.error('Invalid or empty contract list')
        if not args.output or args.archive.resolve() == args.output.resolve() or args.archive.resolve() in args.output.resolve().parents:
            parser.error('Supply output outside the archive')
    count = 0
    for number in range(start, end):
        relative = Path('blocks') / f'{number // 1000:09d}' / f'{number}.json.gz'
        payload = validate_archive(load(args.archive / relative))
        if int(payload['header']['number'], 16) != number:
            raise ValueError('Archive filename/block mismatch')
        count += len(payload['failed'])
        if args.contracts:
            selected = []
            for entry in payload['failed']:
                matched = sorted(contracts & called_addresses(entry['trace']))
                if matched:
                    selected.append({**entry, 'matched_contracts': matched})
            # Includes explicit empty selection for every successfully inspected block.
            save(args.output / relative, {'schema': 1, 'chain_id': payload['chain_id'],
                 'block_number': number, 'block_hash': payload['header']['hash'],
                 'contracts': sorted(contracts), 'failed': selected})
    print(f'Verified {end-start} blocks, {count} archived failed transactions')


if __name__ == '__main__':
    main()
