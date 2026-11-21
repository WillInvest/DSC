"""Fingerprint one finalized block independently of each catalog's partitions."""
import argparse
from collections import defaultdict
import json
from pathlib import Path

from pipeline.compaction.reader import snapshot
from pipeline.compaction.store import Fragment
from shared.artifacts import checksum, digest

MARKET = {'encoded', 'decoded', 'state'}
ARCHIVE = {'receipts', 'traces', 'selected'}


def archive_value(kind, value):
    if kind == 'traces':
        return dict(receipt_summary=value['receipt_summary'],
                    failed=[dict(receipt=item['receipt'], trace=item['trace']) for item in value['failed']])
    if kind == 'selected':
        return dict(block_number=value['block_number'], block_hash=value['block_hash'],
                    failed=[dict(transaction=item['receipt']['transactionHash'],
                                 contracts=item['matched_contracts']) for item in value['failed']])
    return value


def probe(root, block):
    from pipeline.compaction.jsonl import records

    markets = {kind: defaultdict(list) for kind in sorted(MARKET)}
    archives = {}
    checked = 0
    with snapshot(root) as rows:
        for row in (item for item in rows if item['start'] <= block < item['stop']):
            kind = row['stream'].split('/')[0]
            if kind not in MARKET | ARCHIVE:
                continue
            source = Path(row['absolute_path'])
            if checksum(source) != row['sha256']:
                raise ValueError('Accepted catalog part changed')
            if kind in MARKET:
                from pipeline.verify import table_rows
                address = row['stream'].split('/')[2]
                for item in table_rows(source, block, block+1):
                    values = {key: value for key, value in item.items() if key != 'source_file'}
                    markets[kind][address].append(digest(values))
            else:
                if kind in archives:
                    raise ValueError('Duplicate archive stream at the probe block')
                value, = records([Fragment(source, row['start'], row['stop'])], block, block+1)
                archives[kind] = digest(archive_value(kind, value))
            checked += 1
    return dict(block=block, markets={kind: {address: sorted(values) for address, values in m.items()}
                                    for kind, m in markets.items()}, archives=archives, checked_files=checked)


def compare(node, data, revert, count=102):
    if node['block'] != data['block'] or node['block'] != revert['block']:
        raise ValueError('Node and legacy fingerprints refer to different blocks')
    if (node['markets'] != data['markets'] or len(node['markets']['state']) != count
            or any(len(rows) != 1 for rows in node['markets']['state'].values())):
        raise ValueError('Shadow market rows differ from the legacy catalog')
    if (set(node['archives']) != ARCHIVE or set(revert['archives']) != ARCHIVE
            or node['archives'] != revert['archives']
            or node['archives']['selected'] != data['archives']['selected']):
        raise ValueError('Shadow receipts, traces or selection differ from the legacy catalog')
    return dict(state='passed', block=node['block'], state_addresses=count,
                markets=sorted(MARKET), archives=sorted(ARCHIVE))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    read = commands.add_parser('probe')
    read.add_argument('catalog', type=Path)
    read.add_argument('block', type=int)
    check = commands.add_parser('compare')
    for role in ('node', 'data', 'revert'):
        check.add_argument(role, type=Path)
    args = parser.parse_args()
    result = probe(args.catalog, args.block) if args.command == 'probe' else compare(
        *(json.loads(getattr(args, role).read_text()) for role in ('node', 'data', 'revert')))
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
