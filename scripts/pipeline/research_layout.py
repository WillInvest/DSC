"""Publish accepted Node data in the protocol-oriented research directory."""
import argparse
from collections import defaultdict
import fcntl
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow.parquet as pq

from pipeline.compaction.collect import bounds, read
from pipeline.compaction.jsonl import records
from pipeline.compaction.reader import snapshot
from pipeline.compaction.store import Fragment
from reverted.trace_received import pack
from shared.artifacts import checksum, write_json

PROTOCOLS = {
    'uniswap-v2': 'dex/uniswapV2',
    'uniswap-v3': 'dex/uniswapV3',
    'aave-v2': 'lending/aaveV2',
    'aave-v3': 'lending/aaveV3',
}


def link(source, target, expected_sha, size=None):
    if size is not None and source.stat().st_size != size:
        raise ValueError(f'Accepted file size changed: {source}')
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        target.hardlink_to(source)
    except FileExistsError:
        if not target.samefile(source) and checksum(target) != expected_sha:
            raise ValueError(f'Conflicting research file: {target}')


def filename(start, stop, extension):
    return f'{start}-{stop-1}.{extension}'


def market_path(root, protocol, address, kind, start, stop, event=None, schema=None):
    base = root/PROTOCOLS[protocol]
    if kind == 'decoded':
        directory = base/'events'/address/schema/event
    else:
        directory = base/'block-state'/address
    return directory/filename(start, stop, 'parquet')


def publish_historical_market(source, root, manifest):
    address = manifest['identity']['address'].lower()
    protocol = manifest['protocol']
    for row in manifest['files']:
        kind = row['kind']
        if kind not in ('state', 'decoded'):
            continue
        path = source/row['path']
        interval = path if kind == 'state' else path.parents[1].with_suffix('.parquet')
        start, stop = bounds(interval)
        target = market_path(root, protocol, address, kind, start, stop,
                             event=path.stem, schema=path.parent.name)
        link(path, target, row['sha256'], row['bytes'])


def filtered_reverts(rows, addresses):
    for row in rows:
        entries = []
        for entry in row['failed']:
            matched = [address for address in entry['matched_contracts'] if address.lower() in addresses]
            if matched:
                entries.append(dict(entry, matched_contracts=matched))
        yield dict(row, failed=entries)


def publish_selected(path, root, start, stop, lower, addresses):
    name = filename(lower, stop, 'jsonl.gz')
    targets = {protocol: root/folder/'reverted-tx'/name for protocol, folder in PROTOCOLS.items()}
    if all(target.is_file() for target in targets.values()):
        return
    rows = list(records([Fragment(path, start, stop)], lower, stop))
    for protocol, target in targets.items():
        if not target.exists():
            pack(target, filtered_reverts(rows, addresses[protocol]))


def archive_kind(relative):
    if relative.startswith('receipts/'):
        return 'receipts'
    if '/selected/chunks/' in relative:
        return 'selected'
    return 'traces'


def publish_archive(path, root, kind, start, stop, lower, sha, addresses):
    target = root/'revert'/kind/filename(lower, stop, 'jsonl.gz')
    if lower == start:
        link(path, target, sha)
    elif not target.exists():
        pack(target, records([Fragment(path, start, stop)], lower, stop))
    if kind == 'selected':
        publish_selected(path, root, start, stop, lower, addresses)


def publish_tail_markets(catalog, root, cutoff):
    with snapshot(catalog) as rows:
        for row in rows:
            kind, *parts = row['stream'].split('/')
            if kind not in ('decoded', 'state') or row['stop'] <= cutoff:
                continue
            protocol, address = parts[:2]
            path = Path(row['absolute_path'])
            start = max(row['start'], cutoff)
            stop = row['stop']
            schema = '-'.join(parts[2:4]+parts[-1:]) if kind == 'decoded' else None
            event = parts[-2] if kind == 'decoded' else None
            target = market_path(root, protocol, address.lower(), kind, start, stop, event, schema)
            if start == row['start']:
                link(path, target, row['sha256'])
            elif not target.exists():
                target.parent.mkdir(parents=True, exist_ok=True)
                with TemporaryDirectory(dir=target.parent) as scratch:
                    staged = Path(scratch)/target.name
                    pq.write_table(pq.read_table(path, filters=[('block_number', '>=', start)]),
                                   staged, compression='zstd')
                    staged.replace(target)


def publish_tail_reverts(catalog, root, cutoff, addresses):
    with snapshot(catalog) as rows:
        for row in rows:
            kind = row['stream'].split('/')[0]
            if kind in ('receipts', 'traces', 'selected') and row['stop'] > cutoff:
                publish_archive(Path(row['absolute_path']), root, kind, row['start'], row['stop'],
                                max(row['start'], cutoff), row['sha256'], addresses)


def publish(root, historical_markets, historical_reverts, market_tail, revert_tail):
    status = root/'metadata'/'publication.json'
    history = read(historical_markets/'progress.json')
    if history['state'] != 'complete':
        raise ValueError('Historical market import is incomplete')
    addresses = defaultdict(set)
    through = history['through']
    for index, market in enumerate(history['published'], 1):
        source = historical_markets/market['address']
        manifest_path = source/'manifest.json'
        if checksum(manifest_path) != market['sha256']:
            raise ValueError(f'Historical market manifest changed: {manifest_path}')
        data = read(manifest_path)
        addresses[data['protocol']].add(data['identity']['address'].lower())
        publish_historical_market(source, root, data)
        write_json(status, dict(stage='historical-markets', markets=index, through=through))
    publish_tail_markets(market_tail, root, through+1)

    archive = read(historical_reverts/'historical-revert.json')
    if archive['stop'] != through+1:
        raise ValueError('Historical market and Revert cutoffs differ')
    identity = checksum(historical_reverts/'historical-revert.json')
    for start in range(archive['start'], archive['stop'], 1_000_000):
        stop = min(start+1_000_000, archive['stop'])
        source = historical_reverts/f'{start}-{stop-1}'
        group = read(source/'manifest.json')
        if (group['start'], group['stop'], group['parent_inventory_sha256']) != (start, stop, identity):
            raise ValueError('Historical Revert shard has a different identity')
        for row in group['files']:
            relative = row['path']
            path = source/relative
            lower, upper = bounds(path)
            publish_archive(path, root, archive_kind(relative), lower, upper, lower,
                            row['sha256'], addresses)
        write_json(status, dict(stage='historical-reverted', markets=index, through=stop-1))
    publish_tail_reverts(revert_tail, root, through+1, addresses)
    market_end = read(market_tail/'manifest.json')['through']
    revert_end = read(revert_tail/'manifest.json')['through']
    write_json(status, dict(stage='complete', markets=index, market_through=market_end,
                           revert_through=revert_end))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True, type=Path)
    for field in ('historical-markets', 'historical-reverts', 'market-tail', 'revert-tail',
                  'coordinator', 'registry'):
        parser.add_argument('--'+field, type=Path)
    args = parser.parse_args()
    if not args.root.is_dir():
        parser.error('Create the research root on the data disk first')
    historical = (args.historical_markets, args.historical_reverts, args.market_tail, args.revert_tail)
    if args.coordinator:
        if not args.registry or any(historical):
            parser.error('Use a coordinator and registry, or the four historical import paths')
        sources = (args.coordinator,)
    else:
        if not all(historical) or args.registry:
            parser.error('Provide the four historical import paths')
        sources = historical
    for source in sources:
        if source.stat().st_dev != args.root.stat().st_dev:
            parser.error('Research publication and accepted imports must share the data disk')
    with (args.root/'publication.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if args.coordinator:
            publish_committed(args.root, args.coordinator, args.registry)
        else:
            publish(args.root, *historical)


def publish_committed(root, coordinator, registry):
    from registry.catalog import registry as tracked_contracts

    status = root/'metadata/publication.json'
    previous = read(status)
    if previous['stage'] != 'complete' or previous['market_through'] != previous['revert_through']:
        raise ValueError('The initial market and Revert publication must have a common accepted boundary')
    through = previous['market_through']
    with (coordinator/'coordinator.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        baseline = read(coordinator/'baseline.json')
        catalog = coordinator/'canonical'
        if baseline['end_block'] > through:
            raise ValueError('Published data has a gap before the new coordinator baseline')
        if not (coordinator/'compaction.json').is_file():
            return
        committed = read(coordinator/'compaction.json')
        if committed['state'] != 'complete' or committed['through'] != read(catalog/'progress.json')['through']:
            raise ValueError('The Node catalog and coordinator watermarks disagree')
        if committed['through'] <= through:
            return
        addresses = defaultdict(set)
        for contract in tracked_contracts(registry):
            addresses[contract['protocol']].add(contract['address'])
        publish_tail_markets(catalog, root, through+1)
        publish_tail_reverts(catalog, root, through+1, addresses)
        write_json(status, dict(previous, market_through=committed['through'], revert_through=committed['through']))


if __name__ == '__main__':
    main()
