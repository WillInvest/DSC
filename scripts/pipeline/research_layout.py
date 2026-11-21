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
from shared.artifacts import checksum, digest, write_json

PROTOCOLS = {
    'uniswap-v2': 'dex/uniswapV2',
    'uniswap-v3': 'dex/uniswapV3',
    'aave-v2': 'lending/aaveV2',
    'aave-v3': 'lending/aaveV3',
}
MARKET_KINDS = frozenset(('decoded', 'state'))
ENCODED = frozenset(('encoded',))


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
    elif kind == 'encoded':
        directory = root/'metadata/replay-inputs/encoded'/protocol/address
        if schema:
            directory /= schema
    else:
        directory = base/'block-state'/address
    return directory/filename(start, stop, 'parquet')


def publish_historical_market(source, root, manifest, kinds=MARKET_KINDS):
    address = manifest['identity']['address'].lower()
    protocol = manifest['protocol']
    for row in manifest['files']:
        kind = row['kind']
        if kind not in kinds:
            continue
        path = source/row['path']
        interval = path.parents[1].with_suffix('.parquet') if kind == 'decoded' else path
        start, stop = bounds(interval)
        target = market_path(root, protocol, address, kind, start, stop,
                             event=path.stem, schema=path.parent.name if kind == 'decoded' else None)
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
            target.parent.mkdir(parents=True, exist_ok=True)
            with TemporaryDirectory(dir=target.parent) as scratch:
                stage = Path(scratch)/target.name
                pack(stage, filtered_reverts(rows, addresses[protocol]))
                link(stage, target, checksum(stage))


def archive_kind(relative):
    if relative.startswith('receipts/'):
        return 'receipts'
    if '/selected/chunks/' in relative:
        return 'selected'
    if '/archive/' in relative:
        return 'traces'
    raise ValueError(f'Unrecognized historical Revert source: {relative}')


def publish_archive(path, root, kind, start, stop, lower, sha, addresses):
    target = root/'revert'/kind/filename(lower, stop, 'jsonl.gz')
    if lower == start:
        link(path, target, sha)
    elif not target.exists():
        pack(target, records([Fragment(path, start, stop)], lower, stop))
    if kind == 'selected' and addresses is not None:
        publish_selected(path, root, start, stop, lower, addresses)


def publish_tail_markets(catalog, root, cutoff, kinds=MARKET_KINDS):
    with snapshot(catalog) as rows:
        for row in rows:
            kind, *parts = row['stream'].split('/')
            if kind not in kinds or row['stop'] <= cutoff:
                continue
            protocol, address = parts[:2]
            path = Path(row['absolute_path'])
            start = max(row['start'], cutoff)
            stop = row['stop']
            schema = '-'.join(parts[2:4]+parts[-1:]) if kind == 'decoded' else None
            if kind == 'encoded':
                schema = parts[-1]
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


def publish_history(root, historical_markets, status, kinds):
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
        publish_historical_market(source, root, data, kinds)
        write_json(status, dict(stage='historical-markets', markets=index, through=through))
    return addresses, through, index


def publish_encoded(root, historical_markets, market_tail):
    status = root/'metadata'/'encoded-publication.json'
    _, through, markets = publish_history(root, historical_markets, status, ENCODED)
    publish_tail_markets(market_tail, root, through+1, ENCODED)
    write_json(status, dict(stage='complete', markets=markets,
                           market_through=read(market_tail/'manifest.json')['through']))


def publish_revert_history(root, historical_reverts, status, *, addresses=None, markets=None, cutoff=None):
    archive = read(historical_reverts/'historical-revert.json')
    if cutoff is not None and archive['stop'] != cutoff:
        raise ValueError('Historical market and Revert cutoffs differ')
    identity = checksum(historical_reverts/'historical-revert.json')
    progress = {'stage': 'historical-reverted', 'start': archive['start']}
    if markets is not None:
        progress['markets'] = markets
    for start in range(archive['start'], archive['stop'], 1_000_000):
        stop = min(start+1_000_000, archive['stop'])
        source = historical_reverts/f'{start}-{stop-1}'
        group = read(source/'manifest.json')
        if (group['start'], group['stop'], group['parent_inventory_sha256']) != (start, stop, identity):
            raise ValueError('Historical Revert shard has a different identity')
        coverage = defaultdict(list)
        for row in group['files']:
            relative = row['path']
            path = source/relative
            lower, upper = bounds(path)
            kind = archive_kind(relative)
            coverage[kind].append((lower, upper))
            publish_archive(path, root, kind, lower, upper, lower, row['sha256'], addresses)
        if set(coverage) != {'receipts', 'traces', 'selected'}:
            raise ValueError('Historical Revert shard does not contain all three archive families')
        for ranges in coverage.values():
            ranges.sort()
            cursor = start
            for lower, upper in ranges:
                if lower != cursor or upper <= lower:
                    raise ValueError('Historical Revert coverage contains a gap or overlap')
                cursor = upper
            if cursor != stop:
                raise ValueError('Historical Revert coverage ends before or beyond the accepted cutoff')
        write_json(status, dict(progress, through=stop-1))
    return archive


def publish(root, historical_markets, historical_reverts, market_tail, revert_tail):
    status = root/'metadata'/'publication.json'
    addresses, through, index = publish_history(root, historical_markets, status, MARKET_KINDS)
    publish_tail_markets(market_tail, root, through+1)
    publish_revert_history(root, historical_reverts, status,
                           addresses=addresses, markets=index, cutoff=through+1)
    publish_tail_reverts(revert_tail, root, through+1, addresses)
    market_end = read(market_tail/'manifest.json')['through']
    revert_end = read(revert_tail/'manifest.json')['through']
    write_json(status, dict(stage='complete', markets=index, market_through=market_end,
                           revert_through=revert_end))


def publish_protocol_reverts(root, contracts):
    meta = root/'metadata'
    global_archive = read(meta/'global-revert-publication.json')
    if global_archive['stage'] != 'complete':
        raise ValueError('Global Revert publication must finish before splitting it by protocol')
    start, stop = global_archive['start'], global_archive['through']+1
    accepted = meta/'accepted-sources/revert'
    history = read(accepted/'historical-revert.json')
    selection = digest(sorted(row['address'].lower() for row in contracts))
    if history['start'] != start or history['selection'] != selection:
        raise ValueError('Contract registry does not match the accepted Revert selection')
    if history['stop'] < stop:
        tail = read(accepted/f"{history['stop']}-{stop-1}"/'manifest.json')
        streams = {item['stream'] for item in tail['files'] if item['stream'].startswith('selected/')}
        if streams != {'selected/'+selection}:
            raise ValueError('Historical and incremental Revert selections differ')
    addresses = defaultdict(set)
    for row in contracts:
        addresses[row['protocol']].add(row['address'].lower())

    status = meta/'protocol-revert-publication.json'
    cursor = start
    if status.exists():
        saved = read(status)
        if (saved['start'], saved['selection']) != (start, selection) or not start <= saved['through']+1 <= stop:
            raise ValueError('Existing protocol Revert publication has a different identity')
        cursor = saved['through']+1
    parts = sorted((bounds(path), path) for path in (root/'revert/selected').glob('*.jsonl.gz'))
    for count, ((lower, upper), path) in enumerate(parts, 1):
        if upper <= cursor or lower >= stop:
            continue
        if lower != cursor or upper > stop:
            raise ValueError('Global selected archive has a gap or overlap')
        publish_selected(path, root, lower, upper, lower, addresses)
        cursor = upper
        if count % 100 == 0:
            write_json(status, dict(stage='publishing', start=start, through=cursor-1, selection=selection))
    if cursor != stop:
        raise ValueError('Protocol publication does not reach the accepted global endpoint')
    write_json(status, dict(stage='complete', start=start, through=stop-1, selection=selection))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True, type=Path)
    scope = parser.add_mutually_exclusive_group()
    scope.add_argument('--encoded-only', action='store_true')
    scope.add_argument('--global-revert-only', action='store_true')
    scope.add_argument('--protocol-revert-only', action='store_true')
    for field in ('historical-markets', 'historical-reverts', 'market-tail', 'revert-tail',
                  'coordinator', 'registry'):
        parser.add_argument('--'+field, type=Path)
    args = parser.parse_args()
    if not args.root.is_dir():
        parser.error('Create the research root on the data disk first')
    historical = (args.historical_markets, args.historical_reverts, args.market_tail, args.revert_tail)
    lock_name = 'publication.lock'
    if args.encoded_only:
        if not args.historical_markets or not args.market_tail or any(
                (args.historical_reverts, args.revert_tail, args.coordinator, args.registry)):
            parser.error('Raw publication requires historical markets and a market tail')
        sources = args.historical_markets, args.market_tail
        lock_name = 'encoded-publication.lock'
    elif args.global_revert_only:
        if not args.historical_reverts or any(
                (args.historical_markets, args.market_tail, args.revert_tail, args.coordinator, args.registry)):
            parser.error('Global Revert publication requires the historical Revert import')
        sources = (args.historical_reverts,)
        lock_name = 'global-revert-publication.lock'
    elif args.protocol_revert_only:
        if not args.registry or args.coordinator or any(historical):
            parser.error('Protocol Revert publication requires a contract registry')
        sources = ()
        lock_name = 'protocol-revert-publication.lock'
    elif args.coordinator:
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
    with (args.root/lock_name).open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if args.encoded_only:
            publish_encoded(args.root, args.historical_markets, args.market_tail)
        elif args.global_revert_only:
            status = args.root/'metadata/global-revert-publication.json'
            archive = publish_revert_history(args.root, args.historical_reverts, status)
            write_json(status, dict(stage='complete', start=archive['start'], through=archive['stop']-1))
        elif args.protocol_revert_only:
            from registry.catalog import registry as tracked_contracts
            publish_protocol_reverts(args.root, tracked_contracts(args.registry))
        elif args.coordinator:
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
        encoded = read(root/'metadata/encoded-publication.json')
        raw_through = encoded['market_through']
        if encoded['stage'] != 'complete' or not through <= raw_through <= committed['through']:
            raise ValueError('Raw replay input coverage does not join the published market boundary')
        publish_tail_markets(catalog, root, through+1)
        publish_tail_markets(catalog, root, raw_through+1, ENCODED)
        publish_tail_reverts(catalog, root, through+1, addresses)
        write_json(root/'metadata/encoded-publication.json', dict(encoded, market_through=committed['through']))
        write_json(status, dict(previous, market_through=committed['through'], revert_through=committed['through']))


if __name__ == '__main__':
    main()
