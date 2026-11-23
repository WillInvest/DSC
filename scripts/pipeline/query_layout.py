"""Publish direct, flat query files from accepted Node research artifacts."""

import argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import csv
import fcntl
import gzip
import hashlib
from itertools import islice
import json
import os
from pathlib import Path
import re
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from pipeline.compaction.collect import bounds, read
from pipeline.research_layout import PROTOCOLS
from shared.artifacts import checksum, write_json


FORMAT = 'dsc-query-layout-v1'
NAMING = 'range-first-v1'
HF = 'lending/aaveV3/health-factor'
QUERY_HF = 'lending/health-factor'
SPAN = re.compile(r'([0-9]+)-([0-9]+)')
MARKET_PRODUCTS = {'events': '*/*/*/*.parquet', 'block-state': '*/*.parquet',
                   'reverted-tx': '*.jsonl.gz'}


def interval(name):
    match = SPAN.fullmatch(name)
    if match is None:
        raise ValueError(f'Invalid query interval: {name}')
    start, last = map(int, match.groups())
    if start > last:
        raise ValueError(f'Empty query interval: {name}')
    return start, last + 1


def link(source, target):
    try:
        target.hardlink_to(source)
    except FileExistsError:
        if not target.samefile(source):
            raise ValueError(f'Conflicting published file: {target}')


def market_name(product, protocol, parts):
    *identity, partition = parts
    suffix = '.jsonl.gz' if product == 'reverted-tx' else '.parquet'
    span = partition.removesuffix(suffix)
    interval(span)
    if product == 'events':
        address, schema, event = identity
        identity = [address, event, schema]
    return '__'.join((span, protocol, *identity)) + suffix


def wallet_name(path, window):
    metadata = pq.read_metadata(path).metadata
    start, stop, first, end = (int(metadata[key]) for key in
                              (b'chunk_start', b'chunk_stop', b'wallet_start', b'wallet_stop'))
    lower, upper = interval(window)
    if not lower <= start < stop <= upper or not 0 <= first < end:
        raise ValueError(f'Invalid wallet bounds for {path}')
    return f'{start}-{stop-1}__wallet-ids-{first}-{end-1}.parquet'


def markets(root, lower, upper, range_first=False):
    counts = defaultdict(int)
    for protocol, folder in PROTOCOLS.items():
        for product, pattern in MARKET_PRODUCTS.items():
            source = root/folder/product
            target = root/Path(folder).parent/product
            for path in source.glob(pattern):
                _, stop = bounds(path)
                if not lower < stop <= upper:
                    continue
                parts = path.relative_to(source).parts
                name = market_name(product, protocol, parts) if range_first else '__'.join((protocol, *parts))
                link(path, target/name)
                counts[str(target.relative_to(root))] += 1
    return dict(counts)


class Archive:
    def __init__(self, root, receipt):
        self.root = root
        path = root/'pack-index.json'
        if checksum(path) != receipt['index_sha256']:
            raise ValueError(f'Historical HF index changed: {path}')
        self.index = read(path)
        if self.index['all_member_hashes_exact'] is not True:
            raise ValueError(f'Historical HF index lacks verified members: {path}')

    def data(self, name):
        entry = self.index['members'][name]
        relative = Path(entry['pack'])
        if relative.parent != Path('packs') or relative.suffix != '.tar':
            raise ValueError('Invalid HF package path')
        with (self.root/relative).open('rb') as stream:
            stream.seek(entry['offset'])
            data = stream.read(entry['bytes'])
        if len(data) != entry['bytes'] or hashlib.sha256(data).hexdigest() != entry['sha256']:
            raise ValueError(f'Historical HF member differs: {name}')
        return data


def publish_member(archive, name, output, scratch):
    entry = archive.index['members'][name]
    if output.exists():
        if checksum(output) != entry['sha256']:
            raise ValueError(f'Existing query member differs: {output}')
        return
    with TemporaryDirectory(dir=scratch) as stage:
        staged = Path(stage)/'member'
        staged.write_bytes(archive.data(name))
        staged.rename(output)


def restored_inventory(path):
    if path is None or not path.is_file():
        return {}
    with gzip.open(path, 'rt') as stream:
        return {row[0]: row[1:] for line in stream if (row := json.loads(line))}


def restore_wallet(source, inventory, archive, key, target, scratch):
    record = inventory.get(key)
    if record:
        sha, device, inode, size, mtime, ctime, *_ = record
        candidate = source/key
        if candidate.is_file():
            info = candidate.stat()
            accepted = archive.index['members'][key]
            same_member = (sha, size) == (accepted['sha256'], accepted['bytes'])
            same_inode = (device, inode, size, mtime) == (
                info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)
            if same_member and same_inode and (ctime == info.st_ctime_ns or
                    (target.exists() and target.samefile(candidate))):
                link(candidate, target)
                return
    publish_member(archive, key, target, scratch)


def historical_wallets(root, scratch, restored, inventories):
    source = root/HF/'historical'
    marker = read(source/'catalog/wallet-coverage.json')
    assigned = defaultdict(dict)
    with (source/'catalog/CANONICAL_JOBS.csv').open(newline='') as stream:
        rows = list(csv.DictReader(stream))
    end = marker['start']
    for row in rows:
        start, stop = interval(row['job_id'])
        if (start, stop) != (end, int(row['stop_block_exclusive'])) or start != int(row['start_block_inclusive']):
            raise ValueError('Historical wallet job coverage has a gap or overlap')
        assigned[row['canonical_cloud_batch']][row['job_id']] = int(row['logical_wallet_block_values'])
        end = stop
    if len(rows) != marker['accepted_jobs'] or end != marker['through']+1:
        raise ValueError('Historical HF wallet catalog is incomplete')

    target = root/QUERY_HF/'wallets'
    dictionaries = scratch/'dictionaries'
    dictionaries.mkdir(exist_ok=True)
    count = 0
    for batch, jobs in assigned.items():
        print(f'Publishing wallet batch {batch} ({len(jobs)} ranges)', flush=True)
        folder = source/'wallets'/batch
        receipt_path = folder/'local-accepted.json'
        receipt = read(receipt_path if receipt_path.is_file() else folder/'accepted.json')
        archive = Archive(folder, receipt)
        inventory = restored_inventory(inventories/f'{batch}.jsonl.gz' if inventories else None)
        seen, dictionary_keys = set(), []
        parts = 0
        for name in archive.index['members']:
            member = Path(name)
            if member.name == 'users.parquet':
                dictionary_keys.append(name)
            if member.parent.parent.name != 'jobs' or member.suffix != '.parquet':
                continue
            job = member.parent.name
            if job not in jobs:
                raise ValueError(f'Wallet data outside its accepted batch: {name}')
            if member.name.startswith('parts-'):
                restore_wallet(restored/batch if restored else folder, inventory, archive, name,
                               target/f'{job}__{member.name}', scratch)
                seen.add(job)
                parts += 1
        if any(job not in seen for job, values in jobs.items() if values):
            raise ValueError(f'Historical wallet batch has missing Parquet: {batch}')
        if len(dictionary_keys) != 1 or parts != receipt['wallet_parts']:
            raise ValueError(f'Historical wallet inventory differs: {batch}')
        publish_member(archive, dictionary_keys[0], dictionaries/f'{batch}.parquet', scratch)
        count += parts
        del archive, inventory
    return dict(start=marker['start'], through=marker['through'], files=count,
                jobs=len(rows), dictionary_catalog='historical/catalog/CANONICAL_JOBS.csv')


def split_aggregate(table, reserves, protocol, scratch, start, stop):
    identifier = int(table.schema.metadata[b'protocol_scope_id'])
    matches = pc.equal(table['scope_id'], identifier)
    protocol_rows = table.filter(matches)
    reserve_rows = table.filter(pc.invert(matches))
    if len(protocol_rows) != 3*(stop-start) or len(reserve_rows)+len(protocol_rows) != len(table):
        raise ValueError(f'Unexpected cohort coverage for {start}-{stop-1}')
    with TemporaryDirectory(dir=scratch) as temp:
        for path, data in ((reserves, reserve_rows), (protocol, protocol_rows)):
            staged = Path(temp)/path.name
            pq.write_table(data, staged, compression='zstd')
            staged.rename(path)


def aggregate(root, table_reader, name, scratch, range_first=False):
    start, stop = interval(name.removeprefix('aggregates-').removesuffix('.parquet'))
    if range_first:
        name = f'{start}-{stop-1}.parquet'
    outputs = [root/QUERY_HF/scope/name for scope in ('reserves', 'protocol')]
    if all(p.is_file() for p in outputs):
        if pq.ParquetFile(outputs[1]).metadata.num_rows != 3*(stop-start):
            raise ValueError(f'Existing protocol aggregate differs: {name}')
    else:
        split_aggregate(table_reader(), *outputs, scratch, start, stop)
    return start, stop


def contiguous(spans, start, stop):
    for first, end in sorted(spans):
        if first != start or end <= first:
            raise ValueError(f'Expected contiguous coverage at {start}, found {first}-{end-1}')
        start = end
    if start != stop:
        raise ValueError(f'Coverage stops at {start-1}, expected {stop-1}')


def historical_cohorts(root, scratch, workers):
    branches = list((root/HF/'historical/cohorts').glob('*/accepted.json'))
    if len(branches) != 1:
        raise ValueError('Expected one accepted historical HF cohort archive')
    receipt = read(branches[0])
    archive = Archive(branches[0].parent, receipt)
    manifest = archive.root/'manifest.json'
    if checksum(manifest) != receipt['manifest_sha256']:
        raise ValueError('Historical cohort manifest changed')
    scope = read(manifest)['scope']
    files = [name for name in archive.index['members'] if Path(name).name.startswith('aggregates-')
             and name.endswith('.parquet')]
    print(f'Publishing {len(files)} historical aggregate tiles', flush=True)

    def process(key):
        return aggregate(root, lambda: pq.ParquetFile(pa.BufferReader(archive.data(key))).read(),
                         Path(key).name, scratch)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        contiguous(list(pool.map(process, files)), scope['start'], scope['stop'])
    publish_member(archive, 'metadata/reserves.json', scratch/'historical-scopes.json', scratch)
    return dict(start=scope['start'], through=scope['stop']-1, files_per_product=len(files))


def incremental_hf(root, scratch, first, through, workers, range_first):
    source = root/HF/'local-accepted'
    accepted = read(root/HF/'compute/progress.json')['next_block']
    start = through+1
    spans, counts = [], defaultdict(int)
    names = (p for p in source.iterdir() if SPAN.fullmatch(p.name))
    for path in sorted(names, key=lambda p: interval(p.name)):
        lower, stop = interval(path.name)
        if stop <= start or stop > accepted:
            continue
        if lower < first or lower != through+1:
            raise ValueError(f'Incremental HF does not join the query dataset: {path}')
        proof = read(path/'local-placement.json')
        result = path/'result/result.json'
        if not proof['accepted_locally'] or checksum(result) != proof['result_sha256']:
            raise ValueError(f'Incremental query source lacks Node acceptance: {path}')
        data = read(result)
        if (data['scope']['start'], data['scope']['stop']) != (lower, stop):
            raise ValueError(f'Incremental query source range differs: {path}')
        link(result.with_name('wallets.json'), scratch/'dictionaries'/f'{path.name}.json')
        write_json(scratch/'dictionaries'/f'{path.name}.scopes.json', data['aggregates']['reserves'])
        for part in data['wallet_parts']:
            name = Path(part['path']).name
            source_file = result.with_name(name)
            published = wallet_name(source_file, path.name) if range_first else f'{path.name}__{name}'
            link(source_file, root/QUERY_HF/'wallets'/published)
            counts['wallets'] += 1
        def split(part):
            name = Path(part['path']).name
            return aggregate(root, lambda: pq.ParquetFile(result.with_name(name)).read(use_threads=False),
                             name, scratch, range_first)

        with ThreadPoolExecutor(max_workers=workers) as pool:
            agg_spans = list(pool.map(split, data['aggregates']['parts']))
        counts['aggregate_tiles'] += len(agg_spans)
        contiguous(agg_spans, lower, stop)
        spans.append((lower, stop))
        through = stop-1
    contiguous(spans, start, accepted)
    return through, dict(counts)


def publish(root, restored, inventories, workers, refresh_only=False):
    scratch = root/'metadata/query-layout'
    scratch.mkdir(parents=True, exist_ok=True)
    target = root/'metadata/query-publication.json'
    if refresh_only and (not target.is_file() or read(target)['state'] != 'complete'):
        return
    with (scratch/'publish.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | (fcntl.LOCK_NB if refresh_only else 0))
        except BlockingIOError:
            return
        state = read(target) if target.is_file() else dict(format=FORMAT, market_through=0)
        if state['format'] != FORMAT:
            raise ValueError('Unknown query publication format')
        if state.get('state') == 'renaming':
            raise ValueError('Finish the naming cutover before publishing more data')
        range_first = state.get('naming') == NAMING
        for base in ('dex', 'lending'):
            for product in ('events', 'block-state', 'reverted-tx'):
                (root/base/product).mkdir(exist_ok=True)
        for product in ('wallets', 'reserves', 'protocol'):
            (root/QUERY_HF/product).mkdir(parents=True, exist_ok=True)
        market = read(root/'metadata/publication.json')
        selected = read(root/'metadata/protocol-revert-publication.json')
        if market['stage'] != 'complete' or selected['stage'] != 'complete':
            raise ValueError('Query trees require accepted market and Revert publication')
        through = min(market['market_through'], market['revert_through'], selected['through'])
        if through > state['market_through']:
            print(f'Publishing market and Revert files through {through}', flush=True)
            counts = state.setdefault('market_files', {})
            for key, n in markets(root, state['market_through']+1, through+1, range_first).items():
                counts[key] = counts.get(key, 0)+n
            state['market_through'] = through
            if state.get('state') != 'complete':
                write_json(target, dict(state, state='publishing'))
        if 'historical_wallets' not in state:
            state['historical_wallets'] = historical_wallets(root, scratch, restored, inventories)
            write_json(target, dict(state, state='publishing'))
        if 'historical_cohorts' not in state:
            state['historical_cohorts'] = historical_cohorts(root, scratch, workers)
            write_json(target, dict(state, state='publishing'))
        last = state['historical_wallets']['through']
        if last != state['historical_cohorts']['through']:
            raise ValueError('Historical HF families end at different blocks')
        state['hf_through'], counts = incremental_hf(
            root, scratch, last+1, state.get('hf_through', last), workers, range_first)
        total = state.setdefault('incremental_hf_files', {})
        for key, count in counts.items():
            total[key] = total.get(key, 0)+count
        state['state'] = 'complete'
        write_json(target, state)
        print(json.dumps(state, sort_keys=True), flush=True)


def renamed_file(leaf, source):
    product = leaf.name
    name = source.name
    if product == 'wallets':
        window, separator, part = name.partition('__')
        if not separator or not part.startswith(('parts-', 'wallets-')) or source.suffix != '.parquet':
            raise ValueError(f'Unexpected legacy wallet filename: {source}')
        return wallet_name(source, window)
    if product in ('reserves', 'protocol'):
        stem = name.removeprefix('aggregates-')
        if stem == name or source.suffix != '.parquet':
            raise ValueError(f'Unexpected legacy aggregate filename: {source}')
        interval(stem.removesuffix('.parquet'))
        return stem
    protocol, *parts = name.split('__')
    if protocol not in PROTOCOLS:
        raise ValueError(f'Unexpected legacy market filename: {source}')
    return market_name(product, protocol, parts)


def old_partition_end(leaf, name):
    if leaf.name == 'wallets':
        return interval(name.split('__', 1)[0])[1]-1
    if leaf.name in ('reserves', 'protocol'):
        return interval(name.removeprefix('aggregates-').removesuffix('.parquet'))[1]-1
    return bounds(Path(name.rsplit('__', 1)[-1]))[1]-1


def file_counts(state):
    hf_tiles = (state['historical_cohorts']['files_per_product'] +
                state['incremental_hf_files']['aggregate_tiles'])
    return dict(state['market_files'], **{
        f'{QUERY_HF}/wallets': (state['historical_wallets']['files'] +
                                state['incremental_hf_files']['wallets']),
        f'{QUERY_HF}/reserves': hf_tiles,
        f'{QUERY_HF}/protocol': hf_tiles,
    })


def stage_names(root, stage, state, previous, workers):
    wanted = file_counts(state)
    existing = file_counts(previous) if previous else {}
    for relative, total in wanted.items():
        delta = total-existing.get(relative, 0)
        if previous and not delta:
            continue
        leaf = Path(relative)
        target = stage/leaf
        target.mkdir(parents=True, exist_ok=True)
        watermark = 'hf_through' if relative.startswith(QUERY_HF) else 'market_through'
        lower, upper = (previous or {}).get(watermark, 0), state[watermark]
        with os.scandir(target) as names:
            staged_inodes = {entry.inode() for entry in names}

        def publish_one(entry):
            if entry.inode() in staged_inodes:
                return
            if not entry.is_file(follow_symlinks=False):
                raise ValueError(f'Unexpected query directory entry: {entry.path}')
            source = Path(entry.path)
            link(source, target/renamed_file(leaf, source))

        count = 0
        with os.scandir(root/leaf) as entries, ThreadPoolExecutor(max_workers=workers) as pool:
            pending = (entry for entry in entries if lower < old_partition_end(leaf, entry.name) <= upper)
            if leaf.name == 'wallets':
                pending = iter(sorted(pending, key=lambda entry: entry.inode()))
            while batch := list(islice(pending, 2048)):
                for _ in pool.map(publish_one, batch):
                    count += 1
                if count % 65536 == 0:
                    print(f'Staged {relative}: {count} files', flush=True)
        with os.scandir(target) as names:
            staged_count = sum(1 for _ in names)
        if count != delta or staged_count != total:
            raise ValueError(f'Query count mismatch for {relative}: {count}, {staged_count}, expected {delta}, {total}')
        print(f'Verified {relative}: {total} filenames and file identities', flush=True)


def rename_query_files(root, workers):
    metadata = root/'metadata'
    scratch = metadata/'query-layout'
    path = metadata/'query-publication.json'
    migration = scratch/'range-first-v1'
    staged, rollback = migration/'staged', migration/'previous'
    migration.mkdir(exist_ok=True)
    with (migration/'stage.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        state = read(path)
        if state.get('naming') == NAMING:
            return
        if state['format'] != FORMAT or state['state'] not in ('complete', 'renaming'):
            raise ValueError('Query publication is not ready for renaming')
        if state['state'] == 'complete':
            stage_names(root, staged, state, None, workers)
        with (scratch/'publish.lock').open('a') as publication:
            fcntl.flock(publication, fcntl.LOCK_EX)
            current = read(path)
            if current['state'] == 'complete':
                if current != state:
                    stage_names(root, staged, current, state, workers)
                write_json(path, dict(current, state='renaming'))

            expected = file_counts(current)
            for relative in expected:
                live, old, new = root/relative, rollback/relative, staged/relative
                if not old.exists():
                    old.parent.mkdir(parents=True, exist_ok=True)
                    live.rename(old)
                if not live.exists():
                    new.rename(live)
                if new.exists() or not live.is_dir() or not old.is_dir():
                    raise ValueError(f'Incomplete filename cutover for {relative}')
            write_json(path, dict(current, state='complete', naming=NAMING))
            print(f'Activated {NAMING} for {sum(expected.values()):,} files', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--restored-wallets', type=Path)
    parser.add_argument('--wallet-inventories', type=Path)
    parser.add_argument('--workers', type=int, default=4)
    parser.add_argument('--refresh-only', action='store_true')
    parser.add_argument('--rename-query-files', action='store_true')
    args = parser.parse_args()
    if args.workers < 1:
        parser.error('--workers must be positive')
    if args.rename_query_files:
        rename_query_files(args.root, args.workers)
    else:
        publish(args.root, args.restored_wallets, args.wallet_inventories, args.workers, args.refresh_only)


if __name__ == '__main__':
    main()
