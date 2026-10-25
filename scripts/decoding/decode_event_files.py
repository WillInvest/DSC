#!/usr/bin/env python3
"""Parallel file adapter around the existing PoolDecoder and BatchWriter."""
import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from contextlib import ExitStack
import fcntl
import json
import os
from pathlib import Path
import re
import sys
from tempfile import TemporaryDirectory
import time

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from decoding.pool_decode import PoolDecoder
from decoding.decode_storage import BatchWriter, checksum, publish, verify_existing
from protocols.protocol_config import load_protocol
from protocols.deployment import load_deployment


def atomic_json(path, data):
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(data, indent=2) + '\n')
    os.replace(tmp, path)


def decode_file(task):
    source, output, protocol, address, profiles = task
    source, output = Path(source), Path(output)
    pa.set_cpu_count(1)
    started = time.monotonic()
    config = {'kind': 'events', 'protocol': protocol, 'address': address,
              'source_file': source.name, 'source_sha256': checksum(source), 'profiles': profiles}
    schema_configs = []
    for profile in profiles:
        cfg = load_protocol(protocol, profile)
        schema_configs.append((profile, cfg, checksum(cfg['abi'])))
    config['abi_sha256'] = [entry[2] for entry in schema_configs]
    if output.exists():
        receipt = json.loads((output / 'receipt.json').read_text())
        if receipt['config'] != config:
            raise ValueError('Existing decoded chunk has different inputs or ABIs')
        for item in receipt['tables']:
            verify_existing(output / item['directory'], item['config'])
        return receipt
    with TemporaryDirectory(prefix='.decode-', dir=output.parent) as temp:
        work = Path(temp)
        tables = []
        writers = []
        total = 0
        match = re.fullmatch(r'ethereum__logs__(\d+)_to_(\d+)\.parquet', source.name)
        lower, upper = map(int, match.groups())
        with ExitStack() as stack:
            for profile, cfg, abi_hash in schema_configs:
                label = f"schema-v{cfg['schema_version']}"
                folder = work / ('tmp-' + label)
                folder.mkdir()
                decoder = PoolDecoder('events', cfg['abi'], address, cfg['state_getters'],
                                      schema_version=cfg['schema_version'] if profile else None,
                                      deployment=profile)
                writers.append((profile, label, folder, decoder,
                                BatchWriter(folder, stack, 'events', protocol)))
            for batch in pq.ParquetFile(source).iter_batches(batch_size=4096):
                bounds = pc.min_max(batch.column('block_number')).as_py()
                if bounds['min'] < lower or bounds['max'] > upper:
                    raise ValueError('Input rows fall outside the named chunk range')
                assigned = 0
                for profile, label, folder, decoder, writer in writers:
                    subset = batch
                    if profile:
                        subset = batch.filter(pc.and_(pc.greater_equal(batch.column('block_number'), profile['from_block']),
                                                     pc.less(batch.column('block_number'), profile['to_block'])))
                    if subset.num_rows:
                        writer.write_batch(subset, decoder, source.name)
                        assigned += subset.num_rows
                if assigned != batch.num_rows:
                    raise ValueError('ABI profiles do not cover each event exactly once')
                total += assigned
        for profile, label, folder, decoder, writer in writers:
            if writer.counts:
                table_config = {**config, 'selected_profile': profile}
                publish(folder, work / label, writer.counts, table_config)
                tables.append({'directory': label, 'config': table_config})
        if total != pq.read_metadata(source).num_rows or checksum(source) != config['source_sha256']:
            raise ValueError('Input changed or decoded row count mismatch')
        receipt = {'config': config, 'tables': tables, 'rows': total,
                   'seconds': time.monotonic() - started}
        (work / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
        os.rename(work, output)
    return receipt


def run(args):
    source, output = args.input.resolve(), args.output.resolve()
    if source == output or source in output.parents or output in source.parents:
        raise ValueError('Encoded and decoded folders must be separate')
    output.mkdir(parents=True, exist_ok=True)
    lock = (output / '.decode.lock').open('w')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    metadata = json.loads((source / 'metadata.json').read_text())
    address = metadata['contract_address'].lower()
    if json.loads((output / 'metadata.json').read_text())['contract_address'].lower() != address:
        raise ValueError('Destination metadata address mismatch')
    cfg = load_protocol(args.protocol)
    profiles = [None]
    if cfg.get('requires_deployment'):
        if args.profiles is None:
            raise ValueError('Historical ABI profiles required')
        profiles = sorted([load_deployment(p, cfg, address) for p in args.profiles.glob(args.protocol + '-*.json')],
                          key=lambda p: p['from_block'])
        if not profiles or any(a['to_block'] != b['from_block'] for a, b in zip(profiles, profiles[1:])):
            raise ValueError('Historical profiles must be contiguous and nonoverlapping')
    files = sorted(source.glob('*.parquet'))
    if args.limit:
        files = files[:args.limit]
    if not files:
        raise ValueError('No input Parquet files')
    jobs = []
    previous_end = -1
    for file in files:
        match = re.fullmatch(r'ethereum__logs__(\d+)_to_(\d+)\.parquet', file.name)
        if match is None:
            raise ValueError(f'Unexpected input filename: {file.name}')
        start, end = map(int, match.groups())
        if start <= previous_end or end < start:
            raise ValueError('Overlapping input block ranges')
        previous_end = end
        relevant = [p for p in profiles if p is None or p['from_block'] <= end and p['to_block'] > start]
        if not relevant:
            raise ValueError(f'No supported ABI for {file.name}')
        jobs.append((str(file), str(output / file.stem), args.protocol, address, relevant))
    total_rows = sum(pq.read_metadata(f).num_rows for f in files)
    started = time.monotonic()
    done, rows = 0, 0

    def status(state, error=None):
        elapsed = time.monotonic() - started
        atomic_json(output / 'decode-progress.json', {'state': state, 'protocol': args.protocol,
                    'workers': args.workers, 'completed_files': done, 'total_files': len(files),
                    'decoded_rows': rows, 'total_rows': total_rows, 'elapsed_seconds': elapsed,
                    'rows_per_second': rows / elapsed if elapsed else 0, 'error': error,
                    'updated_at': time.time()})
    status('running')
    try:
        with ProcessPoolExecutor(max_workers=args.workers) as pool:
            futures = [pool.submit(decode_file, job) for job in jobs]
            for future in as_completed(futures):
                try:
                    result = future.result()
                except Exception as exc:
                    status('failed', str(exc))
                    for pending in futures:
                        pending.cancel()
                    raise
                done += 1
                rows += result['rows']
                status('running')
    except Exception as exc:
        status('failed', str(exc))
        raise
    if rows != total_rows:
        raise ValueError('Final row-count mismatch')
    status('complete')
    print((output / 'decode-progress.json').read_text(), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--protocol', required=True)
    parser.add_argument('--profiles', type=Path)
    parser.add_argument('--workers', type=int, default=16)
    parser.add_argument('--limit', type=int, help='Optional pilot file limit')
    run(parser.parse_args())
