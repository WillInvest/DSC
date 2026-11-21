"""Inventory accepted historical market files for immutable migration."""
import argparse
import fcntl
from pathlib import Path

import pyarrow.parquet as pq

from automation.common import paths
from pipeline.compaction.collect import MODE, read
from shared.artifacts import checksum, digest, write_json


def inventory_market(source, job, plan, receipt_path):
    source = Path(source).resolve()
    address = job['address']
    start, stop = job['start_block'], plan['end_block'] + 1
    encoded, decoded, reconstructed = paths(job)
    state = reconstructed / MODE
    receipt = read(receipt_path)
    identity = dict(address=address, start_block=start, end_block=stop-1,
                    end_hash=plan['end_hash'], chunk_size=plan['chunk_size'])
    if receipt['identity'] != identity:
        raise ValueError(f'Historical event receipt identity differs: {address}')

    files = []

    def add(path, kind, expected_bytes=None, accepted_sha=None, expected_rows=None):
        relative = path.relative_to(source)
        if path.resolve() != path or not path.is_file():
            raise ValueError(f'Missing or redirected historical file: {path}')
        size = path.stat().st_size
        if expected_bytes is not None and size != expected_bytes:
            raise ValueError(f'Historical file size differs: {path}')
        if expected_rows is not None and pq.read_metadata(path).num_rows != expected_rows:
            raise ValueError(f'Historical Parquet row count differs: {path}')
        files.append(dict(path=str(relative), kind=kind, bytes=size,
                          sha256=accepted_sha or checksum(path)))

    add(receipt_path, 'extraction_receipt')
    for kind, folder in [('encoded', encoded), ('decoded', decoded), ('state', reconstructed)]:
        metadata = folder / 'metadata.json'
        if metadata.exists():
            add(metadata, kind + '_metadata')
    checkpoint = state / 'progress.json'
    saved = read(checkpoint)
    if (saved['state'] != 'complete' or saved['next_block'] != stop
            or saved['identity']['address'] != address or saved['identity']['mode'] != MODE):
        raise ValueError(f'Historical state checkpoint is incomplete: {address}')
    add(checkpoint, 'state_checkpoint')

    expected = set()
    state_paths = set(state.glob('*.parquet'))
    for lower in range(start, stop, plan['chunk_size']):
        upper = min(stop, lower+plan['chunk_size'])
        name = f'ethereum__logs__{lower:08d}_to_{upper-1:08d}.parquet'
        expected.add(name)
        state_file = state / f'{lower}-{upper-1}.parquet'
        if state_file not in state_paths:
            raise ValueError(f'Missing historical state chunk: {state_file}')
        add(state_file, 'state', expected_rows=upper-lower)
    raw, empty = receipt['files'], set(receipt['empty_chunks'])
    if (set(raw) & empty or set(raw) | empty != expected
            or receipt['total_chunks'] != len(expected) or len(state_paths) != len(expected)):
        raise ValueError(f'Historical coverage is inconsistent: {address}')

    for name, item in sorted(raw.items()):
        add(encoded / name, 'encoded', expected_bytes=item['bytes'], expected_rows=item['rows'])
        decoded_root = decoded / Path(name).stem
        decoded_receipt = decoded_root / 'receipt.json'
        if item['rows'] == 0 and not decoded_receipt.exists():
            continue
        evidence = read(decoded_receipt)
        if evidence['rows'] != item['rows']:
            raise ValueError(f'Historical decoded rows differ: {decoded_receipt}')
        add(decoded_receipt, 'decode_receipt')
        rows = 0
        for table in evidence['tables']:
            table_root = decoded_root / table['directory']
            table_manifest = table_root / 'manifest.json'
            add(table_manifest, 'decode_manifest')
            for item in read(table_manifest)['outputs']:
                add(table_root / item['file'], 'decoded', accepted_sha=item['sha256'],
                    expected_rows=item['rows'])
                rows += item['rows']
        if rows != evidence['rows']:
            raise ValueError(f'Historical decoded payloads are incomplete: {decoded_receipt}')

    return dict(version=1, identity=identity, protocol=job['protocol'],
                state_mode=MODE, empty_event_chunks=sorted(empty), files=files)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    with (args.output / 'inventory.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        inventory_plan(args.source, args.plan, args.output)


def inventory_plan(source, plan_path, output_root):
    plan = read(plan_path)
    seal = digest(plan)
    completed = []
    for job in plan['jobs']:
        address = job['address']
        output = output_root / (address + '.json')
        if output.exists():
            saved = read(output)
            if saved['plan_sha256'] != seal or saved['identity']['address'] != address:
                raise ValueError(f'Historical plan changed: {address}')
        else:
            receipt = plan_path.parent / 'receipts' / (address + '.json')
            saved = inventory_market(source, job, plan, receipt)
            saved['plan_sha256'] = seal
            write_json(output, saved)
        completed.append(dict(address=address, files=len(saved['files']), sha256=checksum(output)))
        write_json(output_root / 'progress.json', dict(plan_sha256=seal, markets=completed,
                   total_markets=len(plan['jobs']), through=plan['end_block']))
        print(f'{len(completed)}/{len(plan["jobs"])} markets, {address}, {len(saved["files"])} files', flush=True)


if __name__ == '__main__':
    main()
