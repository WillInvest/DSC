"""Sequential event-only queue on dsc-data. No SQLite or historical header scan."""
import argparse
import csv
import fcntl
import json
from pathlib import Path
import shlex
import subprocess
import time

import pyarrow.parquet as pq
from common import chunks, read, summarize, write

NODE = 'node@10.246.103.160'
NODE_SCRIPT = '/home/node/dsc-extraction/sequential-events/node.py'
NODE_PYTHON = '/home/node/dsc-extraction/.venv-abi/bin/python'
NODE_PLAN = '/home/node/dsc-extraction/sequential-events/plan.json'
SSH = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10',
       '-o', 'ServerAliveInterval=15', '-o', 'ServerAliveCountMax=3', NODE]


def command(mode, address):
    # A run's SSH terminal sends SIGHUP on disconnect, letting node.py stop Cryo.
    prefix = SSH[:-1] + (['-tt'] if mode == 'run' else []) + [NODE]
    return prefix + [shlex.join([NODE_PYTHON, NODE_SCRIPT, mode, NODE_PLAN, address])]


def prepare(registry, data, root):
    if (root / 'plan.json').exists():
        raise ValueError('Plan already exists; resume it instead of replacing it')
    metadata = {}
    for path in data.glob('*/*/*/encoded/*/metadata.json'):
        info = read(path)
        address = info['contract_address'].lower()
        if address in metadata:
            raise ValueError(f'Duplicate destination for {address}')
        metadata[address] = (path.parent, info)
    jobs = []
    with registry.open() as stream:
        for row in csv.DictReader(stream):
            address = row['address'].lower()
            destination, info = metadata[address]
            category, protocol, version = destination.relative_to(data).parts[:3]
            if f'{protocol}-{version}' != row['protocol'] or info['deployment_block'] != int(row['creation_block']):
                raise ValueError(f'Registry/metadata mismatch: {address}')
            node_dir = f'/home/node/dsc-extraction/sequential-events/data/{address}'
            if address == '0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852':
                node_dir = '/home/node/dsc-extraction/manual/cryo-v2-weth-usdt-20261024'
            elif address == '0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2':
                node_dir = '/home/node/dsc-extraction/manual/cryo-aave-v3-20261024'
            jobs.append({'address': address, 'name': row['name'], 'protocol': row['protocol'],
                         'start_block': int(row['creation_block']), 'destination': str(destination),
                         'node_dir': node_dir})
    if len(jobs) != 102 or len({j['address'] for j in jobs}) != 102:
        raise ValueError('Expected exactly 102 distinct registry addresses')
    jobs.sort(key=lambda job: '/manual/' not in job['node_dir'])
    plan = {'end_block': 26266551,
            'end_hash': '0x31a1209f0a733f1da049a77eb6c1d909ec982591c95e922ff8f3a26ad882c92d',
            'chunk_size': 10000, 'concurrency': 32, 'cryo': '/home/node/.cargo/bin/cryo', 'jobs': jobs}
    write(root / 'plan.json', plan)
    write(root / 'progress.json', {**summarize(plan, {}), 'state': 'ready'})


def validate_manifest(manifest, plan, job):
    identity = {'end_block': plan['end_block'], 'end_hash': plan['end_hash'],
                'chunk_size': plan['chunk_size'], 'address': job['address'], 'start_block': job['start_block']}
    expected = chunks(job, plan)
    if manifest['identity'] != identity:
        raise ValueError('Manifest identity mismatch')
    files, empty = set(manifest['files']), set(manifest['empty_chunks'])
    if files & empty or files | empty != expected.keys() or manifest['total_chunks'] != len(expected):
        raise ValueError('Manifest does not cover the requested range exactly')


def validate_destination(manifest, destination):
    if {p.name for p in destination.glob('*.parquet')} != set(manifest['files']):
        raise ValueError('Destination file list mismatch')
    for name, info in manifest['files'].items():
        path = destination / name
        if path.stat().st_size != info['bytes'] or pq.read_metadata(path).num_rows != info['rows']:
            raise ValueError(f'Transferred file mismatch: {name}')


def execute(root):
    lock = (root / 'queue.lock').open('w')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    plan = read(root / 'plan.json')
    state_path = root / 'jobs.json'
    jobs = read(state_path) if state_path.exists() else {}

    def publish(current=None, state='running'):
        write(state_path, jobs)
        write(root / 'progress.json', {**summarize(plan, jobs, current), 'state': state})

    for job in plan['jobs']:
        address = job['address']
        receipt = root / 'receipts' / f'{address}.json'
        if jobs.get(address, {}).get('state') == 'complete':
            try:
                manifest = read(receipt)
                validate_manifest(manifest, plan, job)
                validate_destination(manifest, Path(job['destination']))
                continue
            except (OSError, ValueError, KeyError):
                jobs.pop(address)
        started = time.time()
        current = {**job, 'state': 'extracting', 'started_at': started,
                   'total_chunks': len(chunks(job, plan)), 'written_chunks': 0}
        jobs[address] = {'state': 'running'}
        publish(current)
        try:
            with (root / f'{address}.log').open('a') as log:
                child = subprocess.Popen(command('run', address), stdout=log, stderr=subprocess.STDOUT)
                while child.poll() is None:
                    try:
                        result = subprocess.run(command('status', address), capture_output=True,
                                                text=True, check=True, timeout=20)
                        current.update(json.loads(result.stdout))
                        current['connection_error'] = False
                    except (subprocess.SubprocessError, ValueError):
                        current['connection_error'] = True
                    publish(current)
                    time.sleep(10)
                if child.returncode:
                    raise RuntimeError(f'Node extraction failed (exit {child.returncode}); see {address}.log')
            result = subprocess.run(SSH + [shlex.join(['cat', job['node_dir'] + '/manifest.json'])],
                                    check=True, capture_output=True, text=True, timeout=30)
            manifest = json.loads(result.stdout)
            validate_manifest(manifest, plan, job)
            current['state'] = 'transferring'
            publish(current)
            with (root / f'{address}.log').open('a') as log:
                child = subprocess.Popen(['rsync', '-a', '--partial', '--info=progress2',
                         '-e', 'ssh -o BatchMode=yes -o ConnectTimeout=10',
                         f"{NODE}:{job['node_dir']}/encoded/", job['destination'] + '/'],
                         stdout=log, stderr=subprocess.STDOUT)
                while child.poll() is None:
                    publish(current)
                    time.sleep(10)
                if child.returncode:
                    raise RuntimeError(f'Transfer failed (exit {child.returncode})')
            validate_destination(manifest, Path(job['destination']))
            write(receipt, manifest)
            jobs[address] = {'state': 'complete', 'finished_at': time.time(),
                             'elapsed_seconds': time.time() - started,
                             'rows': sum(f['rows'] for f in manifest['files'].values())}
        except Exception as exc:
            jobs[address] = {'state': 'failed', 'error': str(exc), 'finished_at': time.time()}
        publish()
    publish(state='complete' if all(j['state'] == 'complete' for j in jobs.values()) else 'complete_with_errors')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['prepare', 'run'])
    parser.add_argument('--root', type=Path, default=Path('/home/dsc-data/runs/sequential-events'))
    parser.add_argument('--registry', type=Path, default=Path('/home/dsc-data/contracts/ethereum/pools.csv'))
    parser.add_argument('--data', type=Path, default=Path('/home/dsc-data/data'))
    args = parser.parse_args()
    args.root.mkdir(parents=True, exist_ok=True)
    if args.mode == 'prepare':
        prepare(args.registry, args.data, args.root)
    else:
        execute(args.root)
