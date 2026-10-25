"""Run one Cryo invocation at a time; retry only missing output with smaller RPC ranges."""
import argparse
import fcntl
from pathlib import Path
import signal
import shlex
import subprocess
import time

import pyarrow.parquet as pq
from web3 import Web3
from common import chunks, read, write


def inventory(folder, expected):
    result = {}
    for path in folder.glob('*.parquet'):
        if path.name not in expected:
            raise ValueError(f'Unexpected output: {path.name}')
        try:
            meta = pq.read_metadata(path)
        except Exception:
            # Interrupted writes must not be trusted by Cryo's file-exists resume.
            quarantine = folder.parent / 'quarantine'
            quarantine.mkdir(exist_ok=True)
            path.rename(quarantine / (path.name + f'.{time.time_ns()}'))
            continue
        result[path.name] = {'bytes': path.stat().st_size, 'rows': meta.num_rows}
    return result


def report_successes(folder, expected):
    result = set()
    for path in folder.glob('*.json'):
        if path.name.startswith('incomplete_'):
            continue
        data = read(path).get('results') or {}
        result.update(Path(p).name for p in data.get('completed_paths', []))
    # A report cannot turn a corrupt, quarantined nonempty file into empty evidence.
    invalid = {p.name.split('.parquet.')[0] + '.parquet'
               for p in (folder.parent / 'quarantine').glob('*.parquet.*')}
    return (result - invalid) & expected.keys()


def run(plan, job):
    root = Path(job['node_dir'])
    root.mkdir(parents=True, exist_ok=True)
    lock = (root / 'lock').open('w')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    global_lock = Path('/home/node/dsc-extraction/sequential-events/extraction.lock').open('w')
    fcntl.flock(global_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    identity = {k: plan[k] for k in ('end_block', 'end_hash', 'chunk_size')}
    identity.update(address=job['address'], start_block=job['start_block'])
    if (root / 'identity.json').exists() and read(root / 'identity.json') != identity:
        raise ValueError('Output directory belongs to a different extraction range')
    write(root / 'identity.json', identity)
    rpc = Web3(Web3.HTTPProvider('http://127.0.0.1:8545', request_kwargs={'timeout': 60}))
    if rpc.eth.chain_id != 1 or rpc.eth.get_block(plan['end_block'])['hash'].hex().removeprefix('0x') != plan['end_hash'].removeprefix('0x'):
        raise ValueError('Chain or captured cutoff hash mismatch')
    output, reports = root / 'encoded', root / 'reports'
    output.mkdir(exist_ok=True)
    reports.mkdir(exist_ok=True)
    expected = chunks(job, plan)
    inventory(output, expected)
    started = time.time()
    terminal_active = False

    def stop(signum, frame):
        if terminal_active:
            subprocess.run(['tmux', 'kill-session', '-t', 'cryo-events'], check=False)
        raise SystemExit(128 + signum)

    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(sig, stop)

    def progress(state, inner, error=None):
        files = {p.name for p in output.glob('*.parquet')} & expected.keys()
        write(root / 'progress.json', {'state': state, 'updated_at': time.time(),
              'started_at': started, 'inner_request_size': inner,
              'written_chunks': len(files), 'total_chunks': len(expected),
              'written_blocks': sum(expected[n] for n in files), 'error': error})

    for inner in (1000, 100, 10, 1):
        files = inventory(output, expected)
        evidence = report_successes(reports, expected)
        if set(files) | evidence == expected.keys():
            break
        report_dir = reports / str(time.time_ns())
        report_dir.mkdir()
        cmd = [plan['cryo'], 'logs', '--rpc', 'http://127.0.0.1:8545',
               '--contract', job['address'], '--blocks', f"{job['start_block']}:{plan['end_block'] + 1}",
               '--chunk-size', str(plan['chunk_size']), '--inner-request-size', str(inner),
               '--max-concurrent-requests', str(plan['concurrency']),
               '--max-concurrent-chunks', str(plan['concurrency']), '--include-columns', 'block_hash',
               '--hex', '--output-dir', str(output), '--report-dir', str(report_dir)]
        exit_file = root / f'exit-{report_dir.name}'
        launch = root / f'launch-{report_dir.name}.sh'
        log_path = root / f'cryo-{report_dir.name}.log'
        launch.write_text('#!/bin/bash\n' + shlex.join(cmd) + '\nresult=$?\n' +
                          'printf "%s\\n" "$result" > ' + shlex.quote(str(exit_file)) + '\nexit "$result"\n')
        exists = subprocess.run(['tmux', 'has-session', '-t', 'cryo-events'], capture_output=True).returncode == 0
        if exists:
            dead = subprocess.check_output(['tmux', 'display-message', '-p', '-t', 'cryo-events:0.0', '#{pane_dead}'], text=True).strip()
            if dead != '1':
                raise RuntimeError('Native monitor session already has a live process')
            subprocess.run(['tmux', 'respawn-pane', '-t', 'cryo-events:0.0', 'bash ' + shlex.quote(str(launch))], check=True)
        else:
            subprocess.run(['tmux', 'new-session', '-d', '-s', 'cryo-events', '-x', '160', '-y', '40',
                            'tmux set-option -w remain-on-exit on; bash ' + shlex.quote(str(launch))], check=True)
        terminal_active = True
        subprocess.run(['tmux', 'pipe-pane', '-t', 'cryo-events:0.0', 'cat >> ' + shlex.quote(str(log_path))], check=True)
        while not exit_file.exists():
            progress('extracting', inner)
            dead = subprocess.check_output(['tmux', 'display-message', '-p', '-t', 'cryo-events:0.0', '#{pane_dead}'], text=True).strip()
            if dead == '1' and not exit_file.exists():
                raise RuntimeError('Cryo terminal exited without a result')
            time.sleep(5)
        terminal_active = False
        # Keep final reports at the root for empty-chunk evidence across retries.
        for report in report_dir.glob('*.json'):
            if not report.name.startswith('incomplete_'):
                report.rename(reports / f'{report_dir.name}-{report.name}')
        progress('checking', inner)
    files = inventory(output, expected)
    evidence = report_successes(reports, expected)
    missing = expected.keys() - (set(files) | evidence)
    if missing:
        progress('failed', inner, f'{len(missing)} chunks still missing; inspect Cryo logs')
        raise RuntimeError(f'{len(missing)} chunks still missing')
    write(root / 'manifest.json', {'identity': identity, 'files': files,
          'empty_chunks': sorted(expected.keys() - files.keys()), 'total_chunks': len(expected)})
    progress('extracted', inner)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['run', 'status'])
    parser.add_argument('plan')
    parser.add_argument('address')
    args = parser.parse_args()
    plan = read(args.plan)
    job = next(j for j in plan['jobs'] if j['address'] == args.address)
    if args.mode == 'status':
        import json
        path = Path(job['node_dir']) / 'progress.json'
        print(json.dumps(read(path) if path.exists() else {}))
    else:
        run(plan, job)
