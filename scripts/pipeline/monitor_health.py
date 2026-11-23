"""Read Node's published research coverage and operational signals."""
from concurrent.futures import ThreadPoolExecutor
from collections import Counter
import csv
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import time


WORKERS = ('CU1', 'CU2', 'CU3')
STAGES = ('events', 'state', 'revert', 'hf')
HELPER_NAMES = {'dsc-hf-compute': 'CU1', 'dsc-hf-data': 'CU2'}


def document(path):
    try:
        value = json.loads(path.read_text())
        return (value, int(path.stat().st_mtime)) if isinstance(value, dict) else ({}, None)
    except (OSError, ValueError):
        return {}, None


def chain(previous):
    from requests import RequestException
    from web3 import Web3
    from web3.exceptions import Web3Exception

    rpc = Web3(Web3.HTTPProvider('http://127.0.0.1:8545', request_kwargs={'timeout': 3}))
    try:
        latest = rpc.eth.get_block('latest')
        finalized = rpc.eth.get_block('finalized')
    except (RequestException, Web3Exception, ValueError, OSError):
        return {**previous, 'availability': 'unreachable'}
    return dict(availability='available', latest=latest.number, finalized=finalized.number,
                latest_time=latest.timestamp, finalized_time=finalized.timestamp,
                last_success=int(time.time()), unfinalized=max(0, latest.number-finalized.number))


def schedule(previous):
    def status(unit, keys):
        command = ['systemctl', 'show', unit, *(f'--property={key}' for key in keys)]
        result = subprocess.check_output(command, env={**os.environ, 'TZ': 'UTC', 'LC_ALL': 'C'},
                                         text=True, timeout=4, stderr=subprocess.DEVNULL)
        return dict(line.split('=', 1) for line in result.splitlines() if '=' in line)

    def timestamp(value):
        try:
            return int(datetime.strptime(value, '%a %Y-%m-%d %H:%M:%S %Z')
                       .replace(tzinfo=timezone.utc).timestamp())
        except ValueError:
            return None

    try:
        unit = status('dsc-node-research-pass.service', ('ActiveState', 'Result', 'ExecMainStatus',
                      'ExecMainStartTimestamp', 'ExecMainExitTimestamp'))
        timer = status('dsc-node-research-pass.timer', ('ActiveState', 'UnitFileState', 'NextElapseUSecRealtime'))
    except (OSError, subprocess.SubprocessError):
        return dict(availability='unreachable')
    started = timestamp(unit.get('ExecMainStartTimestamp', ''))
    exited = timestamp(unit.get('ExecMainExitTimestamp', ''))
    running = unit.get('ActiveState') in {'active', 'activating'}
    succeeded = (not running and unit.get('Result') == 'success' and unit.get('ExecMainStatus') == '0')
    last_success = exited if succeeded and exited else previous.get('last_success')
    return dict(availability='available', state='running' if running else unit.get('ActiveState'),
                result=unit.get('Result'), timer=timer.get('UnitFileState'),
                last_start=started, last_success=last_success,
                next_run=timestamp(timer.get('NextElapseUSecRealtime', '')),
                elapsed=int(time.time())-started if running and started else None)


def probe_worker(address):
    try:
        with socket.create_connection((address, 22), timeout=2):
            return 'reachable'
    except OSError:
        return 'unreachable'


def storage(root, previous, now):
    mount = root.parent
    try:
        device = subprocess.check_output(['findmnt', '-T', root, '-n', '-o', 'SOURCE'],
                                         text=True, timeout=3, stderr=subprocess.DEVNULL).strip()
        if not mount.is_mount() or device != '/dev/vdb1':
            return dict(availability='missing')
        usage = shutil.disk_usage(mount)
    except (OSError, subprocess.SubprocessError):
        return dict(availability='unreachable')
    sample = previous.get('sample_time', now)
    baseline = previous.get('sample_used', usage.used)
    growth = previous.get('growth_bytes_per_day')
    if now-sample >= 3600:
        growth = max(0, round((usage.used-baseline)*86400/(now-sample)))
        sample, baseline = now, usage.used
    return dict(availability='available', free_bytes=usage.free, total_bytes=usage.total,
                growth_bytes_per_day=growth, sample_time=sample, sample_used=baseline)


def coverage(root, market, hf, height):
    meta = root/'metadata'
    publication, stamp = document(meta/'publication.json')
    encoded, encoded_stamp = document(meta/'encoded-publication.json')
    archive, archive_stamp = document(meta/'global-revert-publication.json')
    selected, selected_stamp = document(meta/'protocol-revert-publication.json')
    complete = lambda doc, key: doc.get(key) if doc.get('stage') == 'complete' else None
    market_height = complete(publication, 'market_through')
    raw = complete(encoded, 'market_through')
    revert = [complete(publication, 'revert_through'), complete(archive, 'through'), complete(selected, 'through')]
    accepted = {
        'events': (min(raw, market_height) if raw is not None and market_height is not None else None,
                   min(encoded_stamp, stamp) if stamp and encoded_stamp else None),
        'state': (market_height, stamp),
        'revert': (min(revert) if all(n is not None for n in revert) else None,
                   min(archive_stamp, selected_stamp, stamp) if all((archive_stamp, selected_stamp, stamp)) else None),
        'hf': (hf.get('through'), hf.get('evidence_mtime')),
    }
    lanes = {'profiles': 'events', 'events': 'events', 'decode': 'events', 'reconstruct': 'state',
             'receipts': 'revert', 'receipt_delivery': 'revert', 'traces': 'revert', 'selected_delivery': 'revert'}
    phase = market.get('stage')
    cycle = market.get('cycle') or {}
    rows = {}
    for name, (through, updated) in accepted.items():
        row = dict(through=through, accepted_at=int(updated) if updated else None,
                   lag=max(0, height-through) if height is not None and through is not None else None)
        if name == lanes.get(phase) and market.get('state') in {'running', 'error', 'stopped'}:
            row.update(state=market['state'], phase=phase, start=cycle.get('start'), end=cycle.get('end'),
                       heartbeat=market.get('heartbeat'))
        if name == 'hf' and hf.get('state'):
            pending = hf.get('pending') or {}
            row.update(state=hf['state'], phase=(hf.get('active_window') or {}).get('stage'),
                       start=pending.get('start'), end=pending['stop']-1 if pending.get('stop') else None)
        rows[name] = row
    return rows


def drive_status(config, units):
    jobs = {}
    if units:
        try:
            result = subprocess.check_output(
                ['systemctl', '--user', 'show', *units.values(), '-p', 'Id', '-p', 'ActiveState'],
                env={**os.environ, 'XDG_RUNTIME_DIR': f'/run/user/{os.getuid()}'},
                text=True, timeout=4, stderr=subprocess.DEVNULL)
            for block in result.strip().split('\n\n'):
                state = dict(line.split('=', 1) for line in block.splitlines() if '=' in line)
                jobs[state.get('Id')] = state.get('ActiveState')
        except (OSError, subprocess.SubprocessError):
            pass
    receipts = {}
    for scope, paths in config.items():
        row = {}
        for path in paths:
            progress, updated = document(Path(path))
            if not progress:
                continue
            row.update({key: progress[key] for key in (
                'state', 'expected_pack_count', 'uploaded_pack_count', 'pending_pack_count') if key in progress})
            row['updated_at'] = updated
            if progress.get('read_back_exact') or progress.get('state') == 'complete':
                row['verified_through'] = progress.get('through', progress.get('verified_through'))
            elif progress.get('format') == 'dsc-revert-restore-plan-drive-receipt-v1' and progress.get('restore'):
                row.update(verified_through=progress['through'], state='complete')
            elif 'verified_through' in progress:
                row['verified_through'] = progress['verified_through']
            if isinstance(progress.get('verified'), (dict, list)):
                row['verified_count'] = len(progress['verified'])
            if isinstance(progress.get('pending'), list):
                row['pending_count'] = len(progress['pending'])
            if 'verified_windows' in progress:
                row['verified_count'] = progress['verified_windows']
                if progress.get('next_block'):
                    row['verified_through'] = progress['next_block']-1
            if progress.get('all_packages_sha256_exact') and 'jobs' in progress:
                row.update(verified_count=progress['jobs'], state='complete')
            if type(progress.get('windows')) is int:
                row['verified_count'] = progress['windows']
            elif isinstance(progress.get('windows'), list):
                row['verified_count'] = len(progress['windows'])
        if row.get('state') == 'backing_up' and jobs.get(units.get(scope)) == 'failed':
            row['state'] = 'backup_job_stopped'
        receipts[scope] = row
    return receipts


def protocols(path):
    try:
        with Path(path).open(newline='') as stream:
            return Counter(row['protocol'] for row in csv.DictReader(stream))
    except (OSError, KeyError):
        return {}


def hourly_samples(previous, stages, now):
    hour = now//3600
    samples = {key: value for key, value in previous.get('hourly', {}).items()
               if hour-24 <= int(key) <= hour}
    samples.setdefault(str(hour), {key: row['through'] for key, row in stages.items() if row['through'] is not None})
    return samples


def collect(config, market, hf, previous):
    root = Path(config['root'])
    now = int(time.time())
    with ThreadPoolExecutor(max_workers=6) as pool:
        head = pool.submit(chain, previous.get('chain', {}))
        timer = pool.submit(schedule, previous.get('schedule', {}))
        probes = {key: pool.submit(probe_worker, config['workers'][key]) for key in WORKERS}
        disk = storage(root, previous.get('storage', {}), now)
        chain_info = head.result()
        workers = {key: {'availability': future.result()} for key, future in probes.items()}
        schedule_info = timer.result()
    active = hf.get('active_window') or {}
    if helper := HELPER_NAMES.get(active.get('helper')):
        workers[helper]['task'] = 'hf'
        if active.get('unavailable_helpers', 0):
            workers[helper]['fallback'] = 'yes'
    current = market['coordinator']
    if current.get('stage') in {'receipt_delivery', 'traces', 'selected_delivery'} and current.get('state') == 'running':
        workers['CU3']['task'] = 'revert'
    stages = coverage(root, current, hf, chain_info.get('finalized'))
    inventory = protocols(config['registry'])
    for key, row in stages.items():
        old = previous.get('stages', {}).get(key, {})
        if old.get('through') == row['through'] and old.get('accepted_at'):
            row['accepted_at'] = old['accepted_at']
        names = ('aave-v3',) if key == 'hf' else sorted(inventory)
        if row['through'] is not None:
            row['protocols'] = {name: row['through'] for name in names}
    samples = hourly_samples(previous, stages, now)
    for key, before in samples.get(str(now//3600-24), {}).items():
        if stages[key]['through'] is not None:
            stages[key]['advance_24h'] = max(0, stages[key]['through']-before)
    return dict(chain=chain_info, schedule=schedule_info, storage=disk, workers=workers,
                stages=stages, hourly=samples,
                drive=drive_status(config.get('drive', {}), config.get('drive_units', {})))
