"""Read-only progress for the Node shadow, legacy producer and backup lanes."""
import argparse
import fcntl
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import time

from shared.artifacts import write_json
from pipeline import monitor_health

LEGACY_COMMAND = 'dsc-pipeline-status-v1'
MONITOR_COMMAND = 'dsc-node-monitor-v1'
FORMAT = 'dsc-node-progress-v1'
MAX_MONITOR_BYTES = 16 * 1024
LABEL = re.compile(r'[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}')
CYCLE = re.compile(r'[0-9]{1,10}-[0-9]{1,10}')


def fields(data, integers=(), labels=(), counts=()):
    values = {key: int(data[key]) for key in integers
              if type(data.get(key)) is int and data[key] >= 0}
    values.update({key: data[key] for key in labels
                   if isinstance(data.get(key), str) and LABEL.fullmatch(data[key])})
    values.update({key: len(data[key]) for key in counts if isinstance(data.get(key), list)})
    return values


def read(path, project):
    try:
        with path.open() as stream:
            value = json.load(stream)
        if not isinstance(value, dict):
            return dict(availability='invalid')
        return dict(availability='available', evidence_mtime=path.stat().st_mtime, **project(value))
    except FileNotFoundError:
        return dict(availability='missing')
    except (OSError, ValueError, TypeError, AttributeError):
        return dict(availability='invalid')


def market(root):
    def project(value):
        report = fields(value, ('completed_through', 'observed_finalized'),
                        ('stage', 'state', 'purpose'))
        if type(value.get('updated_at')) in (int, float):
            report['heartbeat'] = int(value['updated_at'])
        cycle = value.get('cycle')
        if isinstance(cycle, dict):
            report['cycle'] = fields(cycle, ('start', 'end'), ('id', 'state'))
        return report

    coordinator = read(root/'progress.json', project)
    compaction = read(root/'compaction.json',
                      lambda value: fields(value, ('through', 'target'), ('state', 'role')))
    baseline = read(root/'baseline.json', lambda value: fields(value, ('end_block',), ('site',)))
    report = dict(coordinator=coordinator, compaction=compaction, baseline=baseline)
    cycle = coordinator.get('cycle', {}).get('id', '')
    if CYCLE.fullmatch(cycle):
        report['events'] = read(root/'cycles'/cycle/'events/progress.json',
                                lambda value: fields(value, ('complete_addresses', 'total_addresses'), ('state',)))
    return report


def legacy_report(root):
    return dict(format=FORMAT, site='data', market=market(root))


def ssh_command(config, operation):
    alias = config['host']
    if not isinstance(alias, str) or not LABEL.fullmatch(alias):
        raise ValueError('Invalid status SSH alias')
    return ['ssh', '-F', config['ssh_config'], '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=8',
            alias, operation]


def fetch_legacy(config):
    command = ssh_command(config, LEGACY_COMMAND)
    try:
        result = subprocess.run(command, stdin=subprocess.DEVNULL, capture_output=True, check=True, timeout=15)
        value = json.loads(result.stdout)
        if value['format'] != FORMAT or value['site'] != 'data':
            return dict(availability='invalid')
        market_status = {}
        for group in ('coordinator', 'compaction', 'events'):
            row = value['market'].get(group, {})
            market_status[group] = fields(row, ('completed_through', 'through', 'target',
                                                'complete_addresses', 'total_addresses'),
                                          ('availability', 'stage', 'state', 'purpose', 'role'))
            if type(row.get('evidence_mtime')) in (int, float):
                market_status[group]['evidence_mtime'] = row['evidence_mtime']
        return dict(availability='available', **market_status)
    except (OSError, subprocess.SubprocessError):
        return dict(availability='unreachable')
    except (ValueError, KeyError, AttributeError, TypeError):
        return dict(availability='invalid')


def hf_progress(path, windows=None):
    def project(value):
        result = fields(value, ('next_block', 'target', 'completed_windows', 'verified_windows'),
                        ('state', 'current_window'))
        if 'next_block' in result and result['next_block'] > 0:
            result['through'] = result['next_block']-1
        pending = value.get('pending')
        if isinstance(pending, dict):
            result['pending'] = fields(pending, ('start', 'stop'))
        last = value.get('last_accepted')
        if isinstance(last, dict):
            result.update(fields(last, labels=('helper',), counts=('unavailable_helpers',)))
        return result

    result = read(path, project)
    pending = result.get('pending', {})
    if windows is not None and 'start' in pending and pending.get('stop', 0) > pending['start']:
        active = windows/f"{pending['start']}-{pending['stop']-1}"/'progress.json'
        result['active_window'] = read(active, lambda value: fields(
            value, ('start', 'stop'), ('stage', 'helper'), ('unavailable_helpers',)))
    return result


def handoff(value):
    result = fields(value.get('identity', {}), ('through',))
    result.update(fields(value, counts=('checkpoints',)))
    result.update(fields(value.get('archives', {}), ('failed_transactions', 'selected_transactions')))
    return result


def report(config, previous=None):
    if config['version'] != 1:
        raise ValueError('Unknown Node monitoring configuration')
    data = dict(
        format=FORMAT,
        observed_at=time.time(),
        legacy=fetch_legacy(config['legacy']),
        node_market=market(Path(config['node_market'])),
        shadow=read(Path(config['shadow']), lambda value: fields(
            value, ('block', 'state_addresses'), ('state',))),
        handoff=read(Path(config['handoff']), handoff),
        market_drive=read(Path(config['market_drive']), lambda value: fields(
            value, ('verified_through', 'pending'), ('state',))),
        historical_market=read(Path(config['historical_market']), lambda value: fields(
            value, ('through', 'total_markets'), ('state',), ('published',))),
        historical_market_drive=read(Path(config['historical_market_drive']), lambda value: fields(
            value, ('through',), ('state',), ('verified',))),
        historical_revert_drive=read(Path(config['historical_revert_drive']), lambda value: fields(
            value, ('through',), ('state',), ('verified',))),
        hf=hf_progress(Path(config['hf']), Path(config['hf_windows'])),
        hf_drive=hf_progress(Path(config['hf_drive'])),
    )
    if 'health' in config:
        data['health'] = monitor_health.collect(config['health'], data['node_market'], data['hf'],
                                                (previous or {}).get('health', {}))
    return data


def web_health(data):
    def group(name, keys, integers=(), labels=()):
        return {key: fields(row, integers, labels) for key, row in data.get(name, {}).items()
                if key in keys and isinstance(row, dict)}

    stages = group('stages', monitor_health.STAGES,
                   ('through', 'lag', 'start', 'end', 'accepted_at', 'heartbeat', 'advance_24h'), ('state', 'phase'))
    protocols = {'uniswap-v2', 'uniswap-v3', 'aave-v2', 'aave-v3'}
    for name, row in stages.items():
        coverage = data['stages'][name].get('protocols')
        if isinstance(coverage, dict):
            row['protocols'] = [dict(name=key, through=height) for key, height in coverage.items()
                                if key in protocols and type(height) is int and height >= 0]
    return dict(
        chain=fields(data.get('chain', {}), ('latest', 'finalized', 'latest_time', 'finalized_time',
                                           'last_success', 'unfinalized'), ('availability',)),
        schedule=fields(data.get('schedule', {}), ('last_start', 'last_success', 'next_run', 'elapsed'),
                        ('availability', 'state', 'result', 'timer')),
        storage=fields(data.get('storage', {}), ('free_bytes', 'total_bytes', 'growth_bytes_per_day'),
                       ('availability',)),
        stages=stages,
        workers=group('workers', monitor_health.WORKERS, labels=('availability', 'task', 'fallback')),
        drive=group('drive', ('market_historical', 'market_new', 'revert', 'wallet_historical',
                              'hf_historical_aggregates', 'hf_contiguous', 'hf_tail'),
                    ('verified_through', 'verified_count', 'pending_count', 'expected_pack_count',
                     'uploaded_pack_count', 'pending_pack_count', 'updated_at'), ('state',)),
    )


def web_status(report):
    timestamp = report['observed_at']
    if (report['format'] != FORMAT or type(timestamp) not in (int, float)
            or not math.isfinite(timestamp) or timestamp <= 0):
        raise ValueError('Invalid Node monitor snapshot')
    market = report['node_market']
    legacy = report['legacy']
    return dict(
        format=FORMAT, observed_at=timestamp,
        market=fields(market['coordinator'], ('completed_through',), ('state', 'stage')),
        catalog=fields(market['compaction'], ('through',), ('state',)),
        events=fields(market.get('events', {}), ('complete_addresses', 'total_addresses'), ('state',)),
        legacy=fields(legacy, labels=('availability',)),
        handoff=fields(report['handoff'], ('through',)),
        shadow=fields(report['shadow'], ('block', 'state_addresses'), ('state',)),
        market_drive=fields(report['market_drive'], ('verified_through', 'pending'), ('state',)),
        hf=fields(report['hf'], ('through',), ('state',)),
        hf_drive=fields(report['hf_drive'], ('through',), ('state',)),
        historical_market=fields(report['historical_market'], ('through', 'published', 'total_markets')),
        historical_market_drive=fields(report['historical_market_drive'], ('verified',), ('state',)),
        historical_revert_drive=fields(report['historical_revert_drive'], ('through',), ('state',)),
        health=web_health(report.get('health', {})),
    )


def receive_monitor(path, source):
    body = source.read(MAX_MONITOR_BYTES + 1)
    if len(body) > MAX_MONITOR_BYTES:
        raise ValueError('Node monitor snapshot exceeds size limit')
    safe = web_status(json.loads(body))
    with path.with_suffix('.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        write_json(path, safe)


def publish_monitor(config, data):
    subprocess.run(ssh_command(config, MONITOR_COMMAND), input=json.dumps(data).encode(),
                   capture_output=True, check=True, timeout=12)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='mode', required=True)
    legacy = sub.add_parser('legacy')
    legacy.add_argument('root', type=Path)
    receive = sub.add_parser('receive-monitor')
    receive.add_argument('output', type=Path)
    aggregate = sub.add_parser('aggregate')
    aggregate.add_argument('config', type=Path)
    aggregate.add_argument('--output', type=Path)
    aggregate.add_argument('--follow', action='store_true')
    args = parser.parse_args()
    if args.mode == 'legacy':
        print(json.dumps(legacy_report(args.root)))
    elif args.mode == 'receive-monitor':
        if os.environ.get('SSH_ORIGINAL_COMMAND') != MONITOR_COMMAND:
            parser.error('Monitor receiver requires its fixed SSH command')
        receive_monitor(args.output, sys.stdin.buffer)
    else:
        config = json.loads(args.config.read_text())
        if args.follow and args.output is None:
            parser.error('--follow requires --output')
        previous = None
        if args.output and args.output.is_file():
            try:
                previous = json.loads(args.output.read_text())
            except (OSError, ValueError):
                pass
        while True:
            data = report(config, previous)
            if args.output:
                write_json(args.output, data)
            if 'publish_monitor' in config:
                try:
                    publish_monitor(config['publish_monitor'], data)
                except (OSError, subprocess.SubprocessError):
                    print('Cannot reach the private monitor receiver', file=sys.stderr)
            if not args.follow:
                print(json.dumps(data))
                return
            previous = data
            time.sleep(30)


if __name__ == '__main__':
    main()
