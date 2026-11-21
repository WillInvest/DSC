"""Read-only progress for the Node shadow, legacy producer and backup lanes."""
import argparse
import json
from pathlib import Path
import re
import subprocess
import time

from shared.artifacts import write_json

LEGACY_COMMAND = 'dsc-pipeline-status-v1'
FORMAT = 'dsc-node-progress-v1'
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


def fetch_legacy(config):
    alias = config['host']
    if not isinstance(alias, str) or not LABEL.fullmatch(alias):
        raise ValueError('Invalid legacy status SSH alias')
    command = ['ssh', '-F', config['ssh_config'], '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=8',
               alias, LEGACY_COMMAND]
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


def report(config):
    if config['version'] != 1:
        raise ValueError('Unknown Node monitoring configuration')
    return dict(
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='mode', required=True)
    legacy = sub.add_parser('legacy')
    legacy.add_argument('root', type=Path)
    aggregate = sub.add_parser('aggregate')
    aggregate.add_argument('config', type=Path)
    aggregate.add_argument('--output', type=Path)
    aggregate.add_argument('--follow', action='store_true')
    args = parser.parse_args()
    if args.mode == 'legacy':
        print(json.dumps(legacy_report(args.root)))
    else:
        config = json.loads(args.config.read_text())
        if args.follow and args.output is None:
            parser.error('--follow requires --output')
        while True:
            data = report(config)
            if args.output:
                write_json(args.output, data)
            if not args.follow:
                print(json.dumps(data))
                return
            time.sleep(30)


if __name__ == '__main__':
    main()
