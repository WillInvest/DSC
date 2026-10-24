"""Immutable reporting targets and a heartbeat independent of extraction calls."""
from contextlib import contextmanager
from datetime import datetime, timezone
import threading
import json
from orchestration.artifacts import digest, write_json
from orchestration.locking import file_lock


def record_run(coordinator, key, settings):
    from orchestration.coordinator import fingerprint
    from protocols.protocol_config import load_protocol
    targets = []
    for row in coordinator.rows:
        start, stop = max(settings['start'], row['creation_block']), settings['stop']
        if start >= stop:
            continue
        profiles = [p for p in coordinator.profiles if p['address'].lower() == row['address']]
        bounds = sorted({start, stop} | {n for p in profiles for n in (p['from_block'], p['to_block']) if start < n < stop})
        segments = []
        for a, b in zip(bounds, bounds[1:]):
            matches = [p for p in profiles if p['from_block'] <= a and b <= p['to_block']]
            profile = matches[0] if len(matches) == 1 else None
            configs = {str(bit): digest({'code': fingerprint(kind, row['protocol']), 'profile': profile}) for kind, bit in [('events', 1), ('state', 2)]}
            configs['4'] = fingerprint('reverted')
            segments.append({'start': a, 'stop': b, 'configs': configs,
                             'blocked_profile': bool(load_protocol(row['protocol']).get('requires_deployment') and profile is None)})
        targets.append({**row, 'start': start, 'stop': stop, 'segments': segments})
    manifest = {'schema': 1, 'run_key': key, **settings, 'targets': targets}
    ident = digest(manifest)
    tracker = coordinator.tracker
    with tracker.db:
        tracker.db.execute('INSERT OR IGNORE INTO monitor_runs(id,manifest) VALUES(?,?)', (ident, __import__('json').dumps(manifest)))
    tracker.run_id = ident
    by_address = {t['address']:t for t in targets}
    bits = {'events':1, 'state':2}
    for job in tracker.db.execute('SELECT id,spec FROM jobs WHERE stop>? AND start<?',(settings['start'],settings['stop'])).fetchall():
        spec = json.loads(job['spec'])
        kind = spec['kind']
        match = False
        if kind in bits and spec.get('address') in by_address:
            match = any(s['configs'][str(bits[kind])] == spec['config'] and s['start'] < spec['stop'] and s['stop'] > spec['start'] for s in by_address[spec['address']]['segments'])
        elif kind == 'archive':
            match = spec['config'] == coordinator.archive_config
        elif kind == 'reverted':
            match = spec['config'] == fingerprint('reverted') and bool(set(spec['contracts']) & set(by_address))
        if match:
            with tracker.db:
                tracker.db.execute('INSERT OR IGNORE INTO monitor_jobs VALUES(?,?)',(ident,job['id']))
    with file_lock(coordinator.root / 'reporting.lock'):
        write_json(coordinator.root / 'active-run.json', {'run_id': ident})


@contextmanager
def heartbeat(root, lane='all'):
    stop = threading.Event()
    def emit(state):
        write_json(root / ('heartbeat.json' if lane == 'all' else f'heartbeat-{lane}.json'), {'state': state, 'timestamp': datetime.now(timezone.utc).isoformat()})
    def loop():
        while not stop.wait(15):
            emit('running')
    emit('running')
    thread = threading.Thread(target=loop, daemon=True)
    thread.start()
    completed = False
    try:
        yield
        completed = True
    finally:
        stop.set()
        thread.join()
        emit('complete' if completed and lane != 'all' else 'stopped')
