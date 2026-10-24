"""Read-only, interval-based reporting. No Tracker constructor or migrations."""
import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sqlite3


def masks(target, coverage):
    """Resolve applicable configuration segments into nonoverlapping bitmasks."""
    output = []
    for segment in target['segments']:
        a, b = segment['start'], segment['stop']
        events = {a: [], b: []}
        for bit in (1, 2, 4):
            for lo, hi in coverage.get((target['address'], bit, segment['configs'][str(bit)]), []):
                lo, hi = max(a, lo), min(b, hi)
                if lo < hi:
                    events.setdefault(lo, []).append((bit, 1))
                    events.setdefault(hi, []).append((bit, -1))
        active = {1:0, 2:0, 4:0}
        points = sorted(events)
        for n, end in zip(points, points[1:]):
            for bit, delta in events[n]:
                active[bit] += delta
            mask = sum(bit for bit, count in active.items() if count)
            if output and output[-1][1] == n and output[-1][2] == mask:
                output[-1][1] = end
            else:
                output.append([n, end, mask])
    return output


def contract_report(target, coverage):
    spans = masks(target, coverage)
    total = target['stop'] - target['start']
    complete = sum(b-a for a,b,mask in spans if mask == 7)
    contiguous = target['start'] - 1
    for a,b,mask in spans:
        if a != contiguous + 1 or mask != 7:
            break
        contiguous = b-1
    return {key: target[key] for key in ('address','protocol','name','creation_block','start','stop')} | {
        'total': total, 'complete': complete, 'percent': 100*complete/total if total else 0,
        'datasets': {str(bit): sum(b-a for a,b,mask in spans if mask & bit) for bit in (1,2,4)},
        'contiguous': contiguous, 'intervals': spans,
        'missing_ranges': [span for span in spans if span[2] != 7][:10],
        'missing_range_count': sum(span[2] != 7 for span in spans),
        'blocked_profile': any(s.get('blocked_profile') for s in target['segments'])}


def safe_error(error):
    if not error:
        return None
    # Raw exception strings may include complete commands, paths or credentials.
    text = error.lower()
    for needle, message in [('profile','Historical ABI profile unavailable'), ('checksum','Artifact validation failed'),
                            ('hash','Block or artifact hash mismatch'), ('timed out','Worker connection timed out'),
                            ('permission','Worker access denied'), ('space','Insufficient disk space')]:
        if needle in text:
            return message
    return 'Job failed; inspect its server-side log'


def optional_json(path):
    try:
        return json.loads(path.read_text())
    except FileNotFoundError:
        return None


def read_source(source):
    path = Path(source['db'])
    result = {'source': source['id'], 'label': source['label'], 'runs': [], 'heartbeat': optional_json(path.parent/'heartbeat.json'), 'active': optional_json(path.parent/'active-run.json')}
    if not path.exists():
        result['state'] = 'not_started'
        return result
    connection = sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True, timeout=5)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute('PRAGMA query_only=ON')
        connection.execute('BEGIN')
        tables = {r[0] for r in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if 'monitor_runs' not in tables:
            result['state'] = 'legacy'
            return result
        coverage = {}
        for r in connection.execute('SELECT * FROM coverage'):
            coverage.setdefault((r['address'],r['bit'],r['config']),[]).append((r['start'],r['stop']))
        for run in connection.execute('SELECT * FROM monitor_runs ORDER BY created DESC,id LIMIT 20'):
            manifest = json.loads(run['manifest'])
            contracts = [contract_report(t, coverage) for t in manifest['targets']]
            total = sum(c['total'] for c in contracts)
            done = sum(c['complete'] for c in contracts)
            jobs = []
            counts = dict(connection.execute('SELECT j.stage,count(*) FROM jobs j JOIN monitor_jobs m ON m.job_id=j.id WHERE m.run_id=? GROUP BY j.stage',(run['id'],)).fetchall())
            query = 'SELECT j.* FROM jobs j JOIN monitor_jobs m ON m.job_id=j.id WHERE m.run_id=? ORDER BY j.updated DESC,j.id LIMIT 100'
            for r in connection.execute(query,(run['id'],)):
                if len(jobs) < 100:
                    spec = json.loads(r['spec'])
                    jobs.append({'id':r['id'], 'kind':spec['kind'], 'address':spec.get('address'), 'stage':r['stage'],
                                 'start':r['start'], 'stop':r['stop'], 'updated':r['updated'], 'error':safe_error(r['error'])})
            result['runs'].append({'id': source['id']+':'+run['id'], 'local_id': run['id'], 'created':run['created'],
                'start':manifest['start'], 'stop':manifest['stop'], 'finished':bool(total and done == total),
                'total':total, 'complete':done, 'percent':100*done/total if total else 0,
                'datasets':{str(bit):sum(c['datasets'][str(bit)] for c in contracts) for bit in (1,2,4)},
                'contracts':contracts, 'jobs':jobs, 'job_counts':counts})
        result['state'] = 'available' if result['runs'] else 'legacy'
        return result
    finally:
        connection.close()


def export(config):
    registry = list(csv.DictReader(Path(config['registry']).open()))
    return {'generated_at':datetime.now(timezone.utc).isoformat(),
            'registry':[{k:r[k] for k in ('address','protocol','name','creation_block')} for r in registry],
            'sources':[read_source(source) for source in config['sources']]}
