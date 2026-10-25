"""Small shared helpers for the manually scheduled encoded-event queue."""
import json
import os
from pathlib import Path
import time


def read(path):
    return json.loads(Path(path).read_text())


def write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(value, indent=2) + '\n')
    os.replace(tmp, path)


def chunks(job, plan):
    return {f'ethereum__logs__{start:08d}_to_{min(start + plan["chunk_size"] - 1, plan["end_block"]):08d}.parquet':
            min(plan['chunk_size'], plan['end_block'] - start + 1)
            for start in range(job['start_block'], plan['end_block'] + 1, plan['chunk_size'])}


def summarize(plan, jobs, current=None):
    total = sum(plan['end_block'] - j['start_block'] + 1 for j in plan['jobs'])
    accepted = sum(plan['end_block'] - j['start_block'] + 1 for j in plan['jobs']
                   if jobs.get(j['address'], {}).get('state') == 'complete')
    return {'updated_at': time.time(), 'end_block': plan['end_block'],
            'total_addresses': len(plan['jobs']),
            'complete_addresses': sum(j.get('state') == 'complete' for j in jobs.values()),
            'failed_addresses': sum(j.get('state') == 'failed' for j in jobs.values()),
            'accepted_blocks': accepted, 'total_blocks': total,
            'current': current, 'jobs': jobs}
