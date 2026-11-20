"""Small queue control endpoint for assignments and closed-result discovery.

Run this file directly with the unchanged shared.artifacts helper on PYTHONPATH.
It reads no numerical libraries and does not start or stop system services.
"""
import argparse
import json
import sqlite3
import sys
from pathlib import Path

from shared.artifacts import write_json


def dispatch(root, operation, after=0, assignment=None):
    root = Path(root).resolve()
    config = json.loads((root / 'role.json').read_text())
    plan = json.loads(Path(config['plan_path']).read_text())
    if operation == 'assign':
        if assignment is None:
            assignment = json.load(sys.stdin)
        valid = {job['id'] for job in plan['jobs']}
        keys = assignment['jobs']
        if assignment['generation'] != plan['generation'] or len(keys) != len(set(keys)) or not set(keys) <= valid:
            raise ValueError('Invalid node assignment')
        write_json(root / 'assignments.json', assignment)
        return dict(assigned_jobs=len(keys), generation=plan['generation'])
    progress = root / 'progress.json'
    report = json.loads(progress.read_text()) if progress.exists() else dict(state='not_started', active_jobs=[])
    report['generation'] = plan['generation']
    report['completions'] = []
    path = root / 'local.sqlite'
    if path.exists():
        with sqlite3.connect('file:' + str(path) + '?mode=ro', uri=True) as database:
            database.execute('begin')
            report['completions'] = [dict(sequence=n, job_id=key, receipt_sha256=sha, payload_sha256=payload)
                for n, key, sha, payload in database.execute(
                    'select sequence,job_id,receipt_sha256,payload_sha256 from done where sequence>? order by sequence limit 128', (after,))]
            database.rollback()
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', required=True)
    parser.add_argument('--operation', choices=('status', 'assign'), required=True)
    parser.add_argument('--after', type=int, default=0)
    args = parser.parse_args()
    print(json.dumps(dispatch(args.root, args.operation, args.after), sort_keys=True))
