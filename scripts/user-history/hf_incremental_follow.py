"""Catch up finalized HF windows, advancing only through sealed Node results."""
import argparse
from contextlib import closing
import fcntl
import json
from pathlib import Path
import sqlite3
import time

from hf_incremental_boundary import read_boundary
from shared.artifacts import checksum, write_json


def read(path):
    return json.loads(path.read_text())


def catalog_head(catalog):
    uri = (catalog.resolve()/'catalog.sqlite').as_uri()+'?mode=ro'
    with closing(sqlite3.connect(uri, uri=True)) as connection:
        row = connection.execute("select value from meta where key='through'").fetchone()
    if row is None:
        raise ValueError('The market catalog has no committed block')
    return int(row[0])


def validate_opening(path, next_block=None):
    report = read(path/'boundary.json')
    if next_block is not None and report['next_block'] != next_block:
        raise ValueError('HF checkpoint does not join the requested successor')
    read_boundary(path/'state.json.gz', report['checkpoint']['sha256'], report['next_block'],
                  report['predecessor_hash'], report['release_generation'])
    return report


def advance(cursor, available, target, size):
    if cursor['pending']:
        return cursor['pending']
    start = cursor['next_block']
    last = min(available, target, start+size-1)
    return dict(start=start, stop=last+1) if last >= start else None


def follow(opening, catalog, historical_root, cryo, windows, work, target, size, helpers, once=False):
    from hf_incremental_run import run

    opening = opening.resolve()
    windows = windows.resolve()
    work.mkdir(parents=True, exist_ok=True)
    path = work/'progress.json'
    initial = validate_opening(opening)
    identity = dict(opening=str(opening), opening_sha256=checksum(opening/'boundary.json'),
                    catalog=str(catalog.resolve()), windows=str(windows),
                    historical_plan_sha256=checksum(historical_root/'plan.json'), size=size)
    with (work/'follow.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        cursor = (read(path) if path.exists() else
                  dict(identity=identity, next_block=initial['next_block'], opening=str(opening),
                       pending=None, completed_windows=0, last_accepted=None))
        if cursor['identity'] != identity:
            raise ValueError('HF follower is bound to different sources or window size')
        cursor['target'] = target
        while True:
            if cursor['next_block'] > target and cursor['pending'] is None:
                cursor['state'] = 'complete'
                write_json(path, cursor)
                return cursor
            pending = advance(cursor, catalog_head(catalog), target, size)
            if pending is None:
                cursor['state'] = 'waiting_for_catalog'
                write_json(path, cursor)
                if once:
                    return cursor
                time.sleep(30)
                continue
            if pending['start'] != cursor['next_block'] or pending['stop']-1 > target:
                raise ValueError('Pending HF interval conflicts with the cursor or target')
            source = Path(cursor['opening'])
            validate_opening(source, pending['start'])
            window = windows/f"{pending['start']}-{pending['stop']-1}"
            cursor.update(state='running', pending=pending)
            write_json(path, cursor)
            offset = cursor['completed_windows'] % len(helpers) if helpers else 0
            ordered = helpers[offset:]+helpers[:offset]
            try:
                evidence = run(source, pending['stop']-1, catalog, historical_root, cryo,
                               window, helpers=ordered)
                child = window/'successor'
                if (evidence['next_block'] != pending['stop']
                        or checksum(child/'boundary.json') != evidence['successor_sha256']):
                    raise ValueError('HF runner did not publish the exact successor')
                validate_opening(child, pending['stop'])
            except Exception:
                write_json(path, dict(cursor, state='retry_pending'))
                raise
            cursor.update(state='accepted', opening=str(child), next_block=pending['stop'],
                          pending=None, completed_windows=cursor['completed_windows']+1,
                          last_accepted=dict(path=str(window), **evidence))
            write_json(path, cursor)
            if once:
                return cursor


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for field in ('opening', 'catalog', 'historical-root', 'cryo', 'windows', 'work'):
        parser.add_argument('--'+field, type=Path, required=True)
    parser.add_argument('--through', type=int, required=True)
    parser.add_argument('--size', type=int, default=1024)
    parser.add_argument('--helpers', type=Path)
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args()
    if args.size < 1:
        parser.error('--size must be positive')
    helpers = read(args.helpers) if args.helpers else []
    result = follow(args.opening, args.catalog, args.historical_root, args.cryo, args.windows, args.work,
                    args.through, args.size, helpers, args.once)
    print(json.dumps(result))


if __name__ == '__main__':
    main()
