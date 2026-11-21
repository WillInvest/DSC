"""Follow committed market coverage and advance through sealed HF results."""
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


def advance(cursor, limit, size):
    if limit is None:
        return None
    pending = cursor['pending']
    if pending:
        return pending if pending['stop']-1 <= limit else None
    start = cursor['next_block']
    last = min(limit, start+size-1)
    return dict(start=start, stop=last+1) if last >= start else None


def follow(opening, catalog, historical_root, cryo, windows, work, target, size, helpers, once=False,
           *, local_accepted=None):
    if local_accepted is not None:
        # Resolve the release publisher before historical workers extend sys.path.
        from pipeline.hf_storage import place_local
    from hf_incremental_run import run

    opening = opening.resolve()
    windows = windows.resolve()
    work.mkdir(parents=True, exist_ok=True)
    path = work/'progress.json'
    initial = validate_opening(opening)
    identity = dict(opening=str(opening), opening_sha256=checksum(opening/'boundary.json'),
                    catalog=str(catalog.resolve()), windows=str(windows),
                    historical_plan_sha256=checksum(historical_root/'plan.json'), size=size)
    if local_accepted is not None:
        identity['local_accepted'] = str(local_accepted.resolve())
    with (work/'follow.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        cursor = (read(path) if path.exists() else
                  dict(identity=identity, next_block=initial['next_block'], opening=str(opening),
                       pending=None, completed_windows=0, last_accepted=None))
        if cursor['identity'] != identity:
            raise ValueError('HF follower is bound to different sources or window size')
        cursor['mode'] = 'continuous' if target is None else 'bounded'
        while True:
            if target is not None and cursor['next_block'] > target and cursor['pending'] is None:
                cursor['target'] = target
                cursor['state'] = 'complete'
                write_json(path, cursor)
                return cursor
            if target is None and not (catalog/'progress.json').is_file():
                available = None
            else:
                available = catalog_head(catalog)
            cursor['target'] = available if target is None else target
            if target is not None and cursor['pending'] and cursor['pending']['stop']-1 > target:
                raise ValueError('Pending HF interval exceeds the requested target')
            limit = min(available, target) if available is not None and target is not None else available
            pending = advance(cursor, limit, size)
            if pending is None:
                cursor['state'] = 'waiting_for_catalog'
                write_json(path, cursor)
                if once:
                    return cursor
                time.sleep(30)
                continue
            if pending['start'] != cursor['next_block']:
                raise ValueError('Pending HF interval conflicts with the cursor')
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
                if local_accepted is not None:
                    place_local(window, local_accepted)
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
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument('--through', type=int, help='Inclusive finalized cutoff')
    target.add_argument('--continuous', action='store_true', help='Follow the committed market catalog')
    parser.add_argument('--size', type=int, default=1024)
    parser.add_argument('--helpers', type=Path)
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--local-accepted', type=Path, help='Publish each accepted window here')
    args = parser.parse_args()
    if args.size < 1:
        parser.error('--size must be positive')
    helpers = read(args.helpers) if args.helpers else []
    result = follow(args.opening, args.catalog, args.historical_root, args.cryo, args.windows, args.work,
                    args.through, args.size, helpers, args.once, local_accepted=args.local_accepted)
    print(json.dumps(result))


if __name__ == '__main__':
    main()
