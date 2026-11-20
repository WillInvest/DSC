"""Node acceptance and finite assignments over authenticated HF transport."""
import fcntl
import time
from pathlib import Path

from shared.artifacts import checksum, write_json

from .transport import TLSTransport
from .ledger import Ledger, load_plan
from .runner import load, read_receipt, validate_package


def run(config_path, once=False):
    config = load(config_path)
    plan = load_plan(config['plan_path'])
    validate_package(plan)
    root = Path(config['root'])
    root.mkdir(parents=True, exist_ok=True)
    jobs = {job['id']: job for job in plan['jobs']}
    hosts = config['hosts']
    roles = [host['role'] for host in hosts]
    if len(roles) != len(set(roles)):
        raise ValueError('Duplicate coordinator role')
    cursor_path = root / 'cursors.json'
    cursors = load(cursor_path) if cursor_path.exists() else {}
    with (root / 'coordinator.lock').open('a') as guard:
        fcntl.flock(guard, fcntl.LOCK_EX | fcntl.LOCK_NB)
        ledger = Ledger(root / 'flow.sqlite', plan['generation'], plan['jobs'])
        transport = None
        try:
            transport = TLSTransport(config)
            while True:
                now = time.time()
                online, observations = [], {}
                for host in hosts:
                    role = host['role']
                    cursor = cursors.get(role, 0)
                    try:
                        status = transport.request(host, 'status', after=cursor)
                    except OSError as exc:
                        observations[role] = dict(state='unreachable', error=str(exc), checked_at=now)
                        continue
                    if status['generation'] != plan['generation']:
                        raise ValueError('Worker generation differs: ' + role)
                    if status['state'] == 'failed':
                        raise RuntimeError('Worker reported failure: ' + role + ': ' + str(status.get('error')))
                    transfer_failed = False
                    for entry in status['completions']:
                        key = entry['job_id']
                        if key not in jobs:
                            raise ValueError('Worker reported unknown completion')
                        if not ledger.accepted(key):
                            destination = root / 'received' / role / key
                            try:
                                transport.receive(host, key, destination)
                            except OSError as exc:
                                observations[role] = dict(state='transfer_unreachable', error=str(exc), checked_at=now)
                                transfer_failed = True
                                break
                            if checksum(destination / 'receipt.json') != entry['receipt_sha256']:
                                raise ValueError('Received receipt checksum differs')
                            result = read_receipt(destination, plan, jobs[key])
                            if result['payload_sha256'] != entry['payload_sha256']:
                                raise ValueError('Received payload identity differs')
                            ledger.accept(key, result['payload_sha256'], destination / 'receipt.json', role)
                        elif ledger.accepted(key) != entry['payload_sha256']:
                            raise ValueError('Conflicting late completion: ' + key)
                        cursor = entry['sequence']
                        cursors[role] = cursor
                        write_json(cursor_path, cursors)
                    if transfer_failed:
                        continue
                    ledger.observe(role, status, now)
                    fresh = now - status.get('updated_at', 0) <= config.get('heartbeat_seconds', 120)
                    if fresh and status['state'] != 'held':
                        ledger.renew(role, now, config.get('lease_seconds', 600))
                        online.append(host)
                    observations[role] = dict(state=status['state'] if fresh else 'stale',
                                              completed_jobs=status.get('completed_jobs', 0),
                                              active_jobs=status.get('active_jobs', []), checked_at=now)
                expired = ledger.expire(now)
                for host in online:
                    role = host['role']
                    outstanding = ledger.assigned(role)
                    target = host['slots'] * config.get('assignment_batches_per_slot', 2)
                    ledger.assign(role, max(0, target - len(outstanding)), now,
                                  config.get('lease_seconds', 600))
                    assignment = dict(generation=plan['generation'], jobs=ledger.assigned(role), created_at=now)
                    try:
                        transport.request(host, 'assign', assignment)
                    except OSError as exc:
                        observations[role]['assignment_error'] = str(exc)
                totals = ledger.totals()
                complete = totals['accepted_jobs'] == totals['planned_jobs']
                progress = dict(generation=plan['generation'], updated_at=time.time(),
                                state='complete' if complete else 'running', roles=observations,
                                expired_assignments=expired, **totals)
                write_json(root / 'progress.json', progress)
                if complete:
                    write_json(root / 'complete.json', progress)
                    return
                if once:
                    return
                time.sleep(config.get('poll_seconds', 15))
        except Exception as exc:
            write_json(root / 'coordinator-failure.json', dict(generation=plan['generation'],
                       error_type=type(exc).__name__, error=str(exc), created_at=time.time()))
            raise
        finally:
            ledger.close()
            if transport is not None:
                transport.close()
