"""Independent host-local replay with durable results and resumable assignments."""
import collections
import fcntl
import hashlib
import json
import multiprocessing
import os
import shutil
import time
from pathlib import Path
from unittest.mock import patch

from shared.artifacts import checksum, write_json

from .ledger import JOB_ID, LocalResults, digest, load_plan


def load(path):
    return json.loads(Path(path).read_text())


def validate_package(plan):
    sources = dict(plan['identity']['package_sources'])
    sources.update(plan.get('operational_sources', {}))
    if not sources:
        raise ValueError('Missing replay package identity')
    for name, expected in sources.items():
        if Path(name).name != name or not name.endswith('.py'):
            raise ValueError('Invalid package source pin')
        if checksum(Path(__file__).with_name(name)) != expected:
            raise ValueError('Replay package changed: ' + name)
    if 'runtime' in plan['identity']:
        import numpy
        import pyarrow
        if dict(numpy=numpy.__version__, pyarrow=pyarrow.__version__) != plan['identity']['runtime']:
            raise ValueError('Numerical runtime differs from the pinned plan')


def payload_identity(result):
    return digest(dict(generation=result['generation'], job=result['job'],
                       registry_digest=result['registry_digest'], reserves=result['reserves'],
                       policy=result['policy'], replay_sources=result['replay_sources'],
                       parts=[{key: part[key] for key in ('name', 'sha256', 'start', 'stop', 'rows', 'bytes')}
                              for part in result['parts']]))


def read_receipt(directory, plan, job):
    """Validate a closed local or received result without rerunning numerical work."""
    from .validation import validate_result
    directory = Path(directory)
    result = load(directory / 'receipt.json')
    if result['generation'] != plan['generation'] or result['job'] != job:
        raise ValueError('Receipt generation or job differs')
    if result['identity'] != plan['identity']:
        raise ValueError('Receipt scientific identity differs')
    if result['replay_sources'] != plan['source_pins']:
        raise ValueError('Receipt source pins differ')
    validation = result['node_validation']
    if not validation['structural_exact'] or not validation['within_error_budget']:
        raise ValueError('Receipt lacks passing own-node validation')
    if validation['emode_category_samples'] != len(result['samples']):
        raise ValueError('Receipt eMode sample coverage differs')
    for part in result['parts']:
        if Path(part['name']).name != part['name']:
            raise ValueError('Invalid part filename')
        part['path'] = str(directory / part['name'])
    validate_result(result)
    if result['payload_sha256'] != payload_identity(result):
        raise ValueError('Receipt payload identity differs')
    return result


def run_job(config, plan, job):
    """Compute once, validate against own node, then atomically publish a receipt."""
    part_blocks = plan['identity']['encoding']['part_blocks']
    if config.get('part_blocks', part_blocks) != part_blocks:
        raise ValueError('Host encoding window differs from the pinned plan')
    from web3 import HTTPProvider
    from .calculation import compute
    from .validation import validate_node_samples
    root = Path(config['root'])
    directory = root / 'results' / job['id']
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / 'writer.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if (directory / 'receipt.json').exists():
            result = read_receipt(directory, plan, job)
        else:
            checkpoints = [p for p in config.get('checkpoints', []) if p['block'] <= job['start']]
            checkpoint = max(checkpoints, key=lambda p: p['block']) if checkpoints else None
            result = compute(job, checkpoint, part_blocks=part_blocks,
                             source_pins=plan['source_pins'])
            counts = collections.Counter()
            request = HTTPProvider.make_request
            def counted(provider, method, params):
                counts[str(method)] += 1
                return request(provider, method, params)
            with patch.object(HTTPProvider, 'make_request', counted):
                node_validation = validate_node_samples(result['samples'], job)
            for part in result['parts']:
                raw = part.pop('data')
                target = directory / part['name']
                if hashlib.sha256(raw).hexdigest() != part['sha256']:
                    raise ValueError('Computed part checksum differs')
                if target.exists():
                    if checksum(target) != part['sha256']:
                        raise ValueError('Interrupted output differs')
                else:
                    incoming = target.with_suffix('.incoming')
                    with incoming.open('wb') as stream:
                        stream.write(raw)
                        stream.flush()
                        os.fsync(stream.fileno())
                    os.replace(incoming, target)
                part['path'] = str(target)
            result.update(generation=plan['generation'], identity=plan['identity'],
                          operational_sources=plan.get('operational_sources', {}),
                          role=config['role'], node_validation=node_validation,
                          normal_provider_methods=dict(counts), normal_provider_rpc=sum(counts.values()),
                          completed_at=time.time(), production_coverage=True)
            result['payload_sha256'] = payload_identity(result)
            write_json(directory / 'receipt.json', result)
            result = read_receipt(directory, plan, job)
        return dict(job_id=job['id'], receipt_sha256=checksum(directory / 'receipt.json'),
                    payload_sha256=result['payload_sha256'])


def run(config_path, once=False):
    config = load(config_path)
    plan = load_plan(config['plan_path'])
    validate_package(plan)
    root = Path(config['root'])
    root.mkdir(parents=True, exist_ok=True)
    (root / 'results').mkdir(exist_ok=True)
    jobs = {job['id']: job for job in plan['jobs']}
    with (root / 'worker.lock').open('a') as guard:
        fcntl.flock(guard, fcntl.LOCK_EX | fcntl.LOCK_NB)
        results = LocalResults(root / 'local.sqlite', plan['generation'])
        completed = results.completed()
        active = {}
        started = {}
        began = time.monotonic()
        try:
            with multiprocessing.get_context('spawn').Pool(config['slots'], maxtasksperchild=1) as pool:
                while True:
                    for future in list(active):
                        if not future.ready():
                            if time.monotonic() - started[future] > config.get('max_job_seconds', 7200):
                                raise TimeoutError('Job exceeded configured deadline: ' + active[future])
                            continue
                        key = active.pop(future)
                        started.pop(future)
                        value = future.get()
                        results.commit(key, value['receipt_sha256'], value['payload_sha256'])
                        completed.add(key)
                    assignment = root / 'assignments.json'
                    keys = []
                    if assignment.exists():
                        assigned = load(assignment)
                        if assigned['generation'] != plan['generation']:
                            raise ValueError('Assignment generation differs')
                        keys = assigned['jobs']
                        if len(keys) != len(set(keys)) or not set(keys) <= jobs.keys():
                            raise ValueError('Assignment scope differs')
                    free = shutil.disk_usage(root).free
                    held = (root / 'HOLD').exists()
                    for key in keys:
                        if held or len(active) >= config['slots']:
                            break
                        if key in completed or key in active.values():
                            continue
                        if free < config.get('reserve_bytes', 50 * 1024**3):
                            break
                        future = pool.apply_async(run_job, (config, plan, jobs[key]))
                        active[future] = key
                        started[future] = time.monotonic()
                    status = dict(role=config['role'], generation=plan['generation'],
                                  state='running' if active else ('held' if held else 'waiting_for_assignment'),
                                  updated_at=time.time(), completed_jobs=len(completed),
                                  active_jobs=list(active.values()), assigned_jobs=len(keys),
                                  free_bytes=free, elapsed_seconds=time.monotonic() - began)
                    write_json(root / 'progress.json', status)
                    if once and not active and all(key in completed for key in keys):
                        return
                    time.sleep(config.get('poll_seconds', 2))
        except Exception as exc:
            write_json(root / 'progress.json', dict(role=config['role'], generation=plan['generation'],
                       state='failed', updated_at=time.time(), active_jobs=list(active.values()),
                       error_type=type(exc).__name__, error=str(exc)))
            raise
        finally:
            results.close()
