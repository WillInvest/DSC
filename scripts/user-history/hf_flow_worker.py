"""Host-local HF production; completed jobs survive scheduler/network restarts."""
import argparse
import collections
import fcntl
import json
import multiprocessing
import os
import shutil
import time
from pathlib import Path
from unittest.mock import patch

from shared.artifacts import checksum, digest, write_json
from hf_flow_artifacts import seal, fast_verify
from hf_flow_queue import LocalLedger
from hf_flow_io import capacity_available


def load(path):
    return json.loads(Path(path).read_text())


def validate_config(config):
    plan = load(config['numeric_plan'])
    if checksum(config['numeric_plan']) != config['numeric_plan_sha256'] or digest(plan['identity']) != plan['generation']:
        raise ValueError('Frozen numerical plan changed')
    for name, sha in config['operational_code'].items():
        if checksum(Path(__file__).with_name(name)) != sha:
            raise ValueError('Flow adapter code changed: ' + name)
    if len(config['jobs']) != len(set(config['jobs'])) or not set(config['jobs']) <= {j['id'] for j in plan['jobs']}:
        raise ValueError('Invalid disjoint assignment')
    if digest(config['jobs']) != config['assignment_sha256']:
        raise ValueError('Host assignment identity changed')
    if not config.get('canary_limit') and config.get('production_enabled') is not True:
        raise ValueError('Production configuration has not passed the flow checks')
    return plan


def run_one(payload, directory, role):
    """Use original numerical producer and original complete acceptance once."""
    from hf_f64_compute import compute
    from hf_f64_accept import accept, check_receipt
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / 'flow.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        done = directory / 'flow-done.json'
        if done.exists():
            value = load(done)
            if checksum(value['path']) != value['sha256']:
                raise ValueError('Local completed receipt changed')
            fast_verify(value['certificate'])
            return value
        receipt = directory / 'receipt.json'
        if receipt.exists():
            # Only an interrupted publication, never the normal restart path.
            result = check_receipt(receipt, payload['job'], payload['generation'])
        else:
            result = compute(payload)
            result.update(identity=payload['identity'], generation=payload['generation'], role=role,
                          worker='independent-local-process', checkpoint_used=payload.get('checkpoint'))
            from web3 import HTTPProvider
            counts = collections.Counter()
            original = HTTPProvider.make_request
            def counted(self, method, params):
                counts[str(method)] += 1
                return original(self, method, params)
            with patch.object(HTTPProvider, 'make_request', counted):
                if role == 'web':
                    with Path('/home/fao/dex-compaction-writer.lock').open('a') as writer:
                        while True:
                            try:
                                fcntl.flock(writer, fcntl.LOCK_EX | fcntl.LOCK_NB)
                                break
                            except BlockingIOError:
                                time.sleep(1)
                        result = accept(result, directory)
                else:
                    result = accept(result, directory)
        files = {Path(p['path']).name: dict(bytes=Path(p['path']).stat().st_size, sha256=p['sha256'])
                 for p in result['parts'] + result['aggregate_parts']}
        certificate = seal(directory, files) if files else None
        value = dict(receipt=result, path=str(receipt), sha256=checksum(receipt), certificate=certificate,
                     completed_at=time.time(), hostname=os.uname().nodename,
                     normal_provider_methods=dict(counts) if 'counts' in locals() else {},
                     normal_provider_rpc=sum(counts.values()) if 'counts' in locals() else 0)
        write_json(done, value)
        return value


def run(config_path, once=False):
    entered = time.monotonic()
    config = load(config_path)
    plan = validate_config(config)
    root = Path(config['local_root'])
    root.mkdir(parents=True, exist_ok=True)
    jobs = {j['id']: j for j in plan['jobs']}
    ordered = [jobs[k] for k in config['jobs']]
    if config.get('canary_limit'):
        ordered = ordered[:config['canary_limit']]
    role = config['role']
    os.environ['DSC_HF_ROLE'] = role
    began = time.monotonic()
    with (root / 'producer.lock').open('a') as guard, LocalLedger(root / 'local.sqlite', plan['generation']) as ledger:
        fcntl.flock(guard, fcntl.LOCK_EX | fcntl.LOCK_NB)
        completed = ledger.completed()
        initial_values = sum(jobs[k]['expected_rows'] for k in completed)
        pending = iter(j for j in ordered if j['id'] not in completed)
        next_job = next(pending, None)
        startup_seconds = time.monotonic()-entered
        active = {}
        submitted = {}
        failure = None
        def status(state):
            values = sum(jobs[k]['expected_rows'] for k in completed)
            elapsed = time.monotonic() - began
            write_json(root / 'progress.json', dict(role=role, generation=plan['generation'], state=state,
                updated_at=time.time(), completed_jobs=len(completed), assigned_jobs=len(ordered),
                user_values=values, total_values=sum(j['expected_rows'] for j in ordered),
                buffered_bytes=ledger.buffered_bytes(), free_bytes=shutil.disk_usage(root).free,
                startup_seconds=startup_seconds,
                active_jobs=[j['id'] for j in active.values()], slots=config['slots'],
                rate_values_per_second=(values-initial_values)/elapsed if elapsed > 0 else None,
                error=failure, representation='Original NumPy HF and full acceptance; independent local durable queue'))
        if next_job is None:
            status('complete')
            return
        # Spawn isolates per-job native/EVM caches and global RPC adapters.
        with multiprocessing.get_context('spawn').Pool(config['slots'], maxtasksperchild=1) as pool:
            status('running')
            while True:
                for future in list(active):
                    if not future.ready():
                        if time.monotonic()-submitted[future]>config.get('max_job_seconds',1200):
                            failure=dict(job_id=active[future]['id'],error_type='TimeoutError',error='Local compute/acceptance deadline exceeded')
                            write_json(root/'FAILED_VALIDATION.json',failure);status('failed')
                            raise TimeoutError(failure['error'])
                        continue
                    job = active.pop(future)
                    submitted.pop(future)
                    try:
                        value = future.get()
                        if value['receipt']['job'] != job or value['receipt']['generation'] != plan['generation']:
                            raise ValueError('Completion scope differs')
                        ledger.commit(job['id'], value)
                        completed.add(job['id'])
                    except Exception as exc:
                        failure = dict(job_id=job['id'], error_type=type(exc).__name__, error=str(exc))
                        write_json(root / 'FAILED_VALIDATION.json', failure)
                held = (root / 'HOLD').exists() or (root / 'FAILED_VALIDATION.json').exists()
                free = capacity_available(config,root)
                buffered = ledger.buffered_bytes()
                while next_job is not None and len(active) < config['slots'] and not held:
                    if free < config['reserve_bytes'] + config['slots'] * config['inflight_bytes_per_slot']:
                        break
                    if role != 'node' and buffered >= config['local_high_water_bytes']:
                        break
                    payload = dict(job=next_job, generation=plan['generation'], identity=plan['identity'])
                    candidates = [p for p in plan.get('checkpoints', []) if p['role'] == role and p['block'] <= next_job['start']]
                    if candidates:
                        payload['checkpoint'] = max(candidates, key=lambda p: p['block'])
                    if next_job['id'] in plan['sources']:
                        payload['hf_source'] = plan['sources'][next_job['id']]
                    directory = Path(config['output_root']) / 'outputs' / plan['generation'] / 'jobs' / next_job['id']
                    future=pool.apply_async(run_one, (payload, str(directory), role))
                    active[future]=next_job;submitted[future]=time.monotonic()
                    next_job = next(pending, None)
                if held and not active:
                    status('failed' if failure or (root / 'FAILED_VALIDATION.json').exists() else 'held')
                    return
                if next_job is None and not active:
                    status('complete')
                    return
                status('running' if active else 'waiting_for_transfer')
                if once and completed:
                    return
                time.sleep(2)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args()
    run(args.config, args.once)
