"""Apply only the already-reconciled Data cleanup, using the restricted HF helper."""
import argparse
import json
import subprocess
from pathlib import Path

from hf_flow_artifacts import _proof
from hf_flow_io import retire_local
from hf_flow_queue import read_snapshot
from shared.artifacts import checksum, digest, write_json

FLOW = Path('/var/lib/dsc-aave-hf-flow/20261116')


def read(path):
    return json.loads(path.read_text())


def prepared_groups(request_dir, prepared_sha, acceptance_sha):
    config = read(FLOW / 'role.json')
    local = FLOW / 'local'
    transfers = local / 'transfers'
    if (config['role'] != 'data' or Path(config['local_root']) != local or
            Path(config['output_root']) != local / 'results' or
            not request_dir.resolve().is_relative_to(transfers)):
        raise ValueError('Source is outside the explicit Data HF result scope')
    prepared_path = request_dir / 'prepared.json'
    if checksum(prepared_path) != prepared_sha:
        raise ValueError('Prepared Node proof manifest differs')
    prepared = read(prepared_path)
    if prepared['dataset_acceptance']['publication_sha256'] != acceptance_sha:
        raise ValueError('The final release acceptance proof differs')
    if checksum(local / 'HOLD') != prepared['hold_sha256']:
        raise ValueError('Data producer hold changed')
    jobs = set()
    files = 0
    for group in prepared['groups']:
        name = Path(group['name'])
        if name.parent != Path('.'):
            raise ValueError('Noncanonical retirement payload path')
        path = request_dir / name
        if checksum(path) != group['sha256']:
            raise ValueError(f'Payload SHA differs for {name}')
        payload = read(path)
        certificate = payload['source_certificate']
        proof = payload['destination_proof']
        if (payload['id'] != name.stem or Path(certificate['root']) != local / 'results'
                or payload['dataset_acceptance'] != prepared['dataset_acceptance']
                or proof['dataset_acceptance'] != prepared['dataset_acceptance']
                or len(certificate['files']) != group['files']):
            raise ValueError(f'Retirement batch identity differs for {name}')
        _proof(certificate, proof, digest(proof))
        group_jobs = {v['job_id'] for v in payload['jobs']}
        if jobs & group_jobs or len(group_jobs) != len(payload['jobs']):
            raise ValueError('Repeated Data job in retirement manifests')
        jobs.update(group_jobs)
        files += len(certificate['files'])
    if (len(jobs), files, prepared['bytes']) != (10840, 118683, 150204330486):
        raise ValueError('Retirement plan differs from the verified Data duplicate scope')
    return config, prepared, jobs


def producer_held(config, expected_sha):
    local = Path(config['local_root'])
    state = subprocess.run(
        ['systemctl', 'show', 'dsc-aave-v3-hf-flow-local.service', '-p', 'ActiveState', '--value'],
        capture_output=True, text=True, check=True).stdout.strip()
    if state != 'inactive' or checksum(local / 'HOLD') != expected_sha:
        raise ValueError('Data producer is active or its hold changed')


def apply(request_dir, prepared_sha, acceptance_sha, execute):
    config, prepared, jobs = prepared_groups(request_dir, prepared_sha, acceptance_sha)
    producer_held(config, prepared['hold_sha256'])
    planned = dict(files=prepared['files'], bytes=prepared['bytes'], jobs=len(jobs))
    if not execute:
        print(json.dumps(dict(mode='preflight', **planned)), flush=True)
        return
    local = Path(config['local_root'])
    output = Path(config['output_root'])
    receipts = []
    for group in prepared['groups']:
        producer_held(config, prepared['hold_sha256'])
        path = request_dir / group['name']
        if checksum(path) != group['sha256']:
            raise ValueError(f'Retirement payload changed: {path}')
        payload = read(path)
        receipt = local / 'transfers' / payload['id'] / 'retired.json'
        if not receipt.exists():
            retire_local(config, payload)
        result = read(receipt)
        if (not result['source_copies_retired'] or
                any((output / name).exists() for name in payload['source_certificate']['files'])):
            raise ValueError(f'Retirement did not finish: {payload["id"]}')
        receipts.append(dict(name=payload['id'], sha256=checksum(receipt), result=result))
        print(json.dumps(dict(batch=payload['id'], files=group['files'], verified_absent=True)),
              flush=True)
    with read_snapshot(local / 'local.sqlite') as ledger:
        retired = {row[0] for row in ledger.execute('select job_id from retired')}
    if not jobs <= retired:
        raise ValueError('Source ledger is missing accepted retirement records')
    write_json(request_dir / 'complete.json', dict(
        **planned, all_certified_source_files_absent=True, all_source_jobs_recorded=True,
        dataset_acceptance_sha256=acceptance_sha, prepared_manifest_sha256=prepared_sha,
        receipts=receipts))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--requests', type=Path, required=True)
    parser.add_argument('--prepared-sha256', required=True)
    parser.add_argument('--acceptance-sha256', required=True)
    parser.add_argument('--apply', action='store_true')
    a = parser.parse_args()
    apply(a.requests, a.prepared_sha256, a.acceptance_sha256, a.apply)
