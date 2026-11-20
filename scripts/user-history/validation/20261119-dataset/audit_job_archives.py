"""Reconcile the canonical job catalog against byte-bound archive receipts."""
import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path

from shared.artifacts import checksum, write_json


def read(path):
    return json.loads(path.read_text())


def check(condition, message):
    if not condition:
        raise ValueError(message)


def audit_packed(directory, cloud, restore, jobs):
    check(cloud['all_remote_size_md5_exact'] and cloud['all_local_tar_members_sha256_exact'],
          f'Packed acceptance incomplete for {directory.name}')
    check(all(p['exact'] for p in cloud['restore_probes'] + cloud['package_restore_probes']),
          f'Packed restore probe failed for {directory.name}')
    if restore:
        check(restore.get('all_local_sha256_exact') and
              restore['identity']['index_sha256'] == cloud['index_sha256'] and
              Path(restore['primary_path']) == directory,
              f'Primary restoration proof differs for {directory.name}')
    else:
        check(cloud.get('node_tar_packages_retained'), f'No physical primary copy for {directory.name}')
    index_path = directory / 'pack-index.json'
    check(checksum(index_path) == cloud['index_sha256'], f'Index hash differs for {directory.name}')
    index = read(index_path)
    for name, item in index['packs'].items():
        check((directory / name).stat().st_size == item['bytes'], f'Missing or truncated {name}')
    accepted = dict(cloud['job_receipts'])
    missing = set(jobs)
    for name, item in index['members'].items():
        if not name.endswith('/receipt.json'):
            continue
        _, parent, job, _ = name.rsplit('/', 3)
        if parent == 'jobs' and job in jobs and item['sha256'] == accepted.get(job):
            missing.discard(job)
    check(not missing, f'Canonical receipt bytes missing in {directory.name}: {sorted(missing)[:5]}')
    return set(accepted)


def audit(root, primary, output):
    catalog = root / '01_HF_Vectors/CANONICAL_JOBS.csv'
    proofs = root / '04_Validation_and_Provenance'
    snapshot = read(proofs / 'job-catalog-snapshot.json')
    check(checksum(catalog) == snapshot['catalog_sha256'], 'Catalog hash differs')
    batches = defaultdict(set)
    seen = set()
    next_block = 16291127
    observations = 0
    with catalog.open(newline='') as handle:
        for row in csv.DictReader(handle):
            job = row['job_id']
            batch = row['canonical_cloud_batch']
            start = int(row['start_block_inclusive'])
            stop = int(row['stop_block_exclusive'])
            check(job not in seen and start == next_block and stop > start, f'Bad coverage at {job}')
            check(batch and row['cloud_status'] == 'accepted', f'Cloud pending at {job}')
            seen.add(job)
            batches[batch].add(job)
            next_block = stop
            observations += int(row['logical_wallet_block_values'])
    check((len(seen), next_block, observations) == (92294, 26407882, 910310934885),
          'Canonical scope differs')
    checked = {}
    for batch, jobs in batches.items():
        directory = primary / batch
        cloud = snapshot['accepted_cloud_reports'][batch]
        restore_path = directory / '.primary-restore/complete.json'
        restore = read(restore_path) if restore_path.exists() else {}
        if cloud.get('archive_format') == 'hf-flow-tar-index-v1':
            manifest_path = directory / 'manifest.json'
            expected_sha = cloud['manifest_sha256']
            archived_jobs = audit_packed(directory, cloud, restore, jobs)
        else:
            check(restore.get('all_local_sha256_exact') and restore.get('cloud_previously_accepted'),
                  f'Raw archive not accepted for {batch}')
            manifest_path = directory / restore['manifest_relative']
            expected_sha = cloud['chunks'][0]['source_sha256_manifest'] if 'chunks' in cloud else cloud['manifest_sha256']
            check(restore['manifest_sha256'] == expected_sha, f'Raw receipt differs for {batch}')
            archived_jobs = None
        check(checksum(manifest_path) == expected_sha, f'Manifest hash differs for {batch}')
        if archived_jobs is None:
            manifest = read(manifest_path)
            archived_jobs = set()
            for name, item in manifest['files'].items():
                path = directory / name
                check(path.stat().st_size == item['bytes'] and checksum(path) == item['sha256'],
                      f'Raw contents differ for {name}')
                if path.name == 'receipt.json' and path.parent.parent.name == 'jobs':
                    archived_jobs.add(path.parent.name)
        check(jobs <= archived_jobs, f'Canonical archive missing jobs in {batch}: {jobs - archived_jobs}')
        checked[batch] = dict(canonical_jobs=len(jobs), manifest_sha256=expected_sha,
                              cloud_bytes=cloud['verified_bytes'])
    write_json(output, dict(all_canonical_jobs_have_node_and_drive_archive_proofs=True,
                            jobs=len(seen), observations=observations, batches=checked,
                            catalog_sha256=snapshot['catalog_sha256']))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--primary', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    audit(args.root, args.primary, args.output)
