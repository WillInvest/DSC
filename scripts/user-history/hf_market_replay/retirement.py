"""Verify exact old HF payloads against Drive before manifest-scoped retirement.

Reconstruction inputs and small research metadata are outside the payload set.
The existing privileged retirement helper remains the only deletion path.
"""
import argparse
import csv
import json
import re
import subprocess
from pathlib import Path

OLD_GENERATION = 'db2fce32bfc0a227f5d527ffee45c8212d1da0a939ec26447be07ada39af3efa'
DATASET = Path('/home/node/aave-research-dataset')
PRIMARY = Path('/home/node/hf-flow-20261116/storage/primary-archives')
OPERATIONS = Path('/home/node/hf-flow-20261116/cloud')


def load(path):
    return json.loads(Path(path).read_text())


def selected_payload(directory, report):
    """Read accepted manifests and select only final result payloads."""
    from shared.artifacts import checksum

    if report.get('archive_format') == 'hf-flow-tar-index-v1':
        index_path = directory / 'pack-index.json'
        manifest_path = directory / 'manifest.json'
        if checksum(index_path) != report['index_sha256']:
            raise ValueError('Accepted archive index changed')
        if checksum(manifest_path) != report['manifest_sha256']:
            raise ValueError('Accepted archive manifest changed')
        files = load(index_path)['packs']
        if any(re.fullmatch(r'packs/pack-\d{4}\.tar', name) is None for name in files):
            raise ValueError('Unexpected old HF package path')
        checks = sorted(files) + ['pack-index.json', 'manifest.json']
        probes = [sorted(files)[0]]
    else:
        restore = load(directory / '.primary-restore/complete.json')
        manifest_name = restore['manifest_relative']
        manifest_path = directory / manifest_name
        expected = (report['chunks'][0]['source_sha256_manifest']
                    if 'chunks' in report else report['manifest_sha256'])
        if checksum(manifest_path) != expected:
            raise ValueError('Accepted raw archive manifest changed')
        manifest_files = load(manifest_path)['files']
        files = {name: item for name, item in manifest_files.items()
                 if '/' + OLD_GENERATION + '/jobs/' in name
                 and re.fullmatch(r'(parts|aggregate_parts)-\d{4}\.parquet', Path(name).name)}
        if not files:
            raise ValueError('No final result payload in raw archive')
        checks = sorted(manifest_files) + [manifest_name]
        probes = []
        for prefix in ('parts-', 'aggregate_parts-'):
            name = next((name for name in sorted(files) if Path(name).name.startswith(prefix)), None)
            if name is None:
                raise ValueError('Missing wallet or aggregate restore probe')
            probes.append(name)
    return files, checks, probes


def verify(dataset=DATASET, output=None):
    """Run current remote checksum checks, local full SHA, and SHA readbacks."""
    from hf_flow_cloud import _common, _readback
    from hf_flow_artifacts import PROOF_FORMAT, fast_verify, seal
    from shared.artifacts import checksum, digest, write_json

    dataset = Path(dataset)
    snapshot_path = dataset / '04_Validation_and_Provenance/job-catalog-snapshot.json'
    snapshot = load(snapshot_path)
    with (dataset / 'DATA_CATALOG.csv').open(newline='') as handle:
        archives = [row for row in csv.DictReader(handle)
                    if row['component'].startswith('HF archive ')]
    if len(archives) != 6:
        raise ValueError('Original canonical HF archive scope changed')
    outcomes = []
    for row in archives:
        directory = Path(row['node_path'])
        if directory.parent != PRIMARY or directory.is_symlink():
            raise ValueError('Archive outside exact old primary roots')
        batch = directory.name
        report = snapshot['accepted_cloud_reports'][batch]
        files, checks, probes = selected_payload(directory, report)
        operation = OPERATIONS / batch / 'cohort-retirement-20261120'
        operation.mkdir(parents=True, exist_ok=True)
        proof_path = operation / 'proof.json'
        certificate_path = operation / 'certificate.json'
        if proof_path.exists():
            proof = load(proof_path)
            certificate = load(certificate_path)
            if (proof['source_certificate_sha256'] != digest(certificate)
                    or proof['accepted_report_sha256'] != digest(report)
                    or proof['destination'] != row['drive_path']):
                raise ValueError('Retained verification identity changed')
            fast_verify(certificate)
            outcomes.append(dict(batch=batch, certificate=str(certificate_path),
                                 proof=str(proof_path), proof_sha256=checksum(proof_path)))
            continue
        listing = operation / 'files-to-check.txt'
        listing.write_text('\n'.join(checks) + '\n')
        with (operation / 'rclone.log').open('a') as log:
            subprocess.run(_common() + [
                'check', str(directory), row['drive_path'], '--files-from', str(listing),
                '--one-way', '--checkers', '8', '--combined', str(operation / 'check.txt'),
            ], check=True, stdout=log, stderr=log, timeout=86400)
        if certificate_path.exists():
            certificate = load(certificate_path)
            fast_verify(certificate)
        else:
            certificate = seal(directory, files, output_path=certificate_path)
        readbacks = []
        for name in probes:
            checked = _readback(row['drive_path'] + '/' + name, files[name])
            readbacks.append(dict(path=name, **checked))
        proof = dict(
            format=PROOF_FORMAT,
            source_certificate_sha256=digest(certificate),
            manifest_sha256=digest(certificate['files']), files=certificate['files'],
            all_exact=True, destination=row['drive_path'],
            verification='rclone_size_md5_sha256_restore',
            all_remote_size_md5_exact=True, restore_probes=readbacks,
            generation=OLD_GENERATION, accepted_report_sha256=digest(report),
            catalog_snapshot_sha256=checksum(snapshot_path),
            current_check_sha256=checksum(operation / 'check.txt'),
        )
        write_json(proof_path, proof)
        outcomes.append(dict(batch=batch, certificate=str(certificate_path),
                             proof=str(proof_path), proof_sha256=checksum(proof_path)))
    result = dict(generation=OLD_GENERATION, archives=outcomes, archive_count=len(outcomes),
                  payload_files=sum(len(load(o['certificate'])['files']) for o in outcomes),
                  payload_bytes=sum(sum(v['bytes'] for v in load(o['certificate'])['files'].values())
                                    for o in outcomes), files_retired=0)
    if output is not None:
        write_json(output, result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    print(json.dumps(verify(output=Path(args.output))))


if __name__ == '__main__':
    main()
