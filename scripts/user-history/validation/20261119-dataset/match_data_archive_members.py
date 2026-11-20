"""Bind held Data files to Node archive members before any retirement."""
import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path

from hf_flow_cloud import _safe_member
from shared.artifacts import checksum, write_json


def match(root, primary, preflight, output, final_batch):
    catalog = root / '01_HF_Vectors/CANONICAL_JOBS.csv'
    snapshot = json.loads((root / '04_Validation_and_Provenance/job-catalog-snapshot.json').read_text())
    if checksum(catalog) != snapshot['catalog_sha256']:
        raise ValueError('Canonical catalog changed')
    with catalog.open(newline='') as stream:
        assignments = {row['job_id']:row['canonical_cloud_batch'] or final_batch
                       for row in csv.DictReader(stream)}
    inventory = json.loads((preflight / 'prepared.json').read_text())
    expected = defaultdict(dict)
    for group in inventory['batches']:
        path = preflight / Path(group['path']).name
        if checksum(path) != group['sha256']:
            raise ValueError(f'Data preflight changed: {path.name}')
        original = json.loads(path.read_text())
        allowed = set(original['job_ids'])
        for name, part in original['source_certificate']['files'].items():
            member = Path(name)
            job = member.parent.name
            if job not in allowed or member.parent.parent.name != 'jobs':
                raise ValueError(f'File outside Data job inventory: {name}')
            key = (job, member.name)
            target = expected[assignments[job]]
            if key in target:
                raise ValueError(f'Duplicate Data artifact: {name}')
            target[key] = (original['id'], name, part)

    sources = {}
    for batch, parts in expected.items():
        directory = primary / batch
        index_path = directory / 'pack-index.json'
        if index_path.exists():
            index_sha = checksum(index_path)
            accepted = snapshot['accepted_cloud_reports'].get(batch)
            if accepted is None:
                if batch != final_batch:
                    raise ValueError('Unexpected pending batch')
            elif index_sha != accepted['index_sha256']:
                raise ValueError(f'Archive index identity differs: {batch}')
            index = json.loads(index_path.read_text())
            if index['all_member_hashes_exact'] is not True:
                raise ValueError(f'Archive members are not verified: {batch}')
            entries = index['members']
        else:
            proof = json.loads((directory / '.primary-restore/complete.json').read_text())
            manifest_path = directory / proof['manifest_relative']
            if not proof['all_local_sha256_exact'] or checksum(manifest_path) != proof['manifest_sha256']:
                raise ValueError(f'Raw copy identity differs: {batch}')
            index = None
            index_sha = proof['manifest_sha256']
            entries = json.loads(manifest_path.read_text())['files']
        matches = {}
        for name, observed in entries.items():
            path = Path(name)
            key = (path.parent.name, path.name)
            if path.parent.parent.name != 'jobs' or key not in parts:
                continue
            group, source, declared = parts[key]
            if key in matches or (observed['bytes'], observed['sha256']) != (declared['bytes'], declared['sha256']):
                raise ValueError(f'Different archive bytes: {name}')
            if index is not None:
                packed = _safe_member(index, name, declared['sha256'])
                package = directory / packed['pack']
                if package.stat().st_size != index['packs'][packed['pack']]['bytes']:
                    raise ValueError(f'Truncated local package: {package}')
            elif checksum(directory / name) != declared['sha256']:
                raise ValueError(f'Raw Node file changed: {name}')
            matches[key] = dict(group=group, source=source, member=name, **observed)
        if len(matches) != len(parts):
            raise ValueError(f'Archive {batch} is missing {len(parts)-len(matches)} Data members')
        path = output / (batch+'.json')
        write_json(path, dict(batch=batch, archive_index_or_manifest_sha256=index_sha,
                              matched_files=list(matches.values())))
        sources[batch] = dict(files=len(matches), evidence_sha256=checksum(path), index_or_manifest_sha256=index_sha)
    count = sum(r['files'] for r in sources.values())
    if count != inventory['files']:
        raise ValueError('Unaccounted Data files')
    write_json(output / 'node-archive-member-match.json', dict(
        all_data_files_located_in_node_archives=True, files=count, bytes=inventory['bytes'],
        preflight_sha256=checksum(preflight / 'prepared.json'), archives=sources,
        pending_cloud_batch=final_batch, files_retired=0))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    for name in ('root', 'primary', 'preflight', 'output'):
        parser.add_argument('--'+name, type=Path, required=True)
    parser.add_argument('--final-batch', required=True)
    a = parser.parse_args()
    a.output.mkdir(parents=True, exist_ok=True)
    match(a.root, a.primary, a.preflight, a.output, a.final_batch)
