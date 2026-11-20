"""Prepare ordinary HF retirement payloads for matched Data copies after release."""
import argparse
import json
from collections import defaultdict
from pathlib import Path

from hf_flow_artifacts import PROOF_FORMAT, _proof
from hf_flow_cloud import _readback
from retire_legacy_helpers import read_dataset_gate
from shared.artifacts import checksum, digest, write_json


def read(path):
    return json.loads(path.read_text())


def prepare(root, groups, members, output):
    gate = read_dataset_gate(root)
    if not gate:
        raise ValueError('Final Node and Drive delivery acceptance is pending')
    provenance = root / '04_Validation_and_Provenance'
    cloud = read(provenance / 'job-catalog-snapshot.json')['accepted_cloud_reports']
    audit = read(provenance / 'canonical-archive-audit.json')
    matched = read(members / 'node-archive-member-match.json')
    plan_path = groups / 'plan.json'
    plan = read(plan_path)
    if plan['preflight_sha256'] != matched['preflight_sha256']:
        raise ValueError('Data grouping and archive membership describe different sources')
    if not matched['all_data_files_located_in_node_archives']:
        raise ValueError('Data archive member verification is incomplete')
    evidence = {}
    for batch, info in matched['archives'].items():
        path = members / (batch+'.json')
        if (checksum(path) != info['evidence_sha256'] or batch not in audit['batches']
                or cloud[batch]['index_sha256'] != info['index_or_manifest_sha256']
                or not cloud[batch]['all_remote_size_md5_exact']):
            raise ValueError(f'Archive acceptance identity differs for {batch}')
        for row in read(path)['matched_files']:
            if row['source'] in evidence:
                raise ValueError('Data source is listed in multiple canonical archives')
            evidence[row['source']] = (batch, row)
    if len(evidence) != matched['files']:
        raise ValueError('Data membership record count differs')
    output.mkdir(mode=0o700, parents=True, exist_ok=True)
    payloads = []
    for group in plan['groups']:
        path = groups / group['path']
        if checksum(path) != group['sha256']:
            raise ValueError('Grouped Data certificate changed')
        record = read(path)
        certificate = record['certificate']
        by_archive = defaultdict(list)
        for name, expected in certificate['files'].items():
            batch, row = evidence.pop(name)
            if (row['group'] not in record['originals'] or
                    (row['bytes'], row['sha256']) != (expected['bytes'], expected['sha256'])):
                raise ValueError(f'Archived member differs for {name}')
            by_archive[batch].append(row)
        payload_path = output / path.name
        if payload_path.exists():
            saved = read(payload_path)
            if saved['dataset_acceptance'] != gate or saved['source_certificate'] != certificate:
                raise ValueError('Saved retirement payload scope differs')
            _proof(certificate, saved['destination_proof'], digest(saved['destination_proof']))
        else:
            probes = []
            for batch, rows in sorted(by_archive.items()):
                # Exact range readback from each accepted archive in this batch.
                row = min(rows, key=lambda item: (item['bytes'], item['source']))
                measured = _readback(cloud[batch]['remote']+'/'+row['pack'], row,
                                     offset=row['offset'], count=row['bytes'])
                probes.append(dict(measured, path=row['source'], archive_batch=batch, member=row['member']))
            proof = dict(format=PROOF_FORMAT, source_certificate_sha256=digest(certificate),
                         manifest_sha256=digest(certificate['files']), files=certificate['files'],
                         all_exact=True, destination='dsc_hf:AaveV3_Ethereum',
                         verification='rclone_size_md5_sha256_restore', all_remote_size_md5_exact=True,
                         restore_probes=probes, dataset_acceptance=gate,
                         archive_member_proof_sha256=checksum(members / 'node-archive-member-match.json'))
            _proof(certificate, proof, digest(proof))
            write_json(payload_path, dict(id=record['id'], jobs=[dict(job_id=j) for j in record['jobs']],
                                          all_exact=True, source_certificate=certificate,
                                          destination_proof=proof, dataset_acceptance=gate))
        payloads.append(dict(name=payload_path.name, sha256=checksum(payload_path), files=group['files']))
    if evidence or sum(x['files'] for x in payloads) != matched['files']:
        raise ValueError('Data retirement payloads do not cover the exact inventory')
    write_json(output / 'prepared.json', dict(dataset_acceptance=gate, groups=payloads,
              source_plan_sha256=checksum(plan_path), hold_sha256=plan['hold_sha256'],
              files=matched['files'], bytes=matched['bytes'], files_retired=0))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    for name in ('root', 'groups', 'members', 'output'):
        parser.add_argument('--'+name, type=Path, required=True)
    args = parser.parse_args()
    prepare(args.root, args.groups, args.members, args.output)
