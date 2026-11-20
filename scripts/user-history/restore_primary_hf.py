"""Restore a previously cloud-accepted batch as an immutable node primary copy."""
import argparse
import json
import re
import time
from pathlib import Path

from shared.artifacts import checksum, digest, write_json
from hf_flow_cloud import _copy_and_check, _readback, _safe_member
from backup_hf import verify as verify_backup

ROOT = Path('/home/node/hf-flow-20261116/storage/primary-archives')
OLD = Path('/home/node/hf-flow-20261116/cloud')


def run(batch, source=None, destination=None):
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,96}', batch):
        raise ValueError('Invalid immutable batch identity')
    source = Path(source) if source else OLD / batch
    target = Path(destination) if destination else ROOT / batch
    if not target.resolve().is_relative_to(ROOT.resolve()) or target.is_symlink():
        raise ValueError('Primary restore must stay under the node archive root')
    accepted = json.loads((source / 'complete.json').read_text())
    if (accepted.get('all_remote_size_md5_exact') is not True
            or accepted.get('archive_format') != 'hf-flow-tar-index-v1'
            or not all(p.get('exact') for p in accepted['restore_probes'] + accepted['package_restore_probes'])):
        raise ValueError('Restore requires an independently cloud-accepted packed batch')
    index_path = source / 'pack-index.json'
    if checksum(index_path) != accepted['index_sha256']:
        raise ValueError('Pinned archive index changed')
    index = json.loads(index_path.read_text())
    files = dict(index['packs'])
    for name, sha in [('pack-index.json', accepted['index_sha256']),
                      ('manifest.json', accepted['manifest_sha256'])]:
        path = source / name
        if checksum(path) != sha:
            raise ValueError('Accepted archive metadata changed')
        files[name] = dict(bytes=path.stat().st_size, sha256=sha)
    target.mkdir(parents=True, exist_ok=True)
    ops = target / '.primary-restore'
    ops.mkdir(exist_ok=True)
    identity = dict(batch=batch, source_complete_sha256=checksum(source/'complete.json'),
                    files_sha256=digest(files), index_sha256=accepted['index_sha256'])
    done = ops / 'complete.json'
    if done.exists():
        if json.loads(done.read_text())['identity'] != identity:
            raise ValueError('Completed restore identity differs')
        print('Primary restore already accepted; no repeat download', flush=True)
        return
    write_json(ops/'progress.json', dict(phase='copying', at=time.time(), identity=identity,
                                       files=len(files), bytes=sum(v['bytes'] for v in files.values())))
    # Same maintained copy/check API, reversed: cloud -> node, immutable scope.
    _copy_and_check(accepted['remote'], str(target), files, ops)
    manifest = ops/'verification-manifest.json'
    write_json(manifest, dict(role='node', files=files, new_rows=0, recovered_rows=0))
    write_json(ops/'progress.json', dict(phase='local_sha256_check', at=time.time(), identity=identity))
    verify_backup(target, str(manifest.relative_to(target)), checksum(manifest),
                  report_path=ops/'sha256-verified.json', max_workers=2)
    probes = []
    for probe in accepted['restore_probes']:
        member = _safe_member(index, probe['path'], probe['sha256'])
        result = _readback(str(target/member['pack']), member,
                           offset=member['offset'], count=member['bytes'])
        probes.append(dict(path=probe['path'], **result))
    report = dict(identity=identity, files=len(files), bytes=sum(v['bytes'] for v in files.values()),
                  primary_path=str(target), all_local_sha256_exact=True, restore_probes=probes,
                  completed_at=time.time(), production_coverage_increment=0,
                  source_cloud_unchanged=True)
    write_json(done, report)
    write_json(ops/'progress.json', dict(phase='complete', **report))
    print(json.dumps({k: report[k] for k in ['files', 'bytes', 'primary_path', 'all_local_sha256_exact']}), flush=True)


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('batch')
    p.add_argument('--source')
    p.add_argument('--destination')
    args = p.parse_args()
    run(args.batch, args.source, args.destination)
