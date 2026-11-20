"""Back up pinned legacy HF sources; never delete node originals."""
import argparse
import json
import sqlite3
import tarfile
import time
from pathlib import Path

from shared.artifacts import checksum, digest, write_json
from consolidate_hf import VIRTUAL, WORK
from hf_flow_cloud import _copy_and_check, _readback
from backup_hf import verify as verify_backup

HOME = Path('/home/node')
SOURCE = HOME / 'hf-centralized/20261116'
ROOT = HOME / 'hf-centralized/20261118-archive'
ROLES = ['revert', 'web', 'node', 'inputs']


def identity(path):
    p = Path(path)
    if p.is_symlink() or not p.is_file() or not p.resolve().is_relative_to(HOME):
        raise ValueError('Legacy archive source is not an approved node file')
    s = p.stat()
    return [s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns]


def publish_catalog(role, stage, manifest, report):
    with sqlite3.connect(ROOT/'catalog.sqlite') as db:
        db.execute('pragma journal_mode=WAL')
        db.execute('pragma synchronous=FULL')
        db.execute('create table if not exists batches(role text primary key,manifest_sha256 text,stage text,report text)')
        old = db.execute('select manifest_sha256,stage from batches where role=?', (role,)).fetchone()
        if old and old[0] != checksum(manifest):
            raise ValueError('Legacy archive catalog identity changed')
        if old and old[1] == 'cloud_accepted' and stage == 'node_checked':
            return
        db.execute('insert into batches values(?,?,?,?) on conflict(role) do update set stage=excluded.stage,report=excluded.report',
                   (role, checksum(manifest), stage, json.dumps(report, sort_keys=True)))


def prepare(role):
    out = ROOT/role
    out.mkdir(parents=True, exist_ok=True)
    manifest = out/'manifest.json'
    if manifest.exists():
        return json.loads(manifest.read_text())
    inventory = json.loads((SOURCE/'source-inventory-20261118.json').read_text())
    files = {}
    if role == 'inputs':
        proof = SOURCE/'hf-frozen-inputs.tar.gz.verified.json'
        value = json.loads(proof.read_text())
        if value.get('all_member_hashes_exact') is not True:
            raise ValueError('Frozen inputs lack verified member hashes')
        archive = SOURCE/'hf-frozen-inputs.tar.gz'
        # The scientific/runtime bundle must not carry operational credentials.
        forbidden = {'worker-state', '.ssh', '.aws', '.config'}
        secret_names = {'.env', 'backend-auth.json', 'host-key.pem', 'client-key.pem', 'dsc-hf.conf'}
        with tarfile.open(archive, 'r|gz') as reader:
            for member in reader:
                path = Path(member.name)
                if forbidden.intersection(path.parts) or path.name in secret_names:
                    raise ValueError('Frozen bundle contains an operational credential path')
        files[str(archive.relative_to(HOME))] = dict(bytes=value['archive_bytes'], sha256=value['archive_sha256'])
        files[str(proof.relative_to(HOME))] = dict(bytes=proof.stat().st_size, sha256=checksum(proof))
    else:
        for item in inventory['files']:
            if item['role'] != role:
                continue
            if not item['fully_consumed']:
                raise ValueError('Unfinished conversion still depends on legacy source')
            original = Path(item['path'])
            if original.is_relative_to(VIRTUAL):
                relative = original.relative_to(VIRTUAL)
                node = HOME/WORK/relative if role == 'node' else HOME/'hf-backups/lan-20261116'/role/WORK/relative
            elif role == 'node' and original.is_relative_to(HOME):
                node = original
            else:
                raise ValueError('Unreviewed legacy source location')
            actual_bytes = identity(node)[2]
            # The historical Node vector inventory repeated whole-job bytes
            # for each part. Preserve that evidence, bind actual per-file bytes
            # here, and still require the original pinned SHA before acceptance.
            if role != 'node' and actual_bytes != item['bytes']:
                raise ValueError('Legacy source size differs')
            files[str(node.relative_to(HOME))] = dict(bytes=actual_bytes, sha256=item['sha256'],
                inventory_bytes=item['bytes'], source_jobs=item['source_jobs'])
    if not files:
        raise ValueError('Empty legacy archive scope')
    value = dict(role=role, files=files, inventory_sha256=checksum(SOURCE/'source-inventory-20261118.json'),
                 generation=inventory['generation'], production_coverage_increment=0,
                 remote='dsc_hf:26407881/legacy-hf-source-20261118/'+role,
                 scope='Frozen legacy source/runtime backup, not new analytical HF output')
    write_json(manifest, value)
    return value


def check_node(role):
    value = prepare(role)
    out = ROOT/role
    done = out/'node-checked.json'
    if done.exists():
        report = json.loads(done.read_text())
        if report['manifest_sha256'] != checksum(out/'manifest.json'):
            raise ValueError('Node check manifest differs')
        if any(identity(HOME/n) != pin for n, pin in report['identities'].items()):
            raise ValueError('Previously verified legacy source identity drifted')
        publish_catalog(role, 'node_checked', out/'manifest.json', report)
        return
    manifest = out/'node-verification-manifest.json'
    write_json(manifest, dict(role=role, files=value['files'], new_rows=0, recovered_rows=0))
    started = time.time()
    write_json(out/'progress.json', dict(phase='node_sha256_check', started_at=started,
                                       files=len(value['files']), bytes=sum(v['bytes'] for v in value['files'].values())))
    verify_backup(HOME, str(manifest.relative_to(HOME)), checksum(manifest),
                  report_path=out/'bulk-sha256-verified.json', max_workers=2)
    pins = {n:identity(HOME/n) for n in value['files']}
    if any((HOME/n).stat().st_ctime > started for n in pins):
        raise ValueError('Legacy source changed during the bulk SHA verification')
    report = dict(role=role, all_exact=True, started_at=started, checked_at=time.time(),
                  manifest_sha256=checksum(out/'manifest.json'), identities=pins,
                  files=len(pins), bytes=sum(v['bytes'] for v in value['files'].values()))
    write_json(done, report)
    publish_catalog(role, 'node_checked', out/'manifest.json', report)
    write_json(out/'progress.json', dict(phase='node_checked', **report))
    print(json.dumps(dict(role=role, phase='node_checked', files=report['files'], bytes=report['bytes'])), flush=True)


def upload(role):
    check_node(role)
    out = ROOT/role
    value = json.loads((out/'manifest.json').read_text())
    done = out/'cloud-verified.json'
    if done.exists():
        report = json.loads(done.read_text())
        if report['manifest_sha256'] != checksum(out/'manifest.json'):
            raise ValueError('Completed cloud source manifest changed')
        publish_catalog(role, 'cloud_accepted', out/'manifest.json', report)
        return
    files = dict(value['files'])
    path = out/'manifest.json'
    files[str(path.relative_to(HOME))] = dict(bytes=path.stat().st_size, sha256=checksum(path))
    ops = out/'upload'
    ops.mkdir(exist_ok=True)
    write_json(out/'progress.json', dict(phase='uploading', at=time.time(), files=len(files), bytes=sum(v['bytes'] for v in files.values())))
    _copy_and_check(HOME, value['remote'], files, ops)
    largest = max(value['files'], key=lambda n: value['files'][n]['bytes'])
    probes = {n:_readback(value['remote']+'/'+n, files[n]) for n in [largest, str(path.relative_to(HOME))]}
    checked = json.loads((out/'node-checked.json').read_text())
    if any(identity(HOME/n) != pin for n,pin in checked['identities'].items()):
        raise ValueError('Legacy originals changed after independent node check')
    report = dict(role=role, remote=value['remote'], manifest_sha256=checksum(path),
                  all_remote_size_md5_exact=True, sha_readbacks=probes,
                  files=len(files), bytes=sum(v['bytes'] for v in files.values()),
                  node_originals_retained=True, production_coverage_increment=0, verified_at=time.time())
    write_json(done, report)
    publish_catalog(role, 'cloud_accepted', path, report)
    write_json(out/'progress.json', dict(phase='cloud_accepted', **report))
    print(json.dumps({k:report[k] for k in ['role','files','bytes','node_originals_retained']}), flush=True)


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--role', choices=ROLES)
    p.add_argument('--prepare-only', action='store_true')
    args = p.parse_args()
    ROOT.mkdir(parents=True, exist_ok=True)
    roles = [args.role] if args.role else ROLES
    for role in roles:
        prepare(role)
    if args.prepare_only:
        raise SystemExit(0)
    for role in roles:
        check_node(role)
    for role in roles:
        upload(role)
