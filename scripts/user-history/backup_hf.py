"""Snapshot explicit accepted HF receipts; stream immutable files over existing SSH.

Run prepare/verify on a server, or lan on the operator computer. No private
keys, live SQLite files or unaccepted Parquet files enter this backup.
"""
import argparse
import hashlib
import json
import shlex
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

HOMES = {'node': '/home/node', 'revert': '/home/dsc-revert', 'web': '/home/fao'}
HOSTS = {'node': 'node@100.108.171.14', 'revert': 'dsc-revert@100.91.215.76', 'web': 'fao@100.119.128.31'}
IPS = {'node': '10.246.103.160', 'revert': '10.246.103.26', 'web': '10.246.103.143'}
IMAGE = '1d1a9be72b345989a4a8667573e929c66320535824f480517e6e440633d0e32d'
WORK = 'aave-hf-distributed/5197dae89b140ccb'
VIRTUAL = Path('/home/dsc-data/data/aave-user-history-v3/26407881/accounting/hf-numeric')


def checksum(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda: f.read(2**20), b''):
            h.update(block)
    return h.hexdigest()


def read(path):
    return json.loads(Path(path).read_text())


def active_manifest(base):
    base = Path(base)
    pointer = base / 'active-plan.json'
    if not pointer.exists():
        return base / 'plan.json'
    value = read(pointer); path = Path(value['path'])
    path.resolve().relative_to(base.resolve())
    if checksum(path) != value['sha256'] or read(path)['generation'] != value['generation']:
        raise ValueError('Active assignment pointer identity differs')
    return path


def owned_receipt(base, home, path):
    path = Path(path)
    if path.is_relative_to('/home/dsc-data/worker-state'):
        return Path(home) / WORK / 'state' / path.relative_to('/home/dsc-data/worker-state')
    path.resolve().relative_to((Path(base) / 'state/receipts').resolve())
    return path


def prepare(role, stamp):
    home = Path(HOMES[role]); base = home / 'hf-failover-20261115'
    plan_path = active_manifest(base); plan = read(plan_path); generation = plan['generation']
    checkpoint = base / 'backup-checkpoints' / stamp
    checkpoint.mkdir(parents=True, exist_ok=False)
    files = {}

    def add(path, expected=None):
        path = Path(path)
        relative = path.resolve().relative_to(home.resolve())
        if path.is_symlink() or not path.is_file():
            raise ValueError('Not a regular backup file: ' + str(path))
        identity = checksum(path)
        if expected is not None and identity != expected:
            raise ValueError('Source identity differs: ' + str(path))
        files[str(relative)] = {'sha256': identity, 'bytes': path.stat().st_size}

    def physical(path):
        return home / WORK / 'outputs' / Path(path).relative_to(VIRTUAL)

    def receipt(path, expected=None):
        path = Path(path); r = read(path)
        if r.get('all_exact') is not True or r.get('mismatches') != 0:
            raise ValueError('Unvalidated source receipt')
        if r['factor_generation'] != plan['identity']['factor_generation']:
            raise ValueError('Factor generation differs')
        target = checkpoint / 'receipts' / r['generation'] / path.name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target); add(target, expected or checksum(path))
        out = physical(r['output']); add(out, r['sha256'])
        if out.stat().st_size != r['bytes']:
            raise ValueError('Output size differs')
        add(physical(r['validation']), r['validation_sha256'])
        add(out.parent / 'output.json')
        return r['rows']

    sources = list((base / 'state' / 'receipts' / generation).glob('*.json'))
    new_rows = 0
    for p in sources:
        r = read(p)
        if r['generation'] != generation or plan['assignments'].get(r['job_id']) != role:
            raise ValueError('Receipt assignment differs')
        new_rows += receipt(p)
    recovered_rows = 0; recovered_jobs = 0
    for item in plan['recovered'].values():
        if item['owner'] != role:
            continue
        replica = next(r for r in item['replicas'] if r['role'] == role)
        old_path = owned_receipt(base, home, replica['path'])
        recovered_rows += receipt(old_path, replica['receipt_sha256'])
        recovered_jobs += 1
    metadata = [base / name for name in ['plan.json', 'active-plan.json', 'FAILOVER.md', 'REJOIN.md', 'backup_hf.py',
                                       'private-shm.conf', 'state/pilot.json', 'state/progress.json']]
    metadata.append(plan_path)
    metadata += list((plan_path.parent / 'code').glob('*.py'))
    metadata += list((base / 'source-archives').glob('*/code/*.py'))
    metadata += list(plan_path.parent.glob('*.conf'))
    metadata += list(base.glob('*.service.proposed'))
    metadata += list((base / 'code').glob('*.py')) + list((base / 'inventories').glob('*.json'))
    for p in metadata:
        if not p.is_file():
            continue
        target = checkpoint / 'metadata' / p.relative_to(base)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(p, target); add(target)
    manifest = dict(role=role, created_at=datetime.now(timezone.utc).isoformat(), generation=generation,
                    plan_sha256=checksum(plan_path), input_image_sha256=IMAGE,
                    new_jobs=len(sources), new_rows=new_rows, recovered_jobs=recovered_jobs,
                    recovered_rows=recovered_rows, files=files,
                    scope='Only explicit accepted receipts and immutable outputs; frozen input image has three separate verified replicas, not duplicated in this archive.')
    manifest_path = checkpoint / 'manifest.json'
    manifest_path.write_text(json.dumps(manifest, indent=2) + '\n')
    return dict(role=role, checkpoint=str(checkpoint), manifest=str(manifest_path.relative_to(home)),
                manifest_sha256=checksum(manifest_path),
                bytes=sum(v['bytes'] for v in files.values()), files=len(files), new_rows=new_rows,
                recovered_rows=recovered_rows)


def verify(root, relative, expected, *, report_path=None, max_workers=4):
    root = Path(root); manifest_path = root / relative
    if checksum(manifest_path) != expected:
        raise ValueError('Backup manifest differs')
    manifest = read(manifest_path)
    def check(entry):
        name,item=entry
        p = root / name
        p.resolve().relative_to(root.resolve())
        if p.is_symlink() or not p.is_file() or p.stat().st_size != item['bytes'] or checksum(p) != item['sha256']:
            raise ValueError('Backup differs: ' + name)
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for _ in pool.map(check,manifest['files'].items()): pass
    report = dict(verified_at=datetime.now(timezone.utc).isoformat(), role=manifest['role'],
                  manifest_sha256=expected, files=len(manifest['files']), all_exact=True,
                  new_rows=manifest['new_rows'], recovered_rows=manifest['recovered_rows'])
    from shared.artifacts import write_json
    write_json(Path(report_path) if report_path is not None else root/'backup-verified.json', report)
    return report


def needed(root, payload):
    """Incremental transfer list; an existing conflicting file is never replaced."""
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    manifest = payload['manifest']; names = []; size = 0
    entries = dict(manifest['files'])
    entries[payload['relative']] = {'sha256': payload['sha256'], 'bytes': payload['manifest_bytes']}
    for name, item in entries.items():
        p = root / name
        p.resolve().relative_to(root.resolve())
        if p.exists():
            if p.is_symlink() or not p.is_file() or p.stat().st_size != item['bytes'] or checksum(p) != item['sha256']:
                raise ValueError('Existing backup conflicts; preserved: ' + str(p))
        else:
            names.append(name); size += item['bytes']
    if shutil.disk_usage(root).free - size < 50 * 2**30:
        raise ValueError('Backup capacity reserve would be exceeded')
    return dict(names=names, transfer_bytes=size)


def remote(host, command, **kwargs):
    return subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', host, command], check=True, **kwargs)


def publish(role, stamp, destination):
    """Temporary authenticated, read-only rsync module containing only manifest files."""
    import os
    import secrets
    home = Path(HOMES[role]); checkpoint = home / 'hf-failover-20261115/backup-checkpoints' / stamp
    if not checkpoint.exists():
        prepare(role, stamp)
    manifest_path = checkpoint / 'manifest.json'; manifest = read(manifest_path)
    if manifest['role'] != role or manifest['plan_sha256'] != checksum(active_manifest(home / 'hf-failover-20261115')):
        raise ValueError('Checkpoint role or assignment changed; use a new checkpoint')
    relative = str(manifest_path.relative_to(home)); stage = checkpoint / 'publish'
    stage.mkdir(exist_ok=True)
    for name in list(manifest['files']) + [relative]:
        source = home / name; target = stage / name
        source.resolve().relative_to(home.resolve())
        if source.is_symlink():
            raise ValueError('Symlink cannot be published')
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            if target.is_symlink() or not target.samefile(source):
                raise ValueError('Existing staged file differs; preserved: ' + str(target))
        else:
            os.link(source, target)
    password = secrets.token_hex(32)
    secret = checkpoint / 'rsync-secret'
    secret.write_text('hfbackup:' + password + '\n'); secret.chmod(0o600)
    config = checkpoint / 'rsync.conf'
    config.write_text('use chroot = no\nread only = yes\nlist = no\n[accepted]\npath = ' + str(stage)
                      + '\nhosts allow = ' + IPS[destination] + '\nhosts deny = *\nauth users = hfbackup\nsecrets file = ' + str(secret) + '\n')
    unit = 'dsc-aave-hf-checkpoint-rsync'
    subprocess.run(['sudo', 'systemd-run', '--quiet', '--collect', '--unit=' + unit,
                    '--property=User=' + HOSTS[role].split('@')[0], '--property=MemoryMax=256M',
                    '--property=CPUQuota=100%', '--property=RuntimeMaxSec=3600']
                   + (['/usr/bin/flock','--nonblock',str(home/'dex-compaction-writer.lock')] if role=='web' else [])
                   + ['/usr/bin/rsync', '--daemon', '--no-detach', '--port=18880',
                    '--address=' + IPS[role], '--config=' + str(config)], check=True)
    return dict(manifest=manifest, relative=relative, sha256=checksum(manifest_path),
                manifest_bytes=manifest_path.stat().st_size, password=password, source=role,
                source_ip=IPS[role], unit=unit)


def receive(destination, payload):
    home = Path(HOMES[destination]); role = payload['source']
    target = home / 'hf-backups/lan-20261116' / role
    missing = needed(target, payload)
    private = home / ('hf-failover-20261115/backup-password-' + role)
    filelist = home / ('hf-failover-20261115/backup-missing-' + role)
    private.write_text(payload['password'] + '\n'); private.chmod(0o600)
    filelist.write_bytes(b''.join(name.encode() + b'\0' for name in sorted(missing['names'])))
    try:
        subprocess.run(['rsync', '-a', '--ignore-existing', '--from0', '--files-from=' + str(filelist),
                        '--partial', '--partial-dir=.rsync-partial',
                        '--contimeout=20', '--timeout=300', '--password-file=' + str(private),
                        'rsync://hfbackup@' + payload['source_ip'] + ':18880/accepted/', str(target) + '/'], check=True)
    finally:
        private.unlink(missing_ok=True)
        filelist.unlink(missing_ok=True)
    result = verify(target, payload['relative'], payload['sha256'])
    result.update(source=role, destination=destination, destination_root=str(target),
                  transfer_bytes=missing['transfer_bytes'], transferred_files=len(missing['names']))
    return result


def cleanup(role, stamp):
    """Clean only our temporary source, including an already collected unit."""
    secret = Path(HOMES[role]) / 'hf-failover-20261115/backup-checkpoints' / stamp / 'rsync-secret'
    unit = 'dsc-aave-hf-checkpoint-rsync'
    try:
        state = subprocess.run(['systemctl', 'show', unit, '-p', 'LoadState', '--value'],
                               capture_output=True, text=True)
        if state.stdout.strip() != 'not-found':
            state.check_returncode()
            subprocess.run(['sudo', 'systemctl', 'stop', unit], check=True)
    finally:
        secret.unlink(missing_ok=True)
    return dict(source=role, secret_removed=True)


def lan(role, destination, stamp, evidence):
    if role == destination:
        raise ValueError('Backup must be on another host')
    for r in [role, destination]:
        subprocess.run(['scp', '-q', str(Path(__file__).resolve()), HOSTS[r] + ':hf-failover-20261115/backup_hf.py'], check=True)
    payload = json.loads(remote(HOSTS[role], shlex.join(['python3', HOMES[role] + '/hf-failover-20261115/backup_hf.py', 'publish', role, stamp, destination]), capture_output=True, text=True).stdout)
    try:
        result = json.loads(remote(HOSTS[destination], shlex.join(['python3', HOMES[destination] + '/hf-failover-20261115/backup_hf.py', 'receive', destination]), input=json.dumps(payload), capture_output=True, text=True).stdout)
    except subprocess.CalledProcessError as error:
        failure = Path(evidence).with_suffix('.failure.json')
        failure.parent.mkdir(parents=True, exist_ok=True)
        redact = lambda value: (value or '').replace(payload['password'], '<REDACTED>')[-16000:]
        failure.write_text(json.dumps(dict(source=role, destination=destination,
                           returncode=error.returncode, stderr=redact(error.stderr),
                           stdout=redact(error.stdout)), indent=2) + '\n')
        raise
    finally:
        remote(HOSTS[role], shlex.join(['python3', HOMES[role] + '/hf-failover-20261115/backup_hf.py',
                                     'cleanup', role, stamp]))
    path = Path(evidence); path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, indent=2) + '\n')
    if read(path) != result:
        raise ValueError('Backup evidence readback differs')
    return result


def main():
    p = argparse.ArgumentParser(); sub = p.add_subparsers(dest='action', required=True)
    a = sub.add_parser('prepare'); a.add_argument('role', choices=HOMES); a.add_argument('stamp')
    a = sub.add_parser('verify'); a.add_argument('root'); a.add_argument('manifest'); a.add_argument('sha256')
    a = sub.add_parser('needed'); a.add_argument('root')
    a = sub.add_parser('publish'); a.add_argument('role', choices=HOMES); a.add_argument('stamp'); a.add_argument('destination', choices=HOMES)
    a = sub.add_parser('cleanup'); a.add_argument('role', choices=HOMES); a.add_argument('stamp')
    a = sub.add_parser('receive'); a.add_argument('destination', choices=HOMES)
    a = sub.add_parser('lan'); a.add_argument('role', choices=HOMES); a.add_argument('destination', choices=HOMES); a.add_argument('stamp'); a.add_argument('--evidence', required=True)
    a = p.parse_args()
    if a.action == 'prepare': result = prepare(a.role, a.stamp)
    elif a.action == 'verify': result = verify(a.root, a.manifest, a.sha256)
    elif a.action == 'needed':
        import sys
        result = needed(a.root, json.load(sys.stdin))
    elif a.action == 'publish': result = publish(a.role, a.stamp, a.destination)
    elif a.action == 'cleanup': result = cleanup(a.role, a.stamp)
    elif a.action == 'receive':
        import sys
        result = receive(a.destination, json.load(sys.stdin))
    elif a.action == 'lan': result = lan(a.role, a.destination, a.stamp, a.evidence)
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
