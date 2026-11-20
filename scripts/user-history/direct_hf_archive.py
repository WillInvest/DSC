"""Upload manifest-listed node originals directly; never retire source files."""
import argparse
import fcntl
import hashlib
import json
import subprocess
import time
from pathlib import Path

from shared.artifacts import checksum, write_json

TOOL = '/home/node/hf-drive/tools/rclone'
CONFIG = '/home/node/.config/rclone/dsc-hf.conf'
REMOTE = 'dsc_hf:26407881/hf-vector-f64-v2'
ROOT_ID = '1o2TUruxT2HG67BHyZJTGGe0sjG8pQNMZ'


def source_files(base, files):
    base = Path(base).resolve()
    for name, item in files.items():
        relative = Path(name)
        if relative.is_absolute() or '..' in relative.parts or '\n' in name or '\r' in name:
            raise ValueError('Invalid archive-relative path')
        path = base / relative
        path.resolve().relative_to(base)
        if path.is_symlink() or not path.is_file() or path.stat().st_size != item['bytes'] or checksum(path) != item['sha256']:
            raise ValueError('Archive source identity differs')


def describe_source(payload, base):
    base = Path(base).resolve(); batch = payload['id']
    if not batch or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_' for c in batch):
        raise ValueError('Invalid batch identity')
    source_files(base, payload['files'])
    relative = 'metadata/direct-archive/' + batch + '/manifest.json'
    manifest = dict(id=batch, files=payload['files'], generation=payload['generation'], role='node', new_rows=payload['rows'], recovered_rows=0)
    path = base / relative; path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if json.loads(path.read_text()) != manifest: raise ValueError('Archive manifest conflicts')
    else: write_json(path, manifest)
    return dict(manifest=manifest, source_base=str(base), relative=relative, sha256=checksum(path), manifest_bytes=path.stat().st_size)


def upload_direct(value, tool=TOOL, config=CONFIG, remote=REMOTE, drive_root_id=ROOT_ID):
    manifest = Path(value['source_base']) / value['relative']
    with (manifest.parent / 'upload.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return _upload_direct(value, tool, config, remote, drive_root_id)


def _upload_direct(value, tool, config, remote, drive_root_id):
    base = Path(value['source_base']); batch = value['manifest']['id']
    manifest = base / value['relative']
    if checksum(manifest) != value['sha256'] or json.loads(manifest.read_text()) != value['manifest']:
        raise ValueError('Direct upload manifest changed')
    files = dict(value['manifest']['files'])
    files[value['relative']] = dict(bytes=value['manifest_bytes'], sha256=value['sha256'])
    source_files(base, files)
    probe_names = []
    for marker in ['parts-', 'aggregate_parts-', 'users.parquet']:
        name = next((n for n in files if Path(n).name.startswith(marker)), None)
        if name is None: raise ValueError('Missing user/aggregate/dictionary restore probe')
        probe_names.append(name)
    ops = manifest.parent; listing = ops / 'files.txt'; listing.write_text('\n'.join(sorted(files)) + '\n')
    destination = remote + '/batches/' + batch
    common = [str(tool), '--config', str(config), '--contimeout', '20s', '--timeout', '60s']
    if drive_root_id: common += ['--drive-root-folder-id', drive_root_id]
    subprocess.run(common + ['copy', str(base), destination, '--files-from', str(listing), '--checksum', '--immutable', '--transfers', '4', '--checkers', '8', '--drive-stop-on-upload-limit', '--log-file', str(ops / 'copy.log'), '--log-level', 'INFO'], check=True, capture_output=True, timeout=86400)
    subprocess.run(common + ['check', str(base), destination, '--files-from', str(listing), '--one-way', '--checkers', '8', '--log-file', str(ops / 'check.log'), '--log-level', 'INFO'], check=True, capture_output=True, timeout=86400)
    probes = []
    for name in probe_names:
        h = hashlib.sha256()
        with subprocess.Popen(common + ['cat', destination + '/' + name], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL) as reader:
            for data in iter(lambda: reader.stdout.read(2**20), b''): h.update(data)
            if reader.wait() or h.hexdigest() != files[name]['sha256']: raise ValueError('Drive restore hash differs')
        probes.append(dict(path=name, sha256=h.hexdigest(), exact=True))
    source_files(base, files)
    report = dict(id=batch, state='verified', verified_at=time.time(), files=len(files), verified_bytes=sum(x['bytes'] for x in files.values()), remote=destination, all_remote_size_md5_exact=True, source_sha256_manifest=value['sha256'], restore_probes=probes, node_originals_retained=True, local_staging_released_bytes=0, transport='node_direct_rclone')
    write_json(ops / 'verified.json', report)
    return report


if __name__ == '__main__':
    import hf_f64_archive as controller
    parser = argparse.ArgumentParser(); parser.add_argument('--base', required=True); parser.add_argument('--pilot', action='store_true'); args = parser.parse_args()
    config_path = Path(args.base) / 'direct-archive-config.json'
    config = json.loads(config_path.read_text())
    if checksum(Path(args.base) / 'f64-plan.json') != config['numeric_plan_sha256']:
        raise ValueError('Direct archive numeric source identity changed')
    for name, sha in config['code_sha256'].items():
        if checksum(Path(__file__).with_name(name)) != sha: raise ValueError('Direct archive code identity changed')
    controller.run(args.base, args.pilot, direct=True)
