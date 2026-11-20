"""Manifest-only rsync staging over existing SSH; node still accepts exact bytes."""
import json
import os
import shlex
import subprocess
from pathlib import Path

from hf_flow_queue import read_snapshot
from hf_flow_retire import job_locks
from hf_flow_artifacts import fast_verify
from shared.artifacts import digest


def manifest_entries(value, source_root):
    """Return only original receipt-declared files, rooted beneath frozen output."""
    root = Path(source_root).resolve()
    receipt = value['receipt']
    entries = [(p['path'], Path(p['path']).name, p['sha256'])
               for p in receipt['parts'] + receipt['aggregate_parts']]
    entries += [(receipt['validation'], 'validation.json', receipt['validation_sha256']),
                (value['path'], 'origin-receipt.json', value['sha256'])]
    result = []
    for source, target_name, sha in entries:
        path = Path(source)
        relative = path.relative_to(root)
        if '..' in relative.parts or path.resolve() != path.absolute():
            raise ValueError('Noncanonical or escaping rsync source')
        if '\n' in str(relative) or '\r' in str(relative):
            raise ValueError('Invalid rsync relative path')
        result.append((str(relative), target_name, sha))
    return result


def stage_source(config, pins, batch, needed):
    root = Path(config['local_root'])
    if len(batch) != 24 or any(c not in '0123456789abcdef' for c in batch):
        raise ValueError('Invalid staged batch id')
    known = {}; directories = []; certificates = []
    with read_snapshot(root / 'local.sqlite') as db:
        for key, sha in pins:
            row = db.execute('select value from done where job_id=?', (key,)).fetchone()
            if row is None: raise ValueError('Unaccepted rsync job')
            value = json.loads(row[0])
            if value['sha256'] != sha: raise ValueError('Rsync receipt pin differs')
            if value['certificate'] is not None: certificates.append(value['certificate'])
            directories.append(Path(value['path']).parent)
            for relative, _, expected in manifest_entries(value, config['output_root']):
                if relative in known and known[relative] != expected:
                    raise ValueError('Rsync source conflict')
                known[relative] = expected
    if not needed or any(known.get(name) != sha for name, sha in needed.items()):
        raise ValueError('Rsync request outside accepted receipt scope')
    transport = config['rsync_transport']
    ops = root / 'transfers' / batch; ops.mkdir(parents=True, exist_ok=True)
    filelist = ops / 'rsync-files.bin'
    filelist.write_bytes(b''.join(name.encode() + b'\0' for name in sorted(needed)))
    destination = transport.get('remote_base', '').rstrip('/') + '/' + batch + '/'
    if not transport.get('remote_base'): destination = batch + '/'
    with job_locks(directories, exclusive=False):
        for certificate in certificates: fast_verify(certificate)
        for name in needed:
            p = Path(config['output_root']) / name
            if p.is_symlink() or not p.is_file(): raise ValueError('Nonregular staged source')
        subprocess.run(['rsync', '-a', '--ignore-existing', '--from0',
                        '--files-from=' + str(filelist), '--partial',
                        '--partial-dir=.rsync-partial', '--timeout=300',
                        '-e', transport.get('ssh_command', 'ssh -oBatchMode=yes -oConnectTimeout=15'),
                        str(config['output_root']).rstrip('/') + '/',
                        transport['destination'] + ':' + destination], check=True)
    return dict(batch=batch, manifest_sha256=digest(needed), files=len(needed),
                verification='transport_only_node_sha_required')
