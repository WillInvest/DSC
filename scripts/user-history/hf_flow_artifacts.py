"""Immutable HF file certificates and exact, recoverable replica retirement.

A certificate avoids reopening/decompressing unchanged accepted results at
startup. It is not a new numerical acceptance result. Destination proofs are
pinned by the authoritative caller; arbitrary untrusted JSON is not authority.
"""
import fcntl
import json
import os
import re
import time
import uuid
from pathlib import Path

from consolidate_hf import contained, identity
from shared.artifacts import checksum, digest, write_json

FORMAT = 'hf-flow-artifact-certificate-v1'
PROOF_FORMAT = 'hf-flow-destination-proof-v1'


def _root(root):
    root = Path(root)
    if root.is_symlink() or not root.is_dir():
        raise ValueError('Expected an existing real artifact root')
    return root.resolve()


def _path(root, relative):
    relative = Path(relative)
    if relative.is_absolute() or '..' in relative.parts or not relative.parts:
        raise ValueError('Invalid artifact-relative path')
    if '\n' in str(relative) or '\r' in str(relative):
        raise ValueError('Invalid artifact-relative path')
    path = contained(root / relative, root)
    for parent in [path, *path.parents]:
        if parent == root: break
        if parent.is_symlink(): raise ValueError('Symlink in artifact path')
    return path


def _identity(path):
    value = identity(path)
    stat = path.lstat()
    value.update(ctime_ns=stat.st_ctime_ns, nlink=stat.st_nlink)
    if value['nlink'] != 1: raise ValueError('Unaccounted artifact hardlinks')
    return value


def _files(files):
    normalized = {}
    for name, item in files.items():
        if not isinstance(name, str) or str(Path(name)) != name:
            raise ValueError('Artifact path is not canonical')
        size, sha = item['bytes'], item['sha256']
        if not isinstance(size, int) or isinstance(size, bool) or size < 0:
            raise ValueError('Invalid artifact size')
        if not isinstance(sha, str) or re.fullmatch('[a-f0-9]{64}', sha) is None:
            raise ValueError('Invalid artifact SHA256')
        normalized[name] = dict(bytes=size, sha256=sha)
    if not normalized: raise ValueError('Empty artifact certificate')
    return normalized


def _publish(path, value):
    """Reuse durable JSON writing, with an exclusive final publication."""
    path = Path(path)
    if path.exists(): raise FileExistsError(path)
    incoming = path.with_name(path.name + '.incoming-' + uuid.uuid4().hex)
    write_json(incoming, value)
    os.link(incoming, path)
    incoming.unlink()
    fd = os.open(path.parent, os.O_RDONLY)
    try: os.fsync(fd)
    finally: os.close(fd)


def seal(root, files, output_path=None):
    """Read every declared file once; bind its digest and stable inode identity."""
    root = _root(root); files = _files(files); identities = {}
    for name, item in files.items():
        path = _path(root, name); before = _identity(path)
        if before['bytes'] != item['bytes'] or checksum(path) != item['sha256']:
            raise ValueError('Artifact bytes differ from accepted manifest')
        if _identity(path) != before: raise ValueError('Artifact changed during sealing')
        identities[name] = before
    value = dict(format=FORMAT, root=str(root), files=files,
                 manifest_sha256=digest(files), identities=identities, sealed_at=time.time())
    if output_path is not None: _publish(output_path, value)
    return value


def _certificate(value):
    files = _files(value['files'])
    if value.get('format') != FORMAT or value.get('manifest_sha256') != digest(files):
        raise ValueError('Artifact certificate identity differs')
    if set(value.get('identities', {})) != set(files):
        raise ValueError('Artifact certificate covers different files')
    root = Path(value['root'])
    if not root.is_absolute() or '..' in root.parts or str(root) != value['root']:
        raise ValueError('Artifact root is not a canonical absolute path')
    return root, files


def fast_verify(certificate):
    """Check the exact sealed inode identity; never hash or decode data here."""
    root, files = _certificate(certificate)
    root = _root(root)
    for name in files:
        if _identity(_path(root, name)) != certificate['identities'][name]:
            raise ValueError('Sealed artifact identity changed')
    return True


def combine(root, certificates, output_path=None):
    """Group unchanged sealed files under one ancestor, without reading data.

    Each input is an already-pinned local certificate. The caller persists and
    pins the returned certificate before batch transfer/retirement, allowing a
    single root descriptor scan for the whole closed batch.
    """
    root = _root(root); files = {}; identities = {}; parents = []
    for certificate in certificates:
        fast_verify(certificate)
        source, members = _certificate(certificate)
        for name, item in members.items():
            path = _path(source, name)
            try: relative = str(path.relative_to(root))
            except ValueError: raise ValueError('Combined member is outside batch root') from None
            if relative in files: raise ValueError('Duplicate combined artifact')
            files[relative] = item
            identities[relative] = certificate['identities'][name]
        parents.append(digest(certificate))
    if not files: raise ValueError('Empty combined artifact certificate')
    value = dict(format=FORMAT, root=str(root), files=files,
                 manifest_sha256=digest(files), identities=identities,
                 sealed_at=time.time(), combined_from=parents)
    if output_path is not None: _publish(output_path, value)
    return value


def verify_destination(certificate, destination_root, output_path=None, *, destination_host=None, verified_certificates=None):
    """Independently read a copied manifest at its destination."""
    source, files = _certificate(certificate)
    if destination_host is not None and (not isinstance(destination_host, str) or not destination_host):
        raise ValueError('Expected an explicit remote destination host')
    if _root(destination_root) == source and destination_host is None:
        raise ValueError('Destination must be an independent copy')
    destination = seal(destination_root, files) if verified_certificates is None else combine(destination_root, verified_certificates)
    if destination['files'] != files:
        raise ValueError('Verified destination certificates cover different bytes')
    value = dict(format=PROOF_FORMAT, source_certificate_sha256=digest(certificate),
                 manifest_sha256=digest(files), files=files, all_exact=True,
                 destination=destination['root'], verification='sha256_copy',
                 destination_certificate=destination, verified_at=time.time())
    if destination_host is not None:
        value.update(verification='remote_sha256_copy', destination_host=destination_host,
                     checked_at=value['verified_at'])
    if output_path is not None: _publish(output_path, value)
    return value


def _proof(certificate, proof, proof_sha256):
    _, files = _certificate(certificate)
    if digest(proof) != proof_sha256 or proof.get('format') != PROOF_FORMAT:
        raise ValueError('Pinned destination proof identity differs')
    if (proof.get('all_exact') is not True
            or proof.get('manifest_sha256') != digest(files)
            or proof.get('source_certificate_sha256') != digest(certificate)
            or proof.get('files') != files or not proof.get('destination')):
        raise ValueError('Independent destination proof is incomplete')
    if proof.get('verification') == 'sha256_copy':
        other = proof['destination_certificate']
        if (other['files'] != files or other['root'] != proof['destination']
                or _root(other['root']) == _root(certificate['root'])):
            raise ValueError('Destination certificate differs from verified manifest')
        fast_verify(other)
    elif proof.get('verification') == 'remote_sha256_copy':
        other = proof['destination_certificate']
        _, copied = _certificate(other)
        if (copied != files or other['root'] != proof['destination']
                or not isinstance(proof.get('destination_host'), str)
                or not proof['destination_host'] or not isinstance(proof.get('checked_at'), (int, float))
                or proof['checked_at'] != proof.get('verified_at')):
            raise ValueError('Independent remote SHA proof is incomplete')
        # The authoritative caller obtained this pinned certificate from the
        # destination's SHA readback. Its paths are not mounted on this host.
    elif proof.get('verification') == 'rclone_size_md5_sha256_restore':
        probes = proof.get('restore_probes', [])
        if (proof.get('all_remote_size_md5_exact') is not True or not probes
                or ':' not in proof['destination']):
            raise ValueError('Cloud checksum/readback proof is incomplete')
        if any(p.get('exact') is not True or p.get('path') not in files
               or p.get('sha256') != files[p['path']]['sha256'] for p in probes):
            raise ValueError('Cloud restore probes differ from the declared files')
    else: raise ValueError('Unsupported independent destination proof')


def _open_inodes():
    """Inspect all Linux descriptors; permission failures are never skipped."""
    if os.geteuid() != 0 or not Path('/proc/self/fd').is_dir():
        raise PermissionError('Retirement requires root Linux descriptor inspection')
    found = set()
    for proc in Path('/proc').iterdir():
        if not proc.name.isdigit(): continue
        try:
            for fd in (proc / 'fd').iterdir():
                try:
                    stat = fd.stat(); found.add((stat.st_dev, stat.st_ino))
                except FileNotFoundError: pass
        except FileNotFoundError: pass
    return found


def _append(stream, value):
    stream.write(json.dumps(value, sort_keys=True) + '\n')
    stream.flush(); os.fsync(stream.fileno())


def _append_many(stream, values):
    stream.writelines(json.dumps(value, sort_keys=True) + '\n' for value in values)
    stream.flush(); os.fsync(stream.fileno())


def retire(root, certificate, durable_delete_log, verified_destination_proof, proof_sha256):
    """Retire only declared replicas; accepted inputs and metadata stay outside.

    The caller must own this closed batch and prevent new consumers while this
    runs. The certificate/proof must be pinned in its durable authority before
    invoking the privileged helper. Missing members permit crash recovery after
    an unlink; node/cloud proof is still required on every resumed invocation.
    """
    root = _root(root); source, files = _certificate(certificate)
    if root != source: raise ValueError('Retirement root differs from certificate')
    _proof(certificate, verified_destination_proof, proof_sha256)
    certificate_sha256 = verified_destination_proof['source_certificate_sha256']
    log = Path(durable_delete_log); log.parent.mkdir(parents=True, exist_ok=True)
    if log.is_relative_to(root) and str(log.relative_to(root)) in files:
        raise ValueError('Recovery log cannot be a retirement object')
    with log.with_name(log.name + '.lock').open('a') as guard:
        fcntl.flock(guard, fcntl.LOCK_EX | fcntl.LOCK_NB)
        opened = _open_inodes(); remaining = []; missing = 0
        # Preflight the entire batch before deleting its first remaining member.
        for name, item in files.items():
            path = _path(root, name)
            if not path.exists(): missing += 1; continue
            ident = _identity(path)
            if ident != certificate['identities'][name]:
                raise ValueError('Retirement object changed since sealing')
            if (ident['dev'], ident['ino']) in opened:
                raise ValueError('Retirement object is open')
            if checksum(path) != item['sha256'] or _identity(path) != ident:
                raise ValueError('Retirement bytes changed')
            remaining.append((name, path, ident))
        removed = logical = 0
        with log.open('a') as stream:
            # A group has a durable complete intent set before its first unlink.
            for offset in range(0, len(remaining), 128):
                group = remaining[offset:offset + 128]
                details = []
                for name, path, ident in group:
                    if _identity(path) != ident:
                        raise ValueError('Retirement object changed before unlink')
                    details.append(dict(path=str(path), sha256=files[name]['sha256'],
                                        destination=verified_destination_proof['destination'],
                                        certificate_sha256=certificate_sha256,
                                        proof_sha256=proof_sha256))
                _append_many(stream, [dict(d, action='unlink_intent', at=time.time()) for d in details])
                for name, path, ident in group:
                    if _identity(path) != ident:
                        raise ValueError('Retirement object changed before unlink')
                    path.unlink()
                for parent in sorted({path.parent for _, path, _ in group}):
                    fd = os.open(parent, os.O_RDONLY)
                    try: os.fsync(fd)
                    finally: os.close(fd)
                _append_many(stream, [dict(d, action='unlinked', at=time.time()) for d in details])
                removed += len(group)
                logical += sum(files[name]['bytes'] for name, _, _ in group)
        return dict(removed_files=removed, already_absent_files=missing,
                    retired_logical_bytes=logical, certificate_sha256=certificate_sha256,
                    destination_proof_sha256=proof_sha256, completed_at=time.time())
