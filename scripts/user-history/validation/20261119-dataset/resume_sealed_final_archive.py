"""Resume the fixed final Drive batch from its independently pinned Node seal."""
import argparse
import fcntl
import json
import time
from pathlib import Path

from hf_flow_artifacts import fast_verify
from hf_flow_cloud import _copy_and_check, _probe_names, _readback, _safe_member
from shared.artifacts import checksum, digest, write_json

HOME = Path('/home/node')
BATCH = '3c4e71fb0df22139098dc66d'
ROOT = HOME / 'hf-flow-20261116/storage/primary-archives' / BATCH
REMOTE = 'dsc_hf:26407881/hf-flow-20261116/batches/' + BATCH
INDEX_SHA = '1ef40aedad27ab7363772e179401a4d8205bd020b31d0ebd06d5106f0aab228d'


def read(path):
    return json.loads(path.read_text())


def validate():
    index_path = ROOT / 'pack-index.json'
    manifest_path = ROOT / 'manifest.json'
    if checksum(index_path) != INDEX_SHA:
        raise ValueError('Final index differs from independent Node reconciliation pin')
    index = read(index_path)
    if index['format'] != 'hf-flow-tar-index-v1' or index['all_member_hashes_exact'] is not True:
        raise ValueError('Archive index does not prove full member verification')
    certificate = read(ROOT / 'package-certificate.json')
    packages = {name: {k: row[k] for k in ('bytes', 'sha256')}
                for name, row in index['packs'].items()}
    if Path(certificate['root']) != ROOT or certificate['files'] != packages:
        raise ValueError('Package seal differs from the pinned index')
    fast_verify(certificate)
    manifest_sha = checksum(manifest_path)
    manifest = read(manifest_path)
    members = {name: {k: row[k] for k in ('bytes', 'sha256')}
               for name, row in manifest['files'].items()}
    members[str(manifest_path.relative_to(HOME))] = dict(
        bytes=manifest_path.stat().st_size, sha256=manifest_sha)
    if (manifest['id'] != BATCH or manifest['archive_format'] != index['format'] or
            len(manifest['job_receipts']) != 51356 or members != index['files'] or
            digest(members) != index['original_manifest_sha256']):
        raise ValueError('Archive is not the complete sealed final batch')
    return index, manifest, manifest_sha, certificate


def resume(preflight=False):
    if preflight:
        index, manifest, _, _ = validate()
        print(json.dumps(dict(job_receipts=len(manifest['job_receipts']),
                              tar_files=len(index['packs']),
                              tar_bytes=sum(x['bytes'] for x in index['packs'].values()),
                              sealed_identities_still_exact=True, index_sha256=INDEX_SHA)), flush=True)
        return
    # Use the exact same exclusive lock as upload_and_retire.
    with (ROOT / 'lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if (ROOT / 'verified.json').exists():
            raise ValueError('Existing verified receipt must be preserved')
        index, manifest, manifest_sha, certificate = validate()
        files = dict(index['packs'])
        files['pack-index.json'] = dict(bytes=(ROOT/'pack-index.json').stat().st_size, sha256=INDEX_SHA)
        files['manifest.json'] = dict(bytes=(ROOT/'manifest.json').stat().st_size, sha256=manifest_sha)
        _copy_and_check(ROOT, REMOTE, files, ROOT)
        probes = []
        for name in _probe_names(manifest['files']):
            member = _safe_member(index, name, manifest['files'][name]['sha256'])
            result = _readback(REMOTE+'/'+member['pack'], member,
                               offset=member['offset'], count=member['bytes'])
            probes.append(dict(result, path=name, pack=member['pack'], offset=member['offset']))
        first = next(iter(index['packs']))
        package_probe = dict(_readback(REMOTE+'/'+first, index['packs'][first]), path=first)
        fast_verify(certificate)
        if checksum(ROOT/'pack-index.json') != INDEX_SHA or checksum(ROOT/'manifest.json') != manifest_sha:
            raise ValueError('Archive index or manifest drifted during Drive verification')
        report = dict(id=BATCH, generation=manifest['generation'], remote=REMOTE,
                      archive_format=index['format'], manifest_sha256=manifest_sha,
                      index_sha256=INDEX_SHA, index_path=str(ROOT/'pack-index.json'),
                      verified_bytes=sum(v['bytes'] for v in files.values()),
                      logical_input_bytes=sum(v['bytes'] for v in manifest['files'].values()),
                      all_remote_size_md5_exact=True, all_local_tar_members_sha256_exact=True,
                      restore_probes=probes, package_restore_probes=[package_probe],
                      verified_at=time.time(), job_receipts=manifest['job_receipts'],
                      scope='Sealed exact vectors; unchanged local package identities, full Drive size/MD5, three member and one full TAR SHA readbacks')
        write_json(ROOT/'verified.json', report)
        print('Drive verification receipt accepted. Canonical commit remains with upload_and_retire.', flush=True)


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--preflight', action='store_true')
    resume(p.parse_args().preflight)
