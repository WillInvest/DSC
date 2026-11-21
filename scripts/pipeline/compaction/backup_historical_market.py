"""Back up accepted Node market imports as indexed TAR and verify Drive restores."""
import argparse
import fcntl
import json
from pathlib import Path
import re
from tempfile import TemporaryDirectory
import time

from shared.artifacts import checksum, write_json


def read(path):
    return json.loads(path.read_text())


def accepted_market(root, item, plan_sha256, through):
    address = item['address']
    if not re.fullmatch(r'0x[0-9a-f]{40}', address):
        raise ValueError('Invalid accepted market address')
    source = root / address
    manifest_path = source / 'manifest.json'
    if source.is_symlink() or manifest_path.is_symlink() or checksum(manifest_path) != item['sha256']:
        raise ValueError(f'Imported market manifest differs: {address}')
    manifest = read(manifest_path)
    if (manifest['plan_sha256'] != plan_sha256 or manifest['identity']['address'] != address
            or manifest['identity']['end_block'] != through
            or not re.fullmatch(r'[a-z0-9-]+', manifest['protocol'])):
        raise ValueError(f'Imported market scope differs: {address}')
    rows = manifest['files']
    files = {row['path']: dict(bytes=row['bytes'], sha256=row['sha256']) for row in rows}
    if (len(files) != len(rows) or len(rows) != item['files'] or 'manifest.json' in files
            or sum(row['bytes'] for row in rows) != item['bytes']
            or any(not re.fullmatch(r'[a-z_]+', row['kind']) or row['kind'] in ('index', 'source_manifest')
                   for row in rows)):
        raise ValueError(f'Imported market inventory differs: {address}')
    files['manifest.json'] = dict(bytes=manifest_path.stat().st_size, sha256=item['sha256'])
    return source, manifest, files


def restore_members(manifest, index, remote, work, readback):
    """Read each artifact type and both catalog files through Drive."""
    samples = {}
    for row in sorted(manifest['files'], key=lambda item: (item['bytes'], item['path'])):
        samples.setdefault(row['kind'], row['path'])
    samples['source_manifest'] = 'manifest.json'
    probes = {}
    with TemporaryDirectory(prefix='restore-', dir=work) as directory:
        restored = Path(directory)
        path = work/'pack-index.json'
        index_info = dict(bytes=path.stat().st_size, sha256=checksum(path))
        probes['pack_index'] = readback(remote+'/pack-index.json', index_info, output=restored/'index')
        remote_index = read(restored/'index')
        if remote_index != index:
            raise ValueError('Restored index differs from the locally verified TAR index')
        for kind, name in samples.items():
            member = remote_index['members'][name]
            proof = readback(remote+'/'+member['pack'], member, offset=member['offset'],
                             count=member['bytes'], output=restored/kind)
            probes[kind] = dict(proof, member=name, pack=member['pack'])
        if read(restored/'source_manifest') != manifest:
            raise ValueError('Restored source manifest differs from Node')
    return probes


def backup_market(root, item, plan_sha256, through, directory, drive, target_bytes):
    from hf_flow_cloud import _copy_and_check, _readback
    from hf_flow_packing import pack

    source, manifest, files = accepted_market(root, item, plan_sha256, through)
    work = directory / item['address']
    work.mkdir(parents=True, exist_ok=True)
    remote = '/'.join((drive.rstrip('/'), manifest['protocol'], item['address'], item['sha256']))
    receipt_path = work/'verified.json'
    identity = dict(address=item['address'], protocol=manifest['protocol'], remote=remote,
                    source_manifest_sha256=item['sha256'], plan_sha256=plan_sha256,
                    start=manifest['identity']['start_block'], through=through)
    if receipt_path.exists():
        receipt = read(receipt_path)
        if (any(receipt[key] != value for key, value in identity.items())
                or not receipt['all_local_members_sha256_exact']
                or not receipt['all_drive_packages_size_md5_exact']
                or not all(proof['exact'] for proof in receipt['restore_probes'].values())):
            raise ValueError('Stored historical market backup receipt differs')
        return receipt

    index = pack(source, files, work, target_bytes=target_bytes)
    transfer = [*index['packs'], 'pack-index.json']
    _copy_and_check(work, remote, transfer, work)
    probes = restore_members(manifest, index, remote, work, _readback)
    receipt = dict(**identity, format='dsc-historical-market-drive-v1',
                   index_sha256=probes['pack_index']['sha256'], restore_probes=probes,
                   files=len(files), logical_payload_bytes=sum(row['bytes'] for row in files.values()),
                   all_local_members_sha256_exact=True, all_drive_packages_size_md5_exact=True,
                   packs=index['packs'])
    # The Drive catalog is visible only after package and member verification.
    # Keep the candidate under a different local name until catalog readback passes.
    catalog = work/'catalog'
    catalog.mkdir(exist_ok=True)
    candidate = catalog/receipt_path.name
    write_json(candidate, receipt)
    _copy_and_check(catalog, remote, [candidate.name], catalog)
    _readback(remote+'/'+candidate.name, dict(bytes=candidate.stat().st_size, sha256=checksum(candidate)))
    write_json(receipt_path, receipt)
    return receipt


def backup_available(root, directory, drive, target_bytes, follow=False):
    directory.mkdir(parents=True, exist_ok=True)
    with (directory/'backup.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        while True:
            migration = read(root/'progress.json')
            identity = migration['plan_sha256']
            through = migration['through']
            published = sorted(migration['published'], key=lambda item: item['bytes'])
            addresses = [item['address'] for item in published]
            if len(set(addresses)) != len(addresses) or len(addresses) > migration['total_markets']:
                raise ValueError('Imported market progress inventory differs')
            ready = {address for address in addresses if (directory/address/'verified.json').exists()}
            completed = []
            for item in sorted(published, key=lambda item: item['address'] not in ready):
                new = item['address'] not in ready
                if new:
                    write_json(directory/'progress.json', dict(state='backing_up', plan_sha256=identity,
                               verified=completed, next_address=item['address'], through=through))
                receipt = backup_market(root, item, identity, through, directory, drive, target_bytes)
                completed.append(dict(address=item['address'], remote=receipt['remote'],
                                      receipt_sha256=checksum(directory/item['address']/'verified.json')))
                if new:
                    write_json(directory/'progress.json', dict(state='backing_up', plan_sha256=identity,
                               verified=completed, through=through))
            finished = len(published) == migration['total_markets']
            state = 'complete' if finished else 'waiting_for_node_imports'
            result = dict(state=state, plan_sha256=identity, verified=completed, through=through)
            write_json(directory/'progress.json', result)
            if finished or not follow:
                return result
            time.sleep(30)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--import-root', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--remote', required=True)
    parser.add_argument('--target-bytes', type=int, default=2_000_000_000)
    parser.add_argument('--follow', action='store_true')
    args = parser.parse_args()
    report = backup_available(args.import_root, args.output, args.remote, args.target_bytes, args.follow)
    print(json.dumps(dict(state=report['state'], verified=len(report['verified']), through=report['through'])))


if __name__ == '__main__':
    main()
