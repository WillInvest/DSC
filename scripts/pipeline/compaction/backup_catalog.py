"""Back up a canonical SQLite catalog and restore its exact Parquet/JSONL payloads."""
import argparse
import fcntl
import json
from pathlib import Path
import re
import shutil
import subprocess
from tempfile import TemporaryDirectory
import time

from pipeline.compaction.capture import capture_catalog
from pipeline.compaction.verify_capture import verify_capture
from shared.artifacts import checksum, write_json

FORMAT = 'dsc-canonical-drive-snapshot-v1'
METADATA = ('manifest.json', 'catalog.sqlite')


def read(path):
    return json.loads(path.read_text())


def payload_path(value):
    path = Path(value)
    if (path.is_absolute() or not path.parts or '..' in path.parts or str(path) != value
            or path.parts[0] in (*METADATA, 'catalog.lock', 'drive-receipt.json')):
        raise ValueError('Invalid catalog payload path')
    return path


def inventory(manifest):
    entries = {}
    objects = {}
    for row in manifest['files']:
        relative = payload_path(row['path'])
        identity = row['sha256']
        if (relative in entries or not re.fullmatch(r'[a-f0-9]{64}', identity)
                or not isinstance(row['bytes'], int) or row['bytes'] < 0):
            raise ValueError('Invalid or duplicate catalog object identity')
        previous = objects.setdefault(identity, row['bytes'])
        if previous != row['bytes']:
            raise ValueError('Catalog states different sizes for one object')
        entries[relative] = row
    return entries, objects


def descriptor(path):
    return dict(bytes=path.stat().st_size, sha256=checksum(path))


def copy_objects(source, capture, entries, staging):
    objects = {}
    for relative, row in entries.items():
        identity = row['sha256']
        origin = capture/payload_path(row['captured_path']) if 'captured_path' in row else source/relative
        if origin.resolve() != origin or not origin.is_file():
            raise ValueError(f'Missing or redirected catalog payload: {relative}')
        if identity in objects:
            if descriptor(origin) != objects[identity]:
                raise ValueError(f'Catalog payload changed during backup: {relative}')
            continue
        target = staging/identity
        shutil.copyfile(origin, target)
        info = descriptor(target)
        if info != dict(bytes=row['bytes'], sha256=identity):
            raise ValueError(f'Catalog payload changed during backup: {relative}')
        objects[identity] = info
    return objects


def backup(source, work, remote):
    from hf_flow_cloud import _copy_and_check, _readback

    source = source.resolve()
    work = work.resolve()
    remote = remote.rstrip('/')
    work.mkdir(parents=True, exist_ok=True)
    with (work/'backup.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        capture = work/'capture'
        if not capture.exists():
            capture_catalog(source, capture)
        manifest = read(capture/'manifest.json')
        if manifest['source_root'] != str(source):
            raise ValueError('Pending snapshot belongs to a different Node catalog')
        metadata = {file: descriptor(capture/file) for file in METADATA}
        if metadata['catalog.sqlite']['sha256'] != manifest['catalog_sha256']:
            raise ValueError('Captured SQLite file differs from its manifest')
        snapshot_remote = f"{remote}/snapshots/{manifest['through']}-{metadata['manifest.json']['sha256']}"
        objects_remote = remote+'/objects'
        receipt = work/'drive-receipt.json'
        if receipt.exists():
            saved = read(receipt)
            if saved['snapshot_remote'] != snapshot_remote or saved['metadata'] != metadata:
                raise ValueError('Existing Drive receipt belongs to a different snapshot')
            return saved
        entries, objects = inventory(manifest)
        with TemporaryDirectory(prefix='upload-', dir=work) as directory:
            stage = Path(directory)
            if objects:
                staged = copy_objects(source, capture, entries, stage)
                _copy_and_check(stage, objects_remote, staged, stage)
            _copy_and_check(capture, snapshot_remote, METADATA, stage)
            for name, info in metadata.items():
                _readback(snapshot_remote+'/'+name, info)
            by_kind = {}
            for path, row in sorted(entries.items(), key=lambda item: (item[1]['bytes'], str(item[0]))):
                by_kind.setdefault(row['stream'].split('/')[0], (path, row))
            probes = {}
            for kind, (path, row) in by_kind.items():
                name = row['sha256']
                result = _readback(objects_remote+'/'+name, dict(bytes=row['bytes'], sha256=name))
                probes[kind] = dict(result, path=str(path))
            proof = dict(format=FORMAT, through=manifest['through'], snapshot_remote=snapshot_remote,
                         objects_remote=objects_remote, metadata=metadata, payloads=len(entries),
                         objects=len(objects), all_drive_objects_size_md5_exact=True, restore_probes=probes)
            candidate = stage/receipt.name
            write_json(candidate, proof)
            _copy_and_check(stage, snapshot_remote, [candidate.name], stage)
            _readback(snapshot_remote+'/'+candidate.name, descriptor(candidate))
            write_json(receipt, proof)
        return proof


def fetch(source, output, *options, command='copy'):
    from hf_flow_cloud import _common

    subprocess.run(_common()+[command, source, str(output), *options], check=True, timeout=86400)


def restore(snapshot_remote, receipt_sha256, output):
    from hf_flow_cloud import _readback

    output = output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        raise FileExistsError(output)
    with TemporaryDirectory(prefix='restore-', dir=output.parent) as directory:
        scratch = Path(directory)
        stage = scratch/'restore'
        stage.mkdir()
        receipt = stage/'drive-receipt.json'
        fetch(snapshot_remote+'/'+receipt.name, receipt, command='copyto')
        if checksum(receipt) != receipt_sha256:
            raise ValueError('Drive receipt SHA-256 differs from the accepted snapshot')
        proof = read(receipt)
        if (proof['format'] != FORMAT or proof['snapshot_remote'] != snapshot_remote
                or not proof['all_drive_objects_size_md5_exact']):
            raise ValueError('Drive receipt does not accept this snapshot')
        for name in METADATA:
            _readback(snapshot_remote+'/'+name, proof['metadata'][name], output=stage/name)
        manifest = read(stage/'manifest.json')
        entries, objects = inventory(manifest)
        if (manifest['through'] != proof['through'] or len(entries) != proof['payloads']
                or len(objects) != proof['objects']):
            raise ValueError('Drive receipt and catalog manifest disagree')
        if objects:
            listing = scratch/'objects.txt'
            listing.write_text(''.join(name+'\n' for name in sorted(objects)))
            downloads = scratch/'objects'
            fetch(proof['objects_remote'], downloads, '--files-from', str(listing), '--transfers', '4')
            for name, size in objects.items():
                if descriptor(downloads/name) != dict(bytes=size, sha256=name):
                    raise ValueError('Restored object differs from the verified identity')
            for relative, row in entries.items():
                target = stage/relative
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(downloads/row['sha256'], target)
        (stage/'catalog.lock').touch()
        verification = verify_capture(stage)
        stage.rename(output)
    return verification


def backup_latest(coordinator, work, remote):
    """Retry a pending snapshot before discovering the newest committed catalog."""
    canonical = coordinator.resolve()/'canonical'
    work.mkdir(parents=True, exist_ok=True)
    status = work/'progress.json'
    with (work/'follow.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if status.exists():
            progress = read(status)
            if progress['canonical_root'] != str(canonical) or progress['remote'] != remote:
                raise ValueError('Backup worker is bound to a different catalog or Drive root')
        else:
            progress = dict(canonical_root=str(canonical), remote=remote, verified_through=None,
                            pending=None, state='waiting_for_catalog')
            write_json(status, progress)
        if progress['pending'] is None:
            cursor = coordinator/'compaction.json'
            if not cursor.exists():
                return progress
            head = read(cursor)
            if head['state'] != 'complete' or Path(head['canonical_root']).resolve() != canonical:
                raise ValueError('Coordinator has no accepted Node catalog')
            if progress['verified_through'] is not None and head['through'] <= progress['verified_through']:
                return progress
            progress['pending'] = head['through']
        progress['state'] = 'backing_up'
        write_json(status, progress)
        revision = work/'snapshots'/str(progress['pending'])
        try:
            receipt = backup(canonical, revision, remote)
            if receipt['through'] < progress['pending']:
                raise ValueError('Captured revision precedes the coordinator watermark')
        except Exception:
            write_json(status, dict(progress, state='retry_pending'))
            raise
        progress.update(state='verified', pending=None, verified_through=receipt['through'],
                        snapshot_remote=receipt['snapshot_remote'],
                        receipt_sha256=checksum(revision/'drive-receipt.json'))
        write_json(status, progress)
        return progress


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    copy = commands.add_parser('backup')
    copy.add_argument('source', type=Path)
    copy.add_argument('work', type=Path)
    copy.add_argument('remote')
    recover = commands.add_parser('restore')
    recover.add_argument('snapshot')
    recover.add_argument('receipt_sha256')
    recover.add_argument('output', type=Path)
    follow = commands.add_parser('follow')
    follow.add_argument('coordinator', type=Path)
    follow.add_argument('work', type=Path)
    follow.add_argument('remote')
    follow.add_argument('--once', action='store_true')
    follow.add_argument('--until', type=int)
    args = parser.parse_args()
    if args.command == 'backup':
        result = backup(args.source, args.work, args.remote)
        result = dict(through=result['through'], payloads=result['payloads'], objects=result['objects'],
                      snapshot_remote=result['snapshot_remote'], receipt_sha256=checksum(args.work/'drive-receipt.json'))
    elif args.command == 'restore':
        result = restore(args.snapshot, args.receipt_sha256, args.output)
    else:
        while True:
            result = backup_latest(args.coordinator, args.work, args.remote)
            verified = result['verified_through']
            if args.once or (args.until is not None and verified is not None and verified >= args.until):
                break
            time.sleep(30)
    print(json.dumps(result))


if __name__ == '__main__':
    main()
