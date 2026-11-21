"""Place completed, Drive-verified HF windows under the canonical research root."""
import argparse
import fcntl
from pathlib import Path
from tempfile import TemporaryDirectory

from pipeline.compaction.collect import read
from shared.artifacts import checksum, write_json

ARCHIVE_FORMAT = 'dsc-hf-window-packed-drive-v1'


def member(root, name):
    relative = Path(name)
    if not relative.parts or relative.is_absolute() or '..' in relative.parts or str(relative) != name:
        raise ValueError(f'Invalid HF archive member: {name}')
    path = root/relative
    if path.resolve() != path or not path.is_file():
        raise ValueError(f'Absent or redirected HF archive member: {path}')
    return path


def place(window, archive, output, start, stop):
    manifest_path = archive/'manifest.json'
    index_path = archive/'pack-index.json'
    receipt_path = archive/'verified.json'
    manifest, receipt, status = read(manifest_path), read(receipt_path), read(window/'progress.json')
    span = (start, stop)
    for evidence in (manifest, receipt, status):
        if (evidence['start'], evidence['stop']) != span:
            raise ValueError('HF window and verification range differ')
    if (manifest['format'] != ARCHIVE_FORMAT or receipt['format'] != ARCHIVE_FORMAT
            or receipt['full_drive_restore_exact'] is not True or status['stage'] != 'locally_verified'
            or checksum(manifest_path) != receipt['manifest_sha256']
            or checksum(index_path) != receipt['index_sha256']
            or status['result']['sha256'] != manifest['result_sha256']
            or status['successor_sha256'] != manifest['successor_sha256']):
        raise ValueError('HF window lacks matching Node and Drive acceptance')

    files = {}
    for name, identity in manifest['files'].items():
        path = member(window, name)
        if path.stat().st_size != identity['bytes']:
            raise ValueError(f'HF archive member size differs: {path}')
        files[name] = path
    for alias, original in manifest['derived_files'].items():
        path = member(window, alias)
        if alias in files or checksum(path) != manifest['files'][original]['sha256']:
            raise ValueError(f'HF derived member differs: {path}')
        files[alias] = path
    metadata = {'progress.json': window/'progress.json', 'archive/manifest.json': manifest_path,
                'archive/pack-index.json': index_path, 'archive/verified.json': receipt_path}
    if files.keys() & metadata.keys():
        raise ValueError('HF archive uses a reserved metadata path')
    files.update(metadata)

    if output.exists():
        if any(not (output/name).samefile(path) for name, path in files.items()):
            raise ValueError(f'Existing canonical HF window differs: {output}')
    else:
        with TemporaryDirectory(dir=output.parent) as scratch:
            stage = Path(scratch)/'publish'
            for name, path in files.items():
                target = stage/name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.hardlink_to(path)
            stage.rename(output)
    return dict(window=output.name, start=start, stop=stop, files=len(files),
                manifest_sha256=receipt['manifest_sha256'], receipt_sha256=checksum(receipt_path))


def publish(windows, backup, target):
    if windows.stat().st_dev != target.stat().st_dev or backup.stat().st_dev != target.stat().st_dev:
        raise ValueError('HF placement requires the accepted files and target on the same disk')
    cursor = read(backup/'progress.json')
    if str(windows.resolve()) != str(Path(cursor['identity']['windows']).resolve()):
        # The bind-mount and physical paths can name the same directory.
        if not windows.samefile(cursor['identity']['windows']):
            raise ValueError('Drive cursor belongs to a different source window directory')
    root = target/'windows'
    root.mkdir(parents=True, exist_ok=True)
    accepted = cursor['next_block']
    start = cursor['identity']['start']
    opening = start
    reports = []
    with (target/'placement.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        ranges = {}
        for archive in backup.iterdir():
            path = archive/'verified.json'
            if path.is_file():
                evidence = read(path)
                lower, upper = evidence['start'], evidence['stop']
                if archive.name != f'{lower}-{upper-1}' or lower in ranges:
                    raise ValueError('Invalid or overlapping HF verified archive names')
                ranges[lower] = (upper, archive)
        while start < accepted:
            if start not in ranges:
                raise ValueError(f'Drive-verified HF coverage has a gap starting at {start}')
            stop, archive = ranges[start]
            if not start < stop <= accepted:
                raise ValueError('HF range crosses the accepted Drive watermark')
            reports.append(place(windows/archive.name, archive, root/archive.name, start, stop))
            start = stop
        result = dict(start=opening, through=accepted-1, windows=reports)
        write_json(target/'window-placement.json', result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('windows', type=Path)
    parser.add_argument('backup', type=Path)
    parser.add_argument('target', type=Path)
    args = parser.parse_args()
    report = publish(args.windows.resolve(), args.backup.resolve(), args.target.resolve())
    print(f"Placed {len(report['windows'])} Drive-verified HF windows through {report['through']}")


if __name__ == '__main__':
    main()
