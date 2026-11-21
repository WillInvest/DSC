"""Capture a migration inventory from a live canonical compaction catalog."""
import argparse
from contextlib import closing
from pathlib import Path
import shutil
import sqlite3
from tempfile import TemporaryDirectory

from pipeline.compaction.reader import snapshot
from shared.artifacts import checksum, write_json


def capture_catalog(source, destination):
    """Back up the ledger and hold open payloads that compaction can replace."""
    source = Path(source).resolve()
    destination = Path(destination).resolve()
    if destination.is_relative_to(source):
        raise ValueError('Capture destination must be separate from the live store')
    if destination.exists():
        raise FileExistsError(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)

    with TemporaryDirectory(prefix='.capture-', dir=destination.parent) as temporary:
        capture = Path(temporary) / 'inventory'
        capture.mkdir()
        ledger = capture / 'catalog.sqlite'
        files = []
        with snapshot(source) as rows:
            # The snapshot lock excludes compaction for both the ledger and payload copies.
            uri = (source / 'catalog.sqlite').as_uri() + '?mode=ro'
            with closing(sqlite3.connect(uri, uri=True)) as live, closing(sqlite3.connect(ledger)) as saved:
                live.backup(saved)
                through = saved.execute("select value from meta where key='through'").fetchone()
                if through is None:
                    raise ValueError('Source catalog has no accepted compaction position')
                if saved.execute('pragma quick_check').fetchall() != [('ok',)]:
                    raise ValueError('Catalog snapshot failed integrity check')

            for row in rows:
                path = Path(row['absolute_path'])
                relative_path = Path(row['path'])
                if relative_path.is_absolute() or '..' in relative_path.parts or path.resolve() != path:
                    raise ValueError(f'Invalid catalog payload path: {relative_path}')
                entry = {key: value for key, value in row.items() if key != 'absolute_path'}
                entry['bytes'] = path.stat().st_size
                if not entry['sealed']:
                    pinned = Path('open') / relative_path
                    (capture / pinned).parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(path, capture / pinned)
                    entry['captured_path'] = str(pinned)
                files.append(entry)

        for entry in files:
            if 'captured_path' in entry and checksum(capture / entry['captured_path']) != entry['sha256']:
                raise ValueError(f"Open payload checksum mismatch: {entry['path']}")

        manifest = {
            'version': 1,
            'source_root': str(source),
            'through': int(through[0]),
            'catalog_sha256': checksum(ledger),
            'files': files,
        }
        write_json(capture / 'manifest.json', manifest)
        capture.rename(destination)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', type=Path)
    parser.add_argument('destination', type=Path)
    args = parser.parse_args()
    manifest = capture_catalog(args.source, args.destination)
    files = manifest['files']
    print(f"Captured through block {manifest['through']}: {len(files)} files, "
          f"{sum('captured_path' in file for file in files)} open payloads")


if __name__ == '__main__':
    main()
