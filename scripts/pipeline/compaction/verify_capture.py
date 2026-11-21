"""Verify a materialized compaction capture against its catalog and payload manifest."""
import argparse
import json
from pathlib import Path

from pipeline.compaction.reader import snapshot
from shared.artifacts import checksum


def manifest_identity(manifest, manifest_sha256):
    return dict(through=manifest['through'], manifest_sha256=manifest_sha256,
                catalog_sha256=manifest['catalog_sha256'], files=len(manifest['files']),
                payload_bytes=sum(row['bytes'] for row in manifest['files']))


def verify_capture(root):
    root = Path(root).resolve()
    manifest_file = root / 'manifest.json'
    manifest = json.loads(manifest_file.read_text())
    if manifest['version'] != 1:
        raise ValueError('Unsupported capture manifest')
    if checksum(root / 'catalog.sqlite') != manifest['catalog_sha256']:
        raise ValueError('Captured catalog checksum mismatch')

    entries = {row['path']: row for row in manifest['files']}
    if len(entries) != len(manifest['files']):
        raise ValueError('Duplicate capture payload')
    with snapshot(root) as rows:
        if len(rows) != len(entries) or {row['path'] for row in rows} != entries.keys():
            raise ValueError('Capture manifest and catalog contain different payloads')
        for row in rows:
            path = Path(row.pop('absolute_path'))
            relative = Path(row['path'])
            entry = entries[row['path']]
            if any(entry.get(key) != value for key, value in row.items()):
                raise ValueError(f'Capture manifest differs from catalog: {relative}')
            if relative.is_absolute() or '..' in relative.parts or path.resolve() != path:
                raise ValueError(f'Invalid capture path: {relative}')
            if path.stat().st_size != entry['bytes'] or checksum(path) != row['sha256']:
                raise ValueError(f'Capture payload checksum mismatch: {relative}')

    return manifest_identity(manifest, checksum(manifest_file))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    args = parser.parse_args()
    print(json.dumps(verify_capture(args.root), sort_keys=True))


if __name__ == '__main__':
    main()
