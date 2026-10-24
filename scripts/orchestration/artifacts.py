"""Checksummed batch receipts shared by workers and the accepting coordinator."""
import hashlib
import json
import os
from pathlib import Path


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def checksum(path):
    value = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            value.update(chunk)
    return value.hexdigest()


def write_json(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + '.tmp')
    with temp.open('w') as stream:
        json.dump(data, stream, sort_keys=True)
        stream.flush()
        os.fsync(stream.fileno())
    temp.replace(path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def files(root):
    return sorted(p for p in Path(root).rglob('*') if p.is_file() and
                  (p.suffix == '.parquet' or p.name.endswith('.json.gz') or p.name in ('deployment.json', 'extraction.json')))


def seal(root, spec, headers):
    root = Path(root)
    for p in files(root):
        fd = os.open(p, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    receipt = {'spec': spec, 'headers': headers,
               'files': [{'path': str(p.relative_to(root)), 'sha256': checksum(p), 'bytes': p.stat().st_size} for p in files(root)]}
    write_json(root / 'coverage.json', receipt)
    return receipt


def verify(root, spec=None, headers=None):
    root = Path(root)
    receipt = json.loads((root / 'coverage.json').read_text())
    if spec is not None and receipt['spec'] != spec:
        raise ValueError('artifact configuration mismatch')
    if headers is not None and receipt['headers'] != headers:
        raise ValueError('artifact block hashes changed')
    expected = {entry['path'] for entry in receipt['files']}
    if expected != {str(p.relative_to(root)) for p in files(root)}:
        raise ValueError('artifact inventory mismatch')
    for entry in receipt['files']:
        p = root / entry['path']
        if p.resolve().is_relative_to(root.resolve()) is False or p.stat().st_size != entry['bytes'] or checksum(p) != entry['sha256']:
            raise ValueError('artifact checksum mismatch')
    return receipt
