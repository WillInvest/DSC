"""Portable, checksummed HF bundles exchanged over a restricted SSH session."""
import json
import os
from pathlib import Path
import re
import shutil
import tarfile

from shared.artifacts import checksum, digest

PROTOCOL = 'dsc-hf-worker-v1'
INPUTS = {'window.json', 'state.json.gz', 'factors.json', 'token-replay.json'}
BUNDLE = 'bundle.json'
CANDIDATE = 'candidate.json'
MAX_INPUT_BYTES = 512 * 2**20
MAX_RESULT_BYTES = 20 * 2**30


def hex_digest(value):
    return bool(re.fullmatch('[a-f0-9]{64}', value))


def artifact_name(value):
    return bool(re.fullmatch('[a-zA-Z0-9][a-zA-Z0-9_.-]{0,190}', value)) and '..' not in value


def file_identity(path):
    if path.is_symlink() or not path.is_file():
        raise ValueError('Expected a regular HF artifact')
    return dict(bytes=path.stat().st_size, sha256=checksum(path))


def input_manifest(paths):
    if set(paths) not in (INPUTS, INPUTS | {'wallets.json'}):
        raise ValueError('Unexpected HF input names')
    files = {name: file_identity(path) for name, path in paths.items()}
    if sum(item['bytes'] for item in files.values()) > MAX_INPUT_BYTES:
        raise ValueError('HF input bundle exceeds worker size limit')
    return dict(format=PROTOCOL, files=files)


def check_input(directory, scope):
    if directory.is_symlink():
        raise ValueError('Redirected HF input directory')
    marker = directory/BUNDLE
    file_identity(marker)
    manifest = json.loads(marker.read_text())
    if manifest['format'] != PROTOCOL or digest(manifest) != scope:
        raise ValueError('HF input identity differs from job scope')
    paths = {name: directory/name for name in manifest['files']}
    if input_manifest(paths) != manifest:
        raise ValueError('HF input payload verification failed')
    if {path.name for path in directory.iterdir()} != set(paths) | {BUNDLE}:
        raise ValueError('Unexpected files in the HF input bundle')
    return manifest


def result_paths(directory):
    if directory.is_symlink():
        raise ValueError('Redirected HF result directory')
    marker = directory/CANDIDATE
    file_identity(marker)
    result = json.loads(marker.read_text())
    names = [part['path'] for part in result['wallet_parts']+result['aggregates']['parts']]
    if not names or len(names) != len(set(names)) or any(
            not artifact_name(name) or not name.endswith('.parquet') for name in names):
        raise ValueError('Invalid HF candidate output names')
    expected = {part['path']: part['sha256'] for part in result['wallet_parts']+result['aggregates']['parts']}
    expected['wallets.json'] = result['wallet_dictionary_sha256']
    for file, sha in expected.items():
        if file_identity(directory/file)['sha256'] != sha:
            raise ValueError('HF candidate output checksum differs')
    return {name: directory/name for name in (*expected, CANDIDATE)}


def write_archive(stream, paths):
    with tarfile.open(fileobj=stream, mode='w|') as archive:
        for name, path in sorted(paths.items()):
            if not artifact_name(name) or path.is_symlink() or not path.is_file():
                raise ValueError('Invalid HF bundle source')
            info = archive.gettarinfo(str(path), arcname=name)
            if not info.isfile():
                raise ValueError('Invalid HF bundle source type')
            with path.open('rb') as content:
                archive.addfile(info, content)


def read_archive(stream, destination, allowed, limit):
    seen = set()
    size = 0
    with tarfile.open(fileobj=stream, mode='r|') as archive:
        for item in archive:
            if (not item.isfile() or not artifact_name(item.name) or not allowed(item.name)
                    or item.name in seen or item.size < 0):
                raise ValueError('Invalid or duplicate HF archive member')
            size += item.size
            if size > limit:
                raise ValueError('HF archive size exceeds limit')
            seen.add(item.name)
            with archive.extractfile(item) as source, (destination/item.name).open('xb') as target:
                shutil.copyfileobj(source, target)
    # Stage the whole archive first so the filesystem can group its writes.
    for name in sorted(seen):
        with (destination/name).open('rb') as target:
            os.fsync(target.fileno())
    return seen
