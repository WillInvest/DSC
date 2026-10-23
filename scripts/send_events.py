#!/usr/bin/env python3
"""Send a completed pool extraction from dsc-node directly to dsc-data over SSH."""
import argparse
import fcntl
import hashlib
import json
from pathlib import Path
import shlex
import subprocess

DESTINATION = 'dsc-data@100.79.189.8'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--destination-dir', required=True, help='absolute dedicated directory on dsc-data')
    args = parser.parse_args()
    if not args.destination_dir.startswith('/') or '\n' in args.destination_dir:
        parser.error('destination must be an absolute path without newlines')
    source = args.source.resolve()
    with (source / '.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        manifest = json.loads((source / 'manifest.json').read_text())
        files = []
        for chunk in manifest['chunks']:
            name = chunk['file']
            if Path(name).name != name or '\n' in name:
                raise ValueError('invalid chunk filename')
            if hashlib.sha256((source / name).read_bytes()).hexdigest() != chunk['sha256']:
                raise ValueError(f'checksum mismatch: {name}')
            files.append(name)
        subprocess.run(['ssh', '-o', 'BatchMode=yes', DESTINATION,
                        'mkdir -p -- ' + shlex.quote(args.destination_dir)], check=True)
        target = DESTINATION + ':' + args.destination_dir.rstrip('/') + '/'
        common = ['rsync', '-rtcs', '-e', 'ssh -o BatchMode=yes']
        # Publish the manifest after every referenced data file has arrived.
        subprocess.run(common + ['--files-from=-', str(source) + '/', target],
                       input=''.join(name + '\n' for name in files).encode(), check=True)
        subprocess.run(common + [str(source / 'manifest.json'), target], check=True)
        print(f'Transferred {len(files)} chunks and manifest to {target}')


if __name__ == '__main__':
    main()
