"""List or recover one SHA-verified member from an indexed DSC TAR archive."""
import argparse
import fnmatch
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory


def restore(folder, member, output, index_sha256):
    folder = Path(folder)
    raw = (folder/'pack-index.json').read_bytes()
    if hashlib.sha256(raw).hexdigest() != index_sha256:
        raise ValueError('Archive index differs from the published catalog')
    index = json.loads(raw)
    item = index['members'][member]
    relative = Path(item['pack'])
    if relative.is_absolute() or '..' in relative.parts:
        raise ValueError('Invalid package location')
    package = folder/relative
    if not package.resolve().is_relative_to(folder.resolve()):
        raise ValueError('Package escaped archive folder')
    offset, size = item['offset'], item['bytes']
    if (type(offset) is not int or type(size) is not int or offset < 0 or size < 0
            or offset + size > package.stat().st_size):
        raise ValueError('Member range differs from pinned index')
    output = Path(output)
    if output.exists():
        raise FileExistsError(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    sha = hashlib.sha256()
    with TemporaryDirectory(dir=output.parent) as directory:
        candidate = Path(directory)/'member'
        with package.open('rb') as source, candidate.open('xb') as target:
            source.seek(offset)
            remaining = size
            while remaining:
                data = source.read(min(remaining, 2**20))
                if not data:
                    raise ValueError('Archived package ended inside the member range')
                sha.update(data)
                target.write(data)
                remaining -= len(data)
        if sha.hexdigest() != item['sha256']:
            raise ValueError('Restored member failed SHA256')
        output.hardlink_to(candidate)
    return dict(output=str(output), bytes=size, sha256=sha.hexdigest(), exact=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--folder', type=Path, required=True)
    parser.add_argument('--pattern', default='*')
    parser.add_argument('--limit', type=int, default=20)
    parser.add_argument('--member')
    parser.add_argument('--output', type=Path)
    parser.add_argument('--index-sha256')
    args = parser.parse_args()
    if args.member:
        if not args.output or not args.index_sha256:
            parser.error('Restoration requires --output and --index-sha256 from DATA_CATALOG')
        print(json.dumps(restore(args.folder, args.member, args.output, args.index_sha256)))
    else:
        index = json.loads((args.folder/'pack-index.json').read_text())
        selected = [name for name in index['members'] if fnmatch.fnmatchcase(name, args.pattern)]
        for name in selected[:args.limit]:
            print(json.dumps(dict(member=name, **index['members'][name])))
        print(f'{len(selected)} matching members; listed at most {args.limit}.')
