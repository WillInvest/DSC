"""Publish and independently restore a packed delta from a Drive-restored catalog."""
import argparse
import fcntl
import gzip
import json
from pathlib import Path
import tarfile
from tempfile import TemporaryDirectory

from pipeline.compaction.backup_catalog import METADATA, descriptor, fetch, read
from pipeline.compaction.delta_capture import build_plan, materialize, prepare
from pipeline.compaction.verify_capture import verify_capture
from shared.artifacts import checksum, write_json

FORMAT = 'dsc-catalog-import-delta-v1'
PACK = 'delta.tar.gz'
RECEIPT = 'verified.json'


def write_pack(current, plan, path):
    inputs = {name: current/name for name in METADATA}
    for name, task in plan['tasks'].items():
        if task['source'] != 'reuse':
            if task['path'] in inputs:
                raise ValueError('Duplicate catalog delta member')
            inputs[task['path']] = current/name
    with TemporaryDirectory(dir=path.parent) as directory:
        staged = Path(directory)/PACK
        with (staged.open('wb') as raw,
              gzip.GzipFile(fileobj=raw, mode='wb', filename='', mtime=0, compresslevel=3) as compressed,
              tarfile.open(fileobj=compressed, mode='w|') as archive):
            for name, source in sorted(inputs.items()):
                member = tarfile.TarInfo(name)
                member.size = source.stat().st_size
                with source.open('rb') as stream:
                    archive.addfile(member, stream)
        staged.replace(path)


def restore_pack(previous, receipt, work, output):
    from hf_flow_cloud import _readback

    if receipt['format'] != FORMAT or checksum(previous/'manifest.json') != receipt['previous_manifest_sha256']:
        raise ValueError('Wrong Drive baseline for the catalog delta')
    with TemporaryDirectory(prefix='delta-restore-', dir=work) as directory:
        stage = Path(directory)
        packed = stage/PACK
        _readback(receipt['remote']+'/'+PACK, receipt['package'], output=packed)
        extracted = stage/'members'
        extracted.mkdir()
        with tarfile.open(packed, mode='r:gz') as archive:
            members = archive.getmembers()
            names = {member.name for member in members}
            if len(names) != len(members) or any(not member.isfile() for member in members):
                raise ValueError('Unexpected catalog delta archive member')
            archive.extractall(extracted, members=members, filter='data')
        plan = prepare(previous, extracted, stage/'plan')
        expected = {*METADATA, *(task['path'] for task in plan['tasks'].values() if task['source'] != 'reuse')}
        if names != expected or plan['capture_manifest_sha256'] != receipt['catalog']['manifest_sha256']:
            raise ValueError('Catalog delta members or manifest differ from the receipt')
        restored = materialize(previous, extracted, extracted, stage/'plan', output)
        if restored != receipt['catalog']:
            raise ValueError('Restored delta catalog differs from the accepted import')
        return restored


def backup(previous, current, base_remote, drive, work):
    from hf_flow_cloud import _copy_and_check, _readback

    work.mkdir(parents=True, exist_ok=True)
    with (work/'backup.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        current_proof = verify_capture(current)
        plan = build_plan(previous, current)
        remote = f"{drive.rstrip('/')}/{plan['through']}-{plan['capture_manifest_sha256']}"
        identity = dict(format=FORMAT, remote=remote, base_remote=base_remote,
                        previous_manifest_sha256=plan['previous_manifest_sha256'], catalog=current_proof)
        path = work/RECEIPT
        pending = work/'pending.json'
        pack = work/PACK
        if path.exists():
            saved = read(path)
            if any(saved[key] != value for key, value in identity.items()):
                raise ValueError('Existing Drive verification has a different delta identity')
            return saved
        if pending.exists():
            receipt = read(pending)
            if receipt != dict(identity, package=descriptor(pack)):
                raise ValueError('Pending Drive delta has a different identity')
        else:
            write_pack(current, plan, pack)
            receipt = dict(identity, package=descriptor(pack))
            write_json(pending, receipt)
        _copy_and_check(work, remote, [PACK], work)
        with TemporaryDirectory(prefix='verify-', dir=work) as directory:
            stage = Path(directory)
            restore_pack(previous, receipt, stage, stage/'verified-catalog')
            candidate = stage/RECEIPT
            write_json(candidate, receipt)
            _copy_and_check(stage, remote, [RECEIPT], stage)
            _readback(remote+'/'+RECEIPT, descriptor(candidate))
            write_json(path, receipt)
        return receipt


def restore(previous, remote, receipt_sha256, output):
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        raise FileExistsError(output)
    with TemporaryDirectory(prefix='catalog-delta-', dir=output.parent) as directory:
        work = Path(directory)
        receipt_path = work/RECEIPT
        fetch(remote+'/'+RECEIPT, receipt_path, command='copyto')
        if checksum(receipt_path) != receipt_sha256:
            raise ValueError('Drive delta receipt SHA-256 differs')
        receipt = read(receipt_path)
        if receipt['remote'] != remote:
            raise ValueError('Drive delta receipt points to a different location')
        return restore_pack(previous, receipt, work, output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    publish = commands.add_parser('backup')
    publish.add_argument('previous', type=Path, help='Catalog independently restored from Drive')
    publish.add_argument('current', type=Path, help='Accepted Node import')
    publish.add_argument('base_remote', help='Drive location of the baseline bundle')
    publish.add_argument('drive', help='Drive destination for the new delta')
    publish.add_argument('work', type=Path)
    recover = commands.add_parser('restore')
    recover.add_argument('previous', type=Path, help='Catalog independently restored from Drive')
    recover.add_argument('remote')
    recover.add_argument('receipt_sha256')
    recover.add_argument('output', type=Path)
    args = parser.parse_args()
    if args.command == 'backup':
        proof = backup(args.previous, args.current, args.base_remote, args.drive, args.work)
        result = dict(proof, receipt_sha256=checksum(args.work/RECEIPT))
    else:
        result = restore(args.previous, args.remote, args.receipt_sha256, args.output)
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
