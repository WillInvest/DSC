"""Extend a verified Node import using only changed captured catalog payloads."""
import argparse
import errno
import json
import os
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory

from pipeline.compaction.backup_catalog import inventory, payload_path
from pipeline.compaction.verify_capture import verify_capture
from shared.artifacts import checksum, write_json


def load(path):
    return json.loads(path.read_text())


def read_capture(root):
    path = root/'manifest.json'
    manifest = load(path)
    if manifest['version'] != 1 or checksum(root/'catalog.sqlite') != manifest['catalog_sha256']:
        raise ValueError('Catalog capture metadata checksum differs')
    entries, _ = inventory(manifest)
    return manifest, entries, checksum(path)


def build_plan(previous, capture):
    prior, old_entries, prior_sha = read_capture(previous)
    current, new_entries, current_sha = read_capture(capture)
    if current['through'] < prior['through'] or current['source_root'] != prior['source_root']:
        raise ValueError('New capture must extend the same accepted catalog')
    old_by_digest = {}
    for path, item in old_entries.items():
        old_by_digest.setdefault((item['sha256'], item['bytes']), path)

    tasks = {}
    for path, item in new_entries.items():
        identity = (item['sha256'], item['bytes'])
        if path in old_entries and (old_entries[path]['sha256'], old_entries[path]['bytes']) == identity:
            source, source_path = 'reuse', path
        elif identity in old_by_digest:
            source, source_path = 'reuse', old_by_digest[identity]
        elif 'captured_path' in item:
            source, source_path = 'open', payload_path(item['captured_path'])
            if item['sealed'] or source_path != Path('open')/path:
                raise ValueError('Captured open payload has inconsistent location')
        else:
            if not item['sealed']:
                raise ValueError('Replaceable payload was not pinned under the capture lock')
            source, source_path = 'sealed', path
        tasks[str(path)] = dict(source=source, path=str(source_path), sha256=item['sha256'], bytes=item['bytes'])

    return dict(format='dsc-catalog-delta-v1', through=current['through'],
                previous_manifest_sha256=prior_sha, capture_manifest_sha256=current_sha, tasks=tasks)


def prepare(previous, capture, work):
    plan = build_plan(previous, capture)
    work.mkdir(parents=True, exist_ok=True)
    marker = work/'delta.json'
    if marker.exists() and load(marker) != plan:
        raise ValueError('Existing transfer plan differs')
    for family in ('open', 'sealed'):
        names = sorted({task['path'] for task in plan['tasks'].values() if task['source'] == family})
        if any('\n' in name or '\r' in name for name in names):
            raise ValueError('Invalid rsync filename')
        (work/(family+'.txt')).write_text(''.join(name+'\n' for name in names))
    write_json(marker, plan)
    return plan


def materialize(previous, capture, sealed, work, output):
    previous, capture, sealed, output = (path.resolve() for path in (previous, capture, sealed, output))
    if any(output.is_relative_to(root) for root in (previous, capture, sealed)):
        raise ValueError('Imported snapshot must be separate from its sources')
    roots = dict(reuse=previous, open=capture, sealed=sealed)
    plan = load(work/'delta.json')
    if plan != build_plan(previous, capture):
        raise ValueError('Transfer plan differs from the pinned capture')
    if output.exists():
        result = verify_capture(output)
        if result['manifest_sha256'] != plan['capture_manifest_sha256']:
            raise ValueError('Existing import differs from this capture')
        return result
    output.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=output.parent) as temporary:
        stage = Path(temporary)/'publish'
        stage.mkdir()
        for name in ('manifest.json', 'catalog.sqlite'):
            shutil.copyfile(capture/name, stage/name)
        (stage/'catalog.lock').touch()
        for relative, task in plan['tasks'].items():
            source = roots[task['source']]/task['path']
            if source.resolve() != source or not source.is_file() or source.stat().st_size != task['bytes']:
                raise ValueError(f'Incomplete or redirected transfer: {relative}')
            target = stage/relative
            target.parent.mkdir(parents=True, exist_ok=True)
            if task['source'] == 'reuse':
                try:
                    os.link(source, target, follow_symlinks=False)
                except OSError as error:
                    if error.errno != errno.EXDEV:
                        raise
                    shutil.copyfile(source, target)
            else:
                shutil.copyfile(source, target)
        verified = verify_capture(stage)
        if verified['manifest_sha256'] != plan['capture_manifest_sha256']:
            raise ValueError('Materialized import manifest differs from the capture')
        stage.rename(output)
    return verified


def summary(plan):
    return dict(through=plan['through'], families={
        source: dict(files=sum(task['source'] == source for task in plan['tasks'].values()),
                     bytes=sum(task['bytes'] for task in plan['tasks'].values() if task['source'] == source))
        for source in ('reuse', 'open', 'sealed')})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    planning, assembly = sub.add_parser('plan'), sub.add_parser('materialize')
    for command in (planning, assembly):
        for field in ('previous', 'capture', 'work'):
            command.add_argument('--'+field, type=Path, required=True)
    assembly.add_argument('--sealed', type=Path, required=True)
    assembly.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = (summary(prepare(args.previous, args.capture, args.work)) if args.command == 'plan'
              else materialize(args.previous, args.capture, args.sealed, args.work, args.output))
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
