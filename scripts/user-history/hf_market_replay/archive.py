"""Publish accepted cohort aggregates through the existing immutable Drive archive APIs.

Only closed node-accepted receipts, their declared parts and explicit metadata
enter this archive. This stage preserves every source and packaged copy.
"""
import argparse
import fcntl
import json
import os
import shutil
import sqlite3
import time
from pathlib import Path

from shared.artifacts import checksum, digest, write_json
from .ledger import load_plan

FORMAT = 'hf-market-cohort-archive-v1'
INPUT_DATASET = 'https://drive.google.com/drive/folders/10GrNwf8Hr7TgPSBVUJJKK1SiMaqmj1KM'


def load(path):
    return json.loads(Path(path).read_text())


def _accepted_rows(path, plan):
    with sqlite3.connect('file:' + str(path) + '?mode=ro', uri=True) as database:
        database.execute('begin')
        identity = database.execute('select generation,scope_sha from identity').fetchall()
        if identity != [(plan['generation'], digest(plan['jobs']))]:
            raise ValueError('Node archive authority identity differs')
        rows = database.execute(
            'select id,start,stop,payload_sha,receipt,accepted_role from jobs order by start').fetchall()
        database.rollback()
    jobs = sorted(plan['jobs'], key=lambda job: job['start'])
    if len(rows) != len(jobs) or any(row[3] is None or row[4] is None for row in rows):
        raise ValueError('Node cohort coverage is incomplete')
    cursor = plan['identity']['scope']['start']
    for row, job in zip(rows, jobs):
        if row[:3] != (job['id'], job['start'], job['stop']) or row[1] != cursor:
            raise ValueError('Accepted archive scope differs from the plan')
        cursor = row[2]
    if cursor != plan['identity']['scope']['stop']:
        raise ValueError('Accepted archive block coverage is incomplete')
    return rows


def _inside_file(root, path):
    path = Path(path)
    if not path.is_absolute():
        path = root / path
    for current in (path, *path.parents):
        if current.resolve() == root:
            break
        if current.is_symlink():
            raise ValueError('Symlink archive input')
    try:
        relative = path.resolve().relative_to(root)
    except ValueError:
        raise ValueError('Archive member is outside the node authority root') from None
    if '..' in relative.parts or not relative.parts:
        raise ValueError('Invalid archive member path')
    if not path.is_file():
        raise ValueError('Missing regular archive input: ' + str(relative))
    return relative.as_posix()


def _add_file(files, root, path, expected=None):
    relative = _inside_file(root, path)
    path = root / relative
    actual = dict(bytes=Path(path).stat().st_size, sha256=checksum(path))
    if expected is not None and actual != {key: expected[key] for key in ('bytes', 'sha256')}:
        raise ValueError('Closed archive member differs from its receipt')
    if relative in files and files[relative] != actual:
        raise ValueError('Conflicting archive member')
    files[relative] = actual
    return relative


def accepted_manifest(root, plan):
    """Revalidate every accepted result and enumerate only its explicit files."""
    root = Path(root).resolve()
    rows = _accepted_rows(root / 'flow.sqlite', plan)
    from .runner import read_receipt

    jobs = {job['id']: job for job in plan['jobs']}
    files, receipts = {}, []
    reserves = None
    aggregate_probe = None
    for key, start, stop, payload_sha, receipt, role in rows:
        receipt = Path(receipt)
        _inside_file(root, receipt)
        result = read_receipt(receipt.parent, plan, jobs[key])
        if result['payload_sha256'] != payload_sha:
            raise ValueError('Receipt payload differs from node acceptance')
        if reserves is None:
            reserves = result['reserves']
        elif result['reserves'] != reserves:
            raise ValueError('Accepted reserve dictionaries differ')
        receipt_name = _add_file(files, root, receipt)
        receipts.append(dict(job_id=key, payload_sha256=payload_sha,
                             receipt=receipt_name, receipt_sha256=files[receipt_name]['sha256'],
                             accepted_role=role))
        for part in result['parts']:
            name = _add_file(files, root, receipt.parent / part['name'], part)
            if aggregate_probe is None:
                aggregate_probe = name
    return dict(files=files, reserves=reserves, job_receipts=receipts,
                job_count=len(rows), blocks=sum(row[2] - row[1] for row in rows),
                aggregate_probe=aggregate_probe)


def _json_once(path, value):
    if path.exists():
        if load(path) != value:
            raise ValueError('Immutable archive metadata differs: ' + str(path))
    else:
        write_json(path, value)


def _copy_once(source, target, expected_sha=None):
    source, target = Path(source), Path(target)
    if not source.is_file() or source.is_symlink():
        raise ValueError('Expected a regular metadata source')
    sha = checksum(source)
    if expected_sha is not None and sha != expected_sha:
        raise ValueError('Pinned metadata source changed')
    if target.exists():
        if target.is_symlink() or checksum(target) != sha:
            raise ValueError('Immutable archive metadata copy differs')
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    if source.resolve() == target.resolve():
        return
    temporary = target.with_name(target.name + '.incoming')
    shutil.copyfile(source, temporary)
    with temporary.open('rb') as stream:
        os.fsync(stream.fileno())
    os.replace(temporary, target)


def _catalog_snapshot(root, metadata, plan):
    """Use SQLite's online backup API, then retain the first consistent snapshot."""
    source = root / 'flow.sqlite'
    target = metadata / 'catalog.sqlite'
    current = _accepted_rows(source, plan)
    if not target.exists():
        temporary = target.with_name(target.name + '.incoming')
        with sqlite3.connect('file:' + str(source) + '?mode=ro', uri=True) as src:
            with sqlite3.connect(temporary) as dst:
                src.backup(dst)
                dst.execute('pragma journal_mode=DELETE')
                if dst.execute('pragma quick_check').fetchone() != ('ok',):
                    raise ValueError('Archive catalog backup failed integrity checking')
        with temporary.open('rb') as stream:
            os.fsync(stream.fileno())
        os.replace(temporary, target)
    if _accepted_rows(target, plan) != current:
        raise ValueError('Retained archive catalog differs from node acceptance')
    return target


def _metadata(root, plan, config, accepted):
    from .validation import SCHEMA

    metadata = root / 'metadata'
    metadata.mkdir(exist_ok=True)
    paths = [_catalog_snapshot(root, metadata, plan)]
    _json_once(metadata / 'plan.json', plan)
    scopes = [dict(scope_id=i, namespace='reserve', address=address)
              for i, address in enumerate(accepted['reserves'])]
    scopes.append(dict(scope_id=len(scopes), namespace='protocol', address=None))
    _json_once(metadata / 'reserves.json', scopes)
    dataset = dict(format=FORMAT, generation=plan['generation'],
                   scope=plan['identity']['scope'], reserve_count=len(scopes) - 1,
                   cohorts=dict(all='all borrowers passing the analytical debt rule',
                                non_emode='account category equals zero at block end',
                                emode='account category is nonzero at block end'),
                   metric='debt-value-weighted overall wallet health factor',
                   numerator='sum(wallet reserve debt value times wallet HF)',
                   denominator='sum(wallet reserve debt value)',
                   base_currency_unit=100_000_000, hf_scale=1,
                   zero_debt_aggregate=None, dust_debt_base_exclusive=1_000_000,
                   reconstruction_inputs=INPUT_DATASET,
                   parent_generation=plan['identity'].get('parent_generation'),
                   scientific_identity=plan['identity'],
                   parquet_schema=[dict(name=field.name, type=str(field.type),
                                        nullable=field.nullable) for field in SCHEMA],
                   diagnostics='dust exclusion counts and debt appear only on protocol/all rows',
                   aggregate_rows=accepted['blocks'] * len(scopes) * 3)
    _json_once(metadata / 'dataset.json', dataset)
    readme = '''# Aave V3 market HF cohorts

This release contains every-block reserve and protocol debt-weighted HF for
all included borrowers, non-eMode borrowers, and eMode borrowers. Cohort
membership uses each account's category at the end of that block. The original
strict USD0.01 account debt exclusion rule applies to all cohorts.

Read metadata/dataset.json for units and definitions, metadata/reserves.json
for scope IDs, and metadata/plan.json for fixed coverage and code identities.
Each result receipt declares its aggregate Parquet parts and validation samples.
A zero debt denominator produces a null HF. Individual wallet HF and the full
reconstruction inputs remain in the parent dataset linked in dataset.json.

pack-index.json maps each logical file to a TAR, offset, length and SHA256.
The parent dataset's read_archive.py can restore selected members by pinned
index hash. Source files and node TARs are retained by this archive stage.
'''
    readme_path = metadata / 'README.md'
    if readme_path.exists() and readme_path.read_text() != readme:
        raise ValueError('Immutable archive README differs')
    if not readme_path.exists():
        readme_path.write_text(readme)
    paths.extend(metadata / name for name in ('plan.json', 'reserves.json', 'dataset.json', 'README.md'))
    code_sources = dict(plan['identity'].get('package_sources', {}))
    code_sources.update(plan.get('operational_sources', {}))
    code_sources['archive.py'] = checksum(__file__)
    for name, sha in code_sources.items():
        if Path(name).name != name or not name.endswith('.py'):
            raise ValueError('Invalid pinned code filename')
        target = metadata / 'code' / name
        _copy_once(Path(__file__).with_name(name), target, sha)
        paths.append(target)
    if config.get('source_bundle'):
        if not config.get('source_bundle_sha256'):
            raise ValueError('Source Git bundle needs an explicit checksum')
        target = metadata / 'tested-sources.bundle'
        _copy_once(config['source_bundle'], target, config['source_bundle_sha256'])
        paths.append(target)
    for relative, source in config.get('archive_metadata_files', {}).items():
        name = Path(relative)
        if name.is_absolute() or '..' in name.parts or not name.parts or name.parts[0] != 'metadata':
            raise ValueError('Explicit archive metadata must be below metadata/')
        target = root / name
        _copy_once(source, target)
        paths.append(target)
    return paths


def _completed_report(directory, plan):
    complete_path = directory / 'complete.json'
    if not complete_path.exists():
        return None
    complete = load(complete_path)
    report = load(directory / 'accepted.json')
    if (report['generation'] != plan['generation']
            or complete['accepted_sha256'] != checksum(directory / 'accepted.json')
            or report['index_sha256'] != checksum(directory / 'pack-index.json')
            or report['manifest_sha256'] != checksum(directory / 'manifest.json')):
        raise ValueError('Completed archive publication identity changed')
    return dict(report, publication=complete)


def run(config_path, archive_root=None):
    """Archive a complete immutable node scope and verify its Drive publication."""
    config = load(config_path)
    plan = load_plan(config['plan_path'])
    root = Path(config['root']).resolve()
    directory = Path(archive_root or config.get('archive_root') or
                     root.parent / ('archive-' + plan['generation'][:12]))
    if directory.is_symlink():
        raise ValueError('Symlink archive directory')
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / 'archive.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        completed = _completed_report(directory, plan)
        if completed is not None:
            return completed
        from hf_flow_packing import pack
        from hf_flow_cloud import _copy_and_check, _readback

        accepted = accepted_manifest(root, plan)
        files = accepted['files']
        for path in _metadata(root, plan, config, accepted):
            _add_file(files, root, path)
        manifest = dict(format=FORMAT, generation=plan['generation'],
                        scope=plan['identity']['scope'], files=files,
                        job_count=accepted['job_count'], blocks=accepted['blocks'],
                        job_receipts=accepted['job_receipts'])
        _json_once(directory / 'manifest.json', manifest)
        index = pack(root, files, directory, target_bytes=config.get('archive_pack_bytes', 2_000_000_000))
        index_path = directory / 'pack-index.json'
        transferred = dict(index['packs'])
        for name in ('pack-index.json', 'manifest.json'):
            path = directory / name
            transferred[name] = dict(bytes=path.stat().st_size, sha256=checksum(path))
        remote = config.get('archive_remote') or (
            'dsc_hf:AaveV3_Ethereum/05_Cohort_HF_20261120/' + plan['generation'])
        operations = directory / 'operations'
        operations.mkdir(exist_ok=True)
        _copy_and_check(directory, remote, transferred, operations)
        probes = []
        for name in (accepted['aggregate_probe'], 'metadata/plan.json', 'metadata/reserves.json'):
            member = index['members'][name]
            proof = _readback(remote + '/' + member['pack'], member,
                              offset=member['offset'], count=member['bytes'])
            probes.append(dict(proof, path=name, pack=member['pack'], offset=member['offset']))
        first_pack = sorted(index['packs'])[0]
        package_probe = dict(_readback(remote + '/' + first_pack, index['packs'][first_pack]),
                             path=first_pack)
        report = dict(format=FORMAT, generation=plan['generation'], remote=remote,
                      scope=plan['identity']['scope'], job_count=accepted['job_count'],
                      blocks=accepted['blocks'], archive_format=index['format'],
                      manifest_sha256=checksum(directory / 'manifest.json'),
                      index_sha256=checksum(index_path), index_path=str(index_path),
                      logical_input_bytes=sum(value['bytes'] for value in files.values()),
                      verified_bytes=sum(value['bytes'] for value in transferred.values()),
                      all_remote_size_md5_exact=True, all_local_tar_members_sha256_exact=True,
                      restore_probes=probes, package_restore_probes=[package_probe],
                      node_payloads_retained=True, archive_code_sha256=checksum(__file__))
        target = directory / 'accepted.json'
        if target.exists():
            previous = load(target)
            if {key: value for key, value in previous.items() if key != 'verified_at'} != report:
                raise ValueError('Immutable archive acceptance differs')
            report = previous
        else:
            report['verified_at'] = time.time()
            write_json(target, report)
        publication_dir = operations / 'publication'
        publication_dir.mkdir(exist_ok=True)
        publication_file = dict(bytes=target.stat().st_size, sha256=checksum(target))
        _copy_and_check(directory, remote, {'accepted.json': publication_file}, publication_dir)
        publication = _readback(remote + '/accepted.json', publication_file)
        complete = dict(generation=plan['generation'], accepted_sha256=checksum(target),
                        remote_acceptance_exact=publication, completed_at=time.time())
        write_json(directory / 'complete.json', complete)
        return dict(report, publication=complete)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    parser.add_argument('--archive-root')
    args = parser.parse_args()
    print(json.dumps(run(args.config, args.archive_root), sort_keys=True))


if __name__ == '__main__':
    main()
