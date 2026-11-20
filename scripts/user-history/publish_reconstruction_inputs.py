"""Publish exact reconstruction inputs with the existing pack/copy/check APIs.

No source retirement occurs here. Extraction coverage comes from pinned plans
and explicit manifests, never from a directory listing.
"""
import argparse
import importlib.util
import json
import time
from pathlib import Path

from shared.artifacts import checksum, write_json
from hf_flow_packing import pack
from hf_flow_cloud import _copy_and_check, _readback


def event_chunks(module_path):
    spec = importlib.util.spec_from_file_location('dsc_event_grid', module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.chunks


def token_inventory(base, plan_path, chunks):
    base = Path(base).resolve()
    plan_path = Path(plan_path)
    plan = json.loads(plan_path.read_text())
    files = {}
    contracts = []

    def add(path, expected=None):
        path = Path(path)
        if path.is_symlink() or not path.resolve().is_relative_to(base):
            raise ValueError('Input path escaped the declared source root')
        size = path.stat().st_size
        item = expected or dict(bytes=size, sha256=checksum(path))
        if size != item['bytes']:
            raise ValueError('Input size differs from extraction manifest')
        name = str(path.resolve().relative_to(base))
        value = dict(bytes=size, sha256=item.get('sha256') or checksum(path))
        if name in files and files[name] != value:
            raise ValueError('Conflicting input identity')
        files[name] = value

    add(plan_path)
    for job in plan['jobs']:
        root = Path(job['node_dir'])
        path = root/'manifest.json'
        manifest = json.loads(path.read_text())
        identity = {k: plan[k] for k in ['end_block', 'end_hash', 'chunk_size']}
        identity.update(address=job['address'], start_block=job['start_block'])
        if manifest['identity'] != identity:
            raise ValueError('Extraction identity differs from pinned plan')
        if json.loads((root/'identity.json').read_text()) != identity:
            raise ValueError('Extraction identity receipt differs from pinned plan')
        expected = chunks(job, plan)
        actual = set(manifest['files'])
        empty = set(manifest['empty_chunks'])
        if (actual & empty or actual | empty != set(expected)
                or manifest['total_chunks'] != len(expected)):
            raise ValueError('Missing or overlapping extraction chunk coverage')
        for name, entry in manifest['files'].items():
            if Path(name).name != name:
                raise ValueError('Noncanonical extraction filename')
            add(root/'encoded'/name, entry)
        add(path)
        add(root/'identity.json')
        contracts.append(dict(address=job['address'], kind=job['kind'],
            asset=job['asset'], start=job['start_block'], stop=plan['end_block']+1,
            files=len(actual), empty_chunks=len(empty),
            rows=sum(x['rows'] for x in manifest['files'].values())))
    if len({x['address'] for x in contracts}) != len(contracts):
        raise ValueError('Duplicate token contract in pinned plan')
    return dict(format='dsc-reconstruction-input-manifest-v1',
        scope='All planned aToken, variable-debt and stable-debt raw events',
        end_block=plan['end_block'], end_hash=plan['end_hash'],
        plan_sha256=checksum(plan_path), contracts=contracts, files=files,
        coverage_source='Pinned extraction plan, per-contract identity and complete chunk manifests',
        pool_events_included=False, original_sources_retained=True)


def publish(base, out, remote):
    base, out = Path(base), Path(out)
    manifest_path = out/'source-manifest.json'
    pin = checksum(manifest_path)
    done = out/'complete.json'
    if done.exists():
        report = json.loads(done.read_text())
        if report['source_manifest_sha256'] != pin:
            raise ValueError('Accepted reconstruction input manifest changed')
        return report
    manifest = json.loads(manifest_path.read_text())
    files = dict(manifest['files'])
    files[str(manifest_path.relative_to(base))] = dict(
        bytes=manifest_path.stat().st_size, sha256=pin)
    write_json(out/'progress.json', dict(phase='packing', at=time.time(),
        files=len(files), bytes=sum(x['bytes'] for x in files.values())))
    index = pack(base, files, out, target_bytes=256_000_000)
    transfer = dict(index['packs'])
    for name in ['pack-index.json', 'source-manifest.json']:
        p = out/name
        transfer[name] = dict(bytes=p.stat().st_size, sha256=checksum(p))
    write_json(out/'progress.json', dict(phase='uploading', at=time.time(),
        files=len(transfer), bytes=sum(x['bytes'] for x in transfer.values())))
    _copy_and_check(out, remote, transfer, out)
    # Input archives are small enough for full remote SHA readback, not only
    # samples. Existing packing already checked every local member's SHA.
    probes = {name: _readback(remote+'/'+name, value)
              for name, value in transfer.items()}
    report = dict(source_manifest_sha256=pin, remote=remote,
        index_sha256=checksum(out/'pack-index.json'),
        all_local_member_sha256_exact=True, all_remote_size_md5_exact=True,
        all_remote_sha256_exact=all(x['exact'] for x in probes.values()),
        remote_sha_readbacks=probes, files=len(files),
        source_bytes=sum(x['bytes'] for x in manifest['files'].values()),
        archive_bytes=sum(x['bytes'] for x in transfer.values()),
        original_sources_retained=True, numerical_coverage_increment=0,
        completed_at=time.time())
    write_json(done, report)
    write_json(out/'progress.json', dict(phase='complete', **report))
    return report


def merge_pool_inventories(inventories):
    files = {}
    contracts = []
    for source in inventories:
        for name, identity in source['files'].items():
            if name in files and files[name] != identity:
                raise ValueError('Conflicting shared event input')
            files[name] = identity
        contracts.extend(source['contracts'])
    spans = sorted(contracts, key=lambda x: x['start'])
    if not spans or len({x['address'] for x in spans}) != 1 or any(x['kind'] != 'Pool' for x in spans):
        raise ValueError('Merged event archive must describe one Pool contract')
    if any(a['stop'] != b['start'] for a, b in zip(spans, spans[1:])):
        raise ValueError('Merged event archive has a gap or overlap')
    last = max(inventories, key=lambda x: x['end_block'])
    return dict(inventories[0], scope='Complete pinned Pool raw events', files=files,
        contracts=spans, pool_events_included=True,
        end_block=last['end_block'], end_hash=last['end_hash'],
        source_plan_sha256=[x['plan_sha256'] for x in inventories])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--base', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--remote', required=True)
    parser.add_argument('--token-plan', type=Path)
    parser.add_argument('--event-plan', type=Path, action='append', default=[])
    parser.add_argument('--event-common', type=Path)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    plans = [args.token_plan] if args.token_plan else args.event_plan
    if plans:
        inventories = [token_inventory(args.base, p, event_chunks(args.event_common)) for p in plans]
        value = inventories[0]
        if args.event_plan:
            value = merge_pool_inventories(inventories)
        path = args.out/'source-manifest.json'
        if path.exists():
            if json.loads(path.read_text()) != value:
                raise ValueError('Pinned source inventory changed')
        else:
            write_json(path, value)
    print(json.dumps(publish(args.base, args.out, args.remote)), flush=True)
