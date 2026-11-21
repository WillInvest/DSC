"""Build a Node coordinator baseline from already verified imported catalogs."""
import argparse
from contextlib import ExitStack
import json
from pathlib import Path
import re
from tempfile import TemporaryDirectory

from pipeline.compaction.reader import snapshot
from pipeline.compaction.store import Fragment
from pipeline.stages import MODE, read, validate_previous
from shared.artifacts import checksum, write_json


def catalog_manifest(root, through):
    path = root/'manifest.json'
    manifest = read(path)
    if (manifest['version'] != 1 or manifest['through'] != through
            or checksum(root/'catalog.sqlite') != manifest['catalog_sha256']):
        raise ValueError('Imported catalog does not pin the requested handoff boundary')
    entries = {row['path']: row for row in manifest['files']}
    if len(entries) != len(manifest['files']):
        raise ValueError('Duplicate payloads in the imported catalog manifest')
    return entries, dict(root=str(root), manifest_sha256=checksum(path),
                         catalog_sha256=manifest['catalog_sha256'])


def rows_at(rows, start):
    return [row for row in rows if row['start'] <= start < row['stop']]


def verified_source(root, entries, row):
    relative = Path(row['path'])
    path = root/relative
    candidate = entries[row['path']]
    compared = {key: value for key, value in row.items() if key != 'absolute_path'}
    if (relative.is_absolute() or '..' in relative.parts or path.resolve() != path
            or any(candidate[key] != value for key, value in compared.items())
            or path.stat().st_size != candidate['bytes'] or checksum(path) != row['sha256']):
        raise ValueError('Handoff source does not match the verified catalog')
    return path


def archive_row(root, entries, rows, kind, through):
    from pipeline.compaction.jsonl import records

    matches = [row for row in rows if row['stream'].split('/')[0] == kind]
    if len(matches) != 1:
        raise ValueError(f'Expected exactly one {kind} archive at the handoff')
    row = matches[0]
    path = verified_source(root, entries, row)
    segment = Fragment(path, row['start'], row['stop'])
    record, = records([segment], through, through+1)
    return record, dict(path=row['path'], sha256=row['sha256'], stream=row['stream'])


def verify_boundary_archives(data, revert, data_entries, revert_entries, data_rows, revert_rows,
                             addresses, through, expected_receipts):
    from reverted.trace_received import select

    scanned, receipts = archive_row(revert, revert_entries, revert_rows, 'receipts', through)
    archive, traces = archive_row(revert, revert_entries, revert_rows, 'traces', through)
    selected, selection = archive_row(revert, revert_entries, revert_rows, 'selected', through)
    delivered, delivery = archive_row(data, data_entries, data_rows, 'selected', through)
    if (scanned != expected_receipts or archive['receipt_summary'] != scanned
            or [item['receipt'] for item in archive['failed']] != scanned['failed_receipts']):
        raise ValueError('Archived receipts or full traces disagree with RETH at the handoff')
    if selected != delivered or selected != select(archive, addresses):
        raise ValueError('Revert selection and delivered selection disagree at the handoff')
    return dict(receipts=receipts, traces=traces, selection=selection, delivery=delivery,
                failed_transactions=len(archive['failed']), selected_transactions=len(selected['failed']))


def state_sources(rows, addresses):
    states = {}
    for row in rows:
        parts = row['stream'].split('/')
        if parts[0] != 'state':
            continue
        if len(parts) < 5:
            raise ValueError('Malformed state stream identity')
        address = parts[2]
        if address not in addresses or address in states or parts[3] != MODE:
            raise ValueError('Unexpected or duplicate state stream at the handoff')
        states[address] = row
    if set(states) != addresses:
        raise ValueError('Handoff state does not cover the accepted registry')
    return states


def write_checkpoint(stage, output, job, root, entries, row, through):
    from pipeline.verify import integers, table_rows

    address = job['address'].lower()
    path = verified_source(root, entries, row)
    records = table_rows(path, through, through+1)
    if len(records) != 1 or records[0]['block_number'] != through:
        raise ValueError(f'Expected exactly one state row for {address}')
    last = integers(json.loads(records[0]['state_json']))
    local = Path('checkpoints')/(address+'.json')
    write_json(stage/local, dict(state='complete', next_block=through+1, last_state=last,
                                 identity=dict(mode=MODE, address=job['address'])))
    checkpoint_sha = checksum(stage/local)
    evidence = dict(address=address, checkpoint=str(local), sha256=checkpoint_sha,
                    source_parquet=row['path'], source_sha256=row['sha256'])
    return dict(job, checkpoint=str(output/local), checkpoint_sha256=checkpoint_sha), evidence


def build(plan_path, data, revert, through, block_hash, expected_receipts, output):
    data, revert, output = data.resolve(), revert.resolve(), output.resolve()
    if any(output.is_relative_to(source) for source in (data, revert)):
        raise ValueError('Handoff output must be separate from imported catalogs')
    if not re.fullmatch(r'0x[a-f0-9]{64}', block_hash) or expected_receipts['block_hash'] != block_hash:
        raise ValueError('Expected RETH handoff block hash')
    plan = read(plan_path)
    jobs = plan['jobs']
    addresses = {job['address'].lower() for job in jobs}
    if len(jobs) != 102 or len(addresses) != 102 or plan['end_block'] > through:
        raise ValueError('Invalid registry or scope for the handoff')
    output.parent.mkdir(parents=True, exist_ok=True)
    with ExitStack() as stack:
        data_rows = rows_at(stack.enter_context(snapshot(data)), through)
        revert_rows = rows_at(stack.enter_context(snapshot(revert)), through)
        data_entries, data_capture = catalog_manifest(data, through)
        revert_entries, revert_capture = catalog_manifest(revert, through)
        identity = dict(through=through, hash=block_hash, previous_plan_sha256=checksum(plan_path),
                        data=data_capture, revert=revert_capture)
        if output.exists():
            existing = read(output/'handoff.json')
            if existing['identity'] != identity or existing['baseline_sha256'] != checksum(output/'plan.json'):
                raise ValueError('Existing Node handoff differs')
            validate_previous(read(output/'plan.json'), through+1)
            return existing
        archives = verify_boundary_archives(data, revert, data_entries, revert_entries,
                                           data_rows, revert_rows, addresses, through, expected_receipts)
        states = state_sources(data_rows, addresses)
        temporary = stack.enter_context(TemporaryDirectory(dir=output.parent))
        stage = Path(temporary)/'publish'
        stage.mkdir()
        restored, evidence = [], []
        for source in jobs:
            row = states[source['address'].lower()]
            job, proof = write_checkpoint(stage, output, source, data, data_entries, row, through)
            restored.append(job)
            evidence.append(proof)
        next_plan = dict(plan, jobs=restored, end_block=through, end_hash=block_hash)
        write_json(stage/'plan.json', next_plan)
        result = dict(format='dsc-node-handoff-v1', identity=identity, archives=archives,
                      checkpoints=evidence, baseline_sha256=checksum(stage/'plan.json'))
        write_json(stage/'handoff.json', result)
        stage.rename(output)
        return result


def main():
    from pipeline.runtime import NODE_LOCAL, rpc
    from reverted.scan_receipts import receipt_record

    parser = argparse.ArgumentParser(description=__doc__)
    for key in ('plan', 'data', 'revert', 'output'):
        parser.add_argument('--'+key, type=Path, required=True)
    parser.add_argument('--through', type=int, required=True)
    args = parser.parse_args()
    if not NODE_LOCAL:
        parser.error('Run the handoff on the RETH node with DSC_PIPELINE_SITE=node')
    finalized = rpc('eth_getBlockByNumber', ['finalized', False])
    if args.through > int(finalized['number'], 16):
        raise ValueError('Handoff exceeds the RETH finalized block')
    header = rpc('eth_getBlockByNumber', [hex(args.through), False])
    result = build(args.plan, args.data, args.revert, args.through, header['hash'],
                   receipt_record(args.through), args.output)
    print(json.dumps(dict(identity=result['identity'], checkpoints=len(result['checkpoints']),
                          archives=result['archives'], baseline_sha256=result['baseline_sha256'])))


if __name__ == '__main__':
    main()
