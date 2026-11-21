"""Compute HF on a helper, then verify its portable result on Node."""
import argparse
import fcntl
import json
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory

import numpy as np

from hf_f64_vectors import decode_users
from hf_incremental_compute import (BLOCK_PART, WALLET_TILE, calculate_window,
                                    check_published, commit_directory, load_window)
from hf_market_replay.validation import validate_node_samples, validate_result
from materialize_accounts import row_count
from shared.artifacts import checksum

CANDIDATE = 'candidate.json'
CANDIDATE_FORMAT = 'aave-v3-hf-offline-candidate-v1'


def produce(window_path, state_path, factors_path, tokens_path, root, output):
    window, boundary, factors, tokens, plan, model, provenance = load_window(
        window_path, state_path, factors_path, tokens_path, root)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.with_suffix('.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if output.exists():
            record = check_published(output, provenance, CANDIDATE)
            if record['candidate_format'] != CANDIDATE_FORMAT:
                raise ValueError('Unrecognized offline HF candidate')
        else:
            with TemporaryDirectory(dir=output.parent) as temporary:
                work = Path(temporary)
                stage = work/'publish'
                stage.mkdir()
                record, samples = calculate_window(work, stage, root, plan, model, window, boundary,
                                                   factors, tokens, provenance)
                candidate = dict(record, candidate_format=CANDIDATE_FORMAT, samples=samples)
                commit_directory(stage, output, CANDIDATE, candidate)
    return dict(path=str(output/CANDIDATE), sha256=checksum(output/CANDIDATE),
                wallet_values=record['wallet_values'], aggregate_rows=record['coverage']['rows'])


def validate_wallets(stage, record, model, samples):
    scope = record['scope']
    start, stop = scope['start'], scope['stop']
    registry = [tuple(row) for row in json.loads((stage/'wallets.json').read_text())]
    if registry != model.owners:
        raise ValueError('Offline wallet dictionary differs from the accepted checkpoint')
    parts = {}
    sample_values = {}
    for sample in samples:
        wallet_id, block = sample['entity_id'], sample['block']
        key = (block, wallet_id)
        if (key in sample_values or not start <= block < stop or not 0 <= wallet_id < len(registry)
                or registry[wallet_id][0] != sample['user'] or registry[wallet_id][1] > block):
            raise ValueError('Invalid offline wallet sample identity')
        sample_values[key] = float('inf') if sample['hf'] == '+inf' else float(sample['hf'])
    if {block for block, _ in sample_values} != {start, (start+stop-1)//2, stop-1}:
        raise ValueError('Offline candidate lacks complete HF sample blocks')

    rows = 0
    for part in record['wallet_parts']:
        lower, upper = part['wallet_start'], part['wallet_stop']
        first, end, ids, matrix = decode_users((stage/part['path']).read_bytes(), model.firsts, scope=scope)
        key = (first, lower)
        expected_ids = np.arange(lower, upper, dtype=np.uint32)[model.firsts[lower:upper] < end]
        logical_rows = int(np.count_nonzero(~np.isnan(matrix)))
        if (key in parts or (first, end) != (part['start'], part['stop'])
                or not np.array_equal(ids, expected_ids) or logical_rows != part['rows']):
            raise ValueError('Offline wallet partition coordinates or coverage differ')
        parts[key] = (end, upper)
        rows += logical_rows
        for (block, wallet_id), expected in list(sample_values.items()):
            if first <= block < end and lower <= wallet_id < upper:
                index = int(np.searchsorted(ids, wallet_id))
                if index >= len(ids) or ids[index] != wallet_id or matrix[block-first, index] != expected:
                    raise ValueError('Offline wallet sample differs from returned Parquet')
                del sample_values[block, wallet_id]
    expected_parts = {(block, offset): (min(stop, block+BLOCK_PART), min(len(registry), offset+WALLET_TILE))
                      for block in range(start, stop, BLOCK_PART)
                      for offset in range(0, len(registry), WALLET_TILE)}
    if (parts != expected_parts or rows != record['wallet_values']
            or rows != row_count(registry, start, stop) or sample_values):
        raise ValueError('Offline wallet products do not cover the entire window')


def accept(window_path, state_path, factors_path, tokens_path, root, candidate, candidate_sha256, output):
    window, _, _, _, _, model, provenance = load_window(window_path, state_path, factors_path, tokens_path, root)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.with_suffix('.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if output.exists():
            record = check_published(output, provenance)
            if record.get('offline_candidate_sha256') != candidate_sha256:
                raise ValueError('Existing output belongs to a different offline candidate')
        else:
            record = accept_candidate(candidate, candidate_sha256, output, provenance, model, window)
    return dict(path=str(output/'result.json'), sha256=checksum(output/'result.json'),
                wallet_values=record['wallet_values'], aggregate_rows=record['coverage']['rows'],
                sample_accounts=record['validation']['account_getter_samples'])


def accept_candidate(candidate, candidate_sha256, output, provenance, model, window):
    if checksum(candidate/CANDIDATE) != candidate_sha256:
        raise ValueError('Offline candidate receipt checksum differs')
    record = check_published(candidate, provenance, CANDIDATE)
    if record.pop('candidate_format') != CANDIDATE_FORMAT or 'validation' in record:
        raise ValueError('Unrecognized offline HF candidate')
    samples = record.pop('samples')
    with TemporaryDirectory(dir=output.parent) as temporary:
        stage = Path(temporary)/'publish'
        stage.mkdir()
        if candidate.is_symlink() or (candidate/'wallets.json').is_symlink():
            raise ValueError('Offline candidate cannot use symlinked artifacts')
        shutil.copyfile(candidate/'wallets.json', stage/'wallets.json')
        if checksum(stage/'wallets.json') != record['wallet_dictionary_sha256']:
            raise ValueError('Offline wallet dictionary changed during acceptance')
        for item in record['wallet_parts'] + record['aggregates']['parts']:
            path = candidate/item['path']
            if path.is_symlink():
                raise ValueError('Offline candidate cannot use symlinked artifacts')
            target = stage/path.name
            shutil.copyfile(path, target)
            if checksum(target) != item['sha256']:
                raise ValueError('Offline partition changed during acceptance')
        validate_wallets(stage, record, model, samples)
        result = record['aggregates']
        job = {key: provenance['scope'][key] for key in ('start', 'stop')}
        if result['reserves'] != model.reserves or result['job'] != job:
            raise ValueError('Offline aggregate scope differs')
        materialized = dict(result, parts=[dict(part, path=str(stage/part['path'])) for part in result['parts']])
        record['coverage'] = validate_result(materialized)
        expected_headers = {item['block']: item['hash'] for item in window['headers']}
        record['validation'] = validate_node_samples(samples, result['job'], expected_headers=expected_headers)
        record['offline_candidate_sha256'] = candidate_sha256
        commit_directory(stage, output, 'result.json', record)
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['compute', 'accept'])
    for field in ('window', 'state', 'factors', 'tokens', 'historical-root', 'output'):
        parser.add_argument('--'+field, required=True, type=Path)
    parser.add_argument('--candidate', type=Path)
    parser.add_argument('--candidate-sha256')
    args = parser.parse_args()
    inputs = (args.window, args.state, args.factors, args.tokens, args.historical_root)
    if args.mode == 'compute':
        result = produce(*inputs, args.output)
    else:
        if args.candidate is None or args.candidate_sha256 is None:
            parser.error('Node acceptance requires --candidate and --candidate-sha256')
        result = accept(*inputs, args.candidate, args.candidate_sha256, args.output)
    print(json.dumps(result))


if __name__ == '__main__':
    main()
