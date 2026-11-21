"""Produce wallet vectors and three cohort aggregates from a closed HF window."""
import argparse
from contextlib import closing
import fcntl
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np

from accounting_inputs import connect
from hf_batch_f64 import FloatBatch
from hf_f64_contract import CONTRACT, REGISTRY
from hf_f64_vectors import decode_users, encode_users
from hf_incremental_boundary import read_boundary
from hf_incremental_window import validate_window
from hf_market_replay.calculation import _categories, _samples
from hf_market_replay.cohorts import independent_summary, summarize
from hf_market_replay.validation import FORMAT, encode_part, validate_node_samples, validate_result
from hf_replay_checkpoint import SeededDatabase
from materialize_accounts import owner_universe, row_count
from native_batch import BatchEvaluator
from native_hf import ZERO
from shared.artifacts import checksum, digest, write_json

BLOCK_PART = 32
WALLET_TILE = 30_000  # At most 960,000 values with the existing tile encoder.


class WindowEvaluator(BatchEvaluator):
    """Reuse scientific evaluation with the window's verified price vectors."""

    def __init__(self, root, database, plan, start, stop, price_rows):
        self.price_rows = price_rows
        super().__init__(root, database, plan, start, stop)

    def prices(self, block):
        row = self.price_rows[str(block)]
        return row['assets'], row['values']


def initialize_database(db, window, boundary, factors, positions):
    start = window['identity']['start']
    with db:
        db.executemany('insert into headers values(?,?,?)',
                       [(row['block'], row['timestamp'], row['hash']) for row in window['headers']])
        db.executemany('insert into pool_events values(?,?,?,?,?)', factors['pool_events'])
        openings = [(asset, json.dumps({'reserve': dict(state, configuration={'data': state['configuration']})}))
                    for asset, state in boundary['reserves'].items()]
        db.executemany("insert into snapshots values('reserve',?,?,?)",
                       [(asset, start-1, data) for asset, data in openings])
        db.executemany('insert into snapshots values(?,?,?,?)',
                       [(kind, key, block, json.dumps(row)) for kind, key, block, row in factors['snapshots']])
        for token, streams in positions['tokens'].items():
            db.executemany('insert into changes values(?,?,?,?,?,?)', [
                (token, row['owner'], row['block'], row['transaction_index'], row['log_index'], row['after'])
                for row in streams['history']])
            db.executemany('insert into gho_states values(?,?,?,?,?,?,?)', [
                (token, row['owner'], row['block'], row['transaction_index'], row['log_index'],
                 row['previous_index'], row['discount_percent']) for row in streams.get('gho_history', [])])


def wallet_registry(historical, first_seen, start, stop):
    if digest(historical) != REGISTRY:
        raise ValueError('Frozen HF wallet dictionary differs')
    known = set(dict(historical))
    if any(not start <= first < stop for first in first_seen.values()):
        raise ValueError('Wallet discovery is outside the current window')
    newcomers = [(owner, first) for owner, first in first_seen.items() if owner != ZERO and owner not in known]
    return historical + sorted(newcomers, key=lambda item: (item[1], item[0]))


def write_tiles(directory, start, matrix, firsts, scope):
    for offset in range(0, len(firsts), WALLET_TILE):
        first = firsts[offset:offset+WALLET_TILE]
        raw = encode_users(matrix[:, offset:offset+len(first)], start, first, offset, scope=scope)
        lo, hi, ids, values = decode_users(raw, firsts, scope=scope)
        if not np.array_equal(values, matrix[:, ids], equal_nan=True):
            raise ValueError('Wallet tile readback differs')
        name = f'wallets-{lo}-{hi-1}-{offset}.parquet'
        path = directory / name
        path.write_bytes(raw)
        yield dict(path=name, sha256=checksum(path), start=lo, stop=hi, wallet_start=offset,
                   wallet_stop=offset+len(first), rows=int(np.count_nonzero(~np.isnan(values))))


def compute_parts(evaluator, model, scope, output):
    start, stop = scope['start'], scope['stop']
    sample_blocks = {start, (start+stop-1)//2, stop-1}
    wallet_parts, aggregate_parts, proofs, samples = [], [], [], []
    for block_start in range(start, stop, BLOCK_PART):
        block_stop = min(stop, block_start+BLOCK_PART)
        wallet_values = np.full((block_stop-block_start, len(model.owners)), np.nan)
        block_rows = []
        for block in range(block_start, block_stop):
            evaluator.advance(block)
            value = model.evaluate(evaluator, dust_cutoff=CONTRACT['dust_debt_base_exclusive'])
            wallet_values[block-block_start, :len(value['hf'])] = value['hf']
            args = (value['hf'], value['debt_user_ids'], value['debt_reserve_ids'],
                    value['debt_values'], _categories(value, model, evaluator), len(model.reserves))
            block_rows.append(dict(block=block, cohorts=summarize(*args),
                                   excluded_dust_users=value['excluded_users'],
                                   excluded_dust_debt_base=value['excluded_debt_base']))
            if block in sample_blocks:
                reference = independent_summary(*args)
                proofs.append(dict(block=block, cohorts={name: {key: np.asarray(series).tolist()
                                   for key, series in row.items()} for name, row in reference.items()}))
                samples.extend(_samples(value, args[-2], model, evaluator))
        wallet_parts.extend(write_tiles(output, block_start, wallet_values, model.firsts, scope))
        aggregate = encode_part(block_rows, block_start, block_stop, len(model.reserves))
        path = output / aggregate.pop('name')
        path.write_bytes(aggregate.pop('data'))
        aggregate_parts.append(dict(aggregate, path=str(path)))
    return wallet_parts, aggregate_parts, proofs, samples


def check_published(output, provenance):
    manifest_path = output/'result.json'
    record = json.loads(manifest_path.read_text())
    if any(record[key] != value for key, value in provenance.items()):
        raise ValueError('Existing HF result belongs to a different input generation')
    if checksum(output/'wallets.json') != record['wallet_dictionary_sha256']:
        raise ValueError('Published wallet dictionary checksum differs')
    for part in record['wallet_parts'] + record['aggregates']['parts']:
        path = output/part['path']
        if path.parent != output or checksum(path) != part['sha256']:
            raise ValueError('Published HF partition checksum differs')
    return record


def run(window_path, state_path, factors_path, tokens_path, root, output):
    from hf_incremental_factors import pinned_json

    window = json.loads(window_path.read_text())
    identity = validate_window(window)
    start, stop = identity['start'], identity['stop']
    boundary = read_boundary(state_path, identity['checkpoint_sha256'], start,
                             identity['predecessor_hash'], identity['release_generation'])
    factors = pinned_json(factors_path, window['window_sha256'])
    tokens = pinned_json(tokens_path, window['window_sha256'])
    if (factors['checkpoint_sha256'] != identity['checkpoint_sha256']
            or factors['token_sha256'] != checksum(tokens_path)):
        raise ValueError('Factors are detached from the opening checkpoint or token replay')
    plan = json.loads((root/'plan.json').read_text())
    owners = wallet_registry(owner_universe(root, start), tokens['result']['first_seen'], start, stop)
    scope = dict(start=start, stop=stop, window_sha256=window['window_sha256'],
                 wallet_registry_sha256=digest(owners))
    provenance = dict(format='aave-v3-hf-incremental-result-v1', scope=scope,
                      factors_sha256=checksum(factors_path), token_replay_sha256=checksum(tokens_path))
    model = FloatBatch(owners, plan['jobs'])
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.with_suffix('.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if output.exists():
            record = check_published(output, provenance)
        else:
            record = publish_window(output, root, plan, model, window, boundary, factors, tokens, provenance)
    return dict(path=str(output/'result.json'), sha256=checksum(output/'result.json'),
                wallets=len(owners), wallet_values=record['wallet_values'], aggregate_rows=record['coverage']['rows'],
                sample_accounts=record['validation']['account_getter_samples'])


def publish_window(output, root, plan, model, window, boundary, factors, tokens, provenance):
    scope = provenance['scope']
    start, stop = scope['start'], scope['stop']
    with TemporaryDirectory(dir=output.parent) as temporary:
        work = Path(temporary)
        stage = work/'publish'
        stage.mkdir()
        with closing(connect(work)) as db:
            initialize_database(db, window, boundary, factors, tokens['result'])
            seeded = SeededDatabase(db, boundary['state'], start)
            with closing(WindowEvaluator(root, seeded, plan, start, stop, factors['prices'])) as evaluator:
                wallets, aggregates, proofs, samples = compute_parts(evaluator, model, scope, stage)
        job = dict(start=start, stop=stop)
        result = dict(format=FORMAT, job=job, reserves=model.reserves, parts=aggregates, cohort_proofs=proofs)
        verified = validate_result(result)
        if sum(part['rows'] for part in wallets) != row_count(model.owners, start, stop):
            raise ValueError('Wallet values do not cover the entire published dictionary')
        expected_headers = {row['block']: row['hash'] for row in window['headers']}
        samples_verified = validate_node_samples(samples, job, expected_headers=expected_headers)
        for record in aggregates:
            record['path'] = Path(record['path']).name
        write_json(stage/'wallets.json', model.owners)
        record = dict(**provenance,
                      wallet_dictionary_sha256=checksum(stage/'wallets.json'), wallet_parts=wallets,
                      aggregates=result, coverage=verified, validation=samples_verified,
                      wallet_values=sum(part['rows'] for part in wallets))
        for part in (*wallets, *aggregates):
            with (stage/part['path']).open('rb') as payload:
                os.fsync(payload.fileno())
        write_json(stage/'result.json', record)
        stage.rename(output)
        parent = os.open(output.parent, os.O_RDONLY)
        try:
            os.fsync(parent)
        finally:
            os.close(parent)
        return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for field in ('window', 'state', 'factors', 'tokens', 'historical-root', 'output'):
        parser.add_argument('--'+field, required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(run(args.window, args.state, args.factors, args.tokens, args.historical_root, args.output)))


if __name__ == '__main__':
    main()
