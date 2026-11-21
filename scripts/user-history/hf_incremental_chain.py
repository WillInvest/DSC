"""Commit a verified HF result and its successor checkpoint as one lineage step."""
import argparse
from contextlib import closing
import fcntl
import json
import os
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory

from accounting_inputs import connect
from hf_incremental_boundary import FORMAT, read_boundary, seal_record
from hf_incremental_compute import WindowEvaluator, check_published, initialize_database, load_window
from hf_replay_checkpoint import SeededDatabase
from shared.artifacts import checksum, digest, write_json


def accepted_terminal_checks(record, terminal):
    validation = record['validation']
    reference = validation['exact_reference_node_report']
    if (not record['coverage']['structural_exact'] or not validation['within_error_budget']
            or not reference['all_exact']):
        raise ValueError('HF window has not passed its acceptance checks')
    terminal_checks = [item for item in reference['checks'] if item['block'] == terminal]
    if not terminal_checks or not all(item['exact'] for item in terminal_checks):
        raise ValueError('Accepted closing block has no exact account validation')
    return terminal_checks


def seal_successor(window_path, state_path, factors_path, tokens_path, root, accepted, output):
    window, opening, factors, tokens, plan, model, provenance = load_window(
        window_path, state_path, factors_path, tokens_path, root)
    scope = window['identity']
    closing_block = scope['stop']-1
    record = check_published(accepted, provenance)
    checks = accepted_terminal_checks(record, closing_block)
    identity = dict(window_sha256=window['window_sha256'], accepted_result_sha256=checksum(accepted/'result.json'),
                    opening_checkpoint_sha256=scope['checkpoint_sha256'],
                    wallet_registry_sha256=provenance['scope']['wallet_registry_sha256'],
                    wallet_file_sha256=record['wallet_dictionary_sha256'],
                    token_registry_sha256=checksum(root/'plan.json'))
    generation = digest(identity)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.with_suffix('.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if output.exists():
            return verify_successor(output, identity)
        with TemporaryDirectory(dir=output.parent) as temporary:
            work = Path(temporary)
            stage = work/'publish'
            stage.mkdir()
            with closing(connect(work)) as db:
                initialize_database(db, window, opening, factors, tokens['result'])
                seeded = SeededDatabase(db, opening['state'], scope['start'])
                with closing(WindowEvaluator(root, seeded, plan, scope['start'], scope['stop'],
                                             factors['prices'])) as evaluator:
                    for block in range(scope['start'], scope['stop']):
                        evaluator.advance(block)
                    for check in checks:
                        account = evaluator.account(check['user'])
                        if account['account'] != check['node'] or account['block_hash'] != check['block_hash']:
                            raise ValueError('Reconstructed closing state disagrees with accepted RETH samples')
                    checkpoint = seal_record(evaluator, generation, closing_block, scope['end_hash'],
                                             identity, stage/'state.json.gz')
            checkpoint['path'] = 'state.json.gz'
            wallets = stage/'wallets.json'
            shutil.copyfile(accepted/'wallets.json', wallets)
            with wallets.open('rb') as stream:
                os.fsync(stream.fileno())
            boundary = dict(format=FORMAT, release_generation=generation, next_block=scope['stop'],
                            predecessor_hash=scope['end_hash'], input_identity=identity,
                            checkpoint=checkpoint, wallet_count=len(model.owners),
                            exact_closing_account_samples=len(checks))
            write_json(stage/'boundary.json', boundary)
            verify_successor(stage, identity)
            stage.rename(output)
            parent = os.open(output.parent, os.O_RDONLY)
            try:
                os.fsync(parent)
            finally:
                os.close(parent)
        return boundary


def verify_successor(directory, expected):
    manifest = json.loads((directory/'boundary.json').read_text())
    generation = digest(expected)
    if manifest['input_identity'] != expected or manifest['release_generation'] != generation:
        raise ValueError('Existing successor connects to a different HF result')
    boundary = read_boundary(directory/'state.json.gz', manifest['checkpoint']['sha256'],
                             manifest['next_block'], manifest['predecessor_hash'], generation)
    if boundary['input_identity'] != expected or checksum(directory/'wallets.json') != expected['wallet_file_sha256']:
        raise ValueError('Successor state or wallet dictionary checksum differs')
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for option in ('window', 'state', 'factors', 'tokens', 'historical-root', 'accepted', 'output'):
        parser.add_argument('--'+option, required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(seal_successor(args.window, args.state, args.factors, args.tokens, args.historical_root,
                                   args.accepted, args.output)))


if __name__ == '__main__':
    main()
