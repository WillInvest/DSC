"""Replay one finalized Aave HF window and commit the checkpoint for its successor."""
import argparse
import fcntl
import json
from pathlib import Path

import accounting_node
from hf_incremental_boundary import FORMAT, read_boundary
from hf_incremental_chain import seal_successor
from hf_incremental_compute import run as compute
from hf_incremental_factors import collect as collect_factors
from hf_incremental_oracle import collect as collect_oracle
from hf_incremental_pool import collect as collect_pool
from hf_incremental_positions import run as replay_tokens
from hf_incremental_token_inputs import collect_raw_tokens
from hf_incremental_window import pin_window
from shared.artifacts import checksum, write_json


def run(opening, end_block, catalog, historical_root, cryo, output):
    from_block = json.loads((opening/'boundary.json').read_text())
    state = opening/'state.json.gz'
    if from_block['format'] != FORMAT:
        raise ValueError('Unknown HF boundary format')
    start, stop = from_block['next_block'], end_block+1
    read_boundary(state, from_block['checkpoint']['sha256'], start,
                  from_block['predecessor_hash'], from_block['release_generation'])
    w3 = accounting_node.web()

    def rpc(method, args):
        response = w3.provider.make_request(method, args)
        if 'error' in response:
            raise RuntimeError(f"RETH rejected {method}: {response['error']}")
        return response['result']

    output.mkdir(parents=True, exist_ok=True)
    status = output/'progress.json'
    window_path = output/'window.json'
    pool, oracle, raw = output/'pool', output/'oracle', output/'raw-tokens'
    tokens, factors = output/'token-replay.json', output/'factors.json'
    result, successor = output/'result', output/'successor'
    jobs = {job['address'].lower(): job for job in json.loads((historical_root/'plan.json').read_text())['jobs']}

    def progress(stage, **evidence):
        write_json(status, dict(start=start, stop=stop, stage=stage, **evidence), durable=False)

    with (output/'window.lock').open('a') as guard:
        fcntl.flock(guard, fcntl.LOCK_EX | fcntl.LOCK_NB)
        progress('pin_headers')
        window = pin_window(rpc, start, stop, from_block['predecessor_hash'],
                            generation=from_block['release_generation'],
                            checkpoint_sha256=from_block['checkpoint']['sha256'])
        if window_path.exists():
            if json.loads(window_path.read_text()) != window:
                raise ValueError('Existing job directory contains a different HF window')
        else:
            write_json(window_path, window)
        progress('collect_pool')
        collect_pool(window_path, catalog, pool)
        progress('collect_oracle')
        collect_oracle(window_path, oracle)
        progress('collect_token_logs')
        collect_raw_tokens(window, jobs, raw, w3, cryo)
        progress('replay_scaled_positions')
        replay_tokens(window_path, state, raw/'registered-token-inputs.json', historical_root, tokens, w3)
        progress('collect_configuration')
        collect_factors(window_path, state, pool/'pool-inputs.json', oracle/'oracle-inputs.json', tokens, factors)
        progress('compute_hf')
        computed = compute(window_path, state, factors, tokens, historical_root, result)
        progress('seal_successor')
        sealed = seal_successor(window_path, state, factors, tokens, historical_root, result, successor)
        evidence = dict(window_sha256=window['window_sha256'], result=computed,
                        successor_sha256=checksum(successor/'boundary.json'), next_block=sealed['next_block'])
        progress('locally_verified', **evidence)
        return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for field in ('opening', 'catalog', 'historical-root', 'cryo', 'output'):
        parser.add_argument('--'+field, required=True, type=Path)
    parser.add_argument('--end-block', required=True, type=int)
    args = parser.parse_args()
    print(json.dumps(run(args.opening, args.end_block, args.catalog, args.historical_root,
                         args.cryo, args.output)))


if __name__ == '__main__':
    main()
