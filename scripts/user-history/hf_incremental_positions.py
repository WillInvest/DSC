"""Replay token logs from a sealed HF checkpoint within verified ABI intervals."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import fcntl
import json
from pathlib import Path

from decoding.pool_decode import PoolDecoder
from decode_tokens import UPGRADED, owners
import gho_replay
from hf_incremental_boundary import read_boundary
from hf_incremental_token_inputs import check_anchors, read_token_inputs, verify_routes
from hf_incremental_window import validate_window
from oracle_common import function
from replay_positions import Math, modern_versions
from shared.artifacts import checksum, digest, write_json
from web3 import HTTPProvider, Web3

GHO_TOKEN = '0x786dbff3f1292ae8f92ea68cf93c30b34b1ed04b'
POSITION_EVENTS = {'Mint', 'Burn', 'BalanceTransfer'}
GHO_EVENTS = POSITION_EVENTS | {'DiscountPercentUpdated'}
UPGRADE_TOPIC = Web3().eth.contract(abi=[UPGRADED]).events.Upgraded().topic


def decode_logs(rows, token, version, abi, headers):
    decoder = PoolDecoder('events', abi, token, [], schema_version=version)
    events = []
    first_seen = {}
    seen = set()
    for row in rows:
        block = row['block_number']
        if headers.get(block) != row['block_hash'].lower():
            raise ValueError('Token log is outside the pinned block lineage')
        if row['topic0'] == UPGRADE_TOPIC:
            raise ValueError(f'Token upgrade requires a reviewed profile: {token}')
        decoded = decoder.decode_row(row, token)
        if decoded.key in seen:
            raise ValueError('Duplicate token log')
        seen.add(decoded.key)
        fields = decoder.decoder.event_by_topic(row['topic0']).abi['inputs']
        for owner in owners(decoded, fields):
            first_seen[owner] = min(block, first_seen.get(owner, block))
        if decoded.name in (GHO_EVENTS if token == GHO_TOKEN else POSITION_EVENTS):
            events.append(decoded.values)
    events.sort(key=lambda row: (row['block_number'], row['transaction_index'], row['log_index']))
    return events, first_seen


def replay_events(worker, token, kind, profile, modern, state, events):
    if token == GHO_TOKEN:
        implementation = profile['implementation'].lower()
        kernel = Path(gho_replay.__file__).with_name('gho-kernel.json')
        supported = set(json.loads(kernel.read_text())['implementations']) | gho_replay.STANDARD_IMPLEMENTATIONS
        if implementation not in supported:
            raise ValueError(f'Unverified GHO implementation: {implementation}')
        initial = {}
        for owner in {row[field] for row in events
                      for field in ('arg__onBehalfOf', 'arg__from', 'arg__user') if row.get(field)}:
            shares = state['positions'].get(owner, {}).get(token, '0')
            if int(shares) and owner not in state['gho']:
                raise ValueError(f'Opening GHO discount state missing for {owner}')
            index, discount = state['gho'].get(owner, ('0', '0'))
            initial[owner] = dict(shares=shares, previous_index=index, discount_percent=discount)
        return worker.call(dict(operation='replay', initial=initial, events=events,
                                profiles={str(profile['version']): implementation},
                                modern_versions=modern))
    initial = {owner: state['positions'].get(owner, {}).get(token, '0')
               for row in events for field in ('arg__onBehalfOf', 'arg__from', 'arg__to')
               if (owner := row.get(field))}
    return worker.call(dict(initial=initial, events=events, modern_versions=modern,
                            debt=kind == 'variableDebtToken'))


def replay(window, boundary, historical_root, jobs, rows_by_token, implementations):
    """Compute sparse deltas. The caller supplies Node-verified token routes."""
    scope = window['identity']
    headers = {header['block']: header['hash'] for header in window['headers']}
    states = {}
    first_seen = {}
    workers = {}
    with ExitStack() as stack:
        for token, rows in sorted(rows_by_token.items()):
            job = jobs[token]
            profile_path = historical_root / 'token-profiles' / (token + '.profiles.json')
            profile = json.loads(profile_path.read_text())
            version = max((item for item in profile['profiles'] if item['block'] < scope['start']),
                          key=lambda item: (item['block'], item['log_index']))
            if version['implementation'].lower() != implementations[token].lower():
                raise ValueError(f'Token implementation differs from the sealed profile: {token}')
            abi = profile_path.parent / version['abi']
            events, sightings = decode_logs(rows, token, version['version'], abi, headers)
            for owner, block in sightings.items():
                first_seen[owner] = min(block, first_seen.get(owner, block))
            if not events:
                continue
            kind = job['kind']
            if kind == 'stableDebtToken':
                raise ValueError(f'Stable-debt activity requires a verified adapter: {token}')
            worker_class = gho_replay.GhoMath if token == GHO_TOKEN else Math
            if worker_class not in workers:
                workers[worker_class] = worker_class()
                stack.callback(workers[worker_class].close)
            result = replay_events(workers[worker_class], token, kind, version,
                                   modern_versions(profile, historical_root), boundary['state'], events)
            states[token] = dict(result, profile_sha256=checksum(profile_path), abi_sha256=checksum(abi))
    return dict(tokens=states, first_seen=first_seen)


def validate_scaled(w3, window, result):
    """Check the last scaled value per holder in each changed block via RETH."""
    scope = window['identity']
    check_anchors(w3, scope)
    checks = {}
    for token, item in result['tokens'].items():
        abi = [function('scaledBalanceOf', ['address'], ['uint256'])]
        contract = w3.eth.contract(address=Web3.to_checksum_address(token), abi=abi)
        expected = {(row['block'], row['owner']): row['after'] for row in item['history']}

        def compare(sample):
            (block, owner), scaled = sample
            actual = contract.functions.scaledBalanceOf(Web3.to_checksum_address(owner)).call(
                block_identifier=block)
            if int(scaled) != actual:
                raise ValueError(f'Scaled balance differs at block {block}: {token}/{owner}')

        with ThreadPoolExecutor(max_workers=8) as executor:
            list(executor.map(compare, expected.items()))
        checks[token] = len(expected)
    check_anchors(w3, scope)
    return dict(all_exact=True, comparisons=sum(checks.values()), per_token=checks)


def load_inputs(window_path, state_path, raw_manifest, historical_root):
    window = json.loads(window_path.read_text())
    scope = validate_window(window)
    boundary = read_boundary(state_path, scope['checkpoint_sha256'], scope['start'],
                             scope['predecessor_hash'], scope['release_generation'])
    plan_path = historical_root / 'plan.json'
    plan = json.loads(plan_path.read_text())
    if (plan['end_block'], plan['end_hash']) != (scope['start']-1, scope['predecessor_hash']):
        raise ValueError('Historical token registry does not join the HF window')
    jobs = {job['address'].lower(): job for job in plan['jobs']}
    rows = read_token_inputs(window, raw_manifest, jobs)
    identity = dict(window_sha256=window['window_sha256'], checkpoint_sha256=scope['checkpoint_sha256'],
                    raw_manifest_sha256=checksum(raw_manifest), registry_sha256=checksum(plan_path))
    return window, boundary, jobs, rows, identity


def run(window_path, state_path, raw_manifest, historical_root, output, w3):
    window, boundary, jobs, rows, identity = load_inputs(window_path, state_path, raw_manifest, historical_root)
    routes = verify_routes(w3, window, jobs)
    result = replay(window, boundary, historical_root, jobs, rows, routes['implementations'])
    validation = validate_scaled(w3, window, result)
    payload = dict(format='aave-v3-hf-token-replay-v1', **identity,
                   routes=routes, result=result, validation=validation)
    publish(output, payload)
    return dict(path=str(output), sha256=checksum(output), raw_events=sum(map(len, rows.values())),
                token_delta_streams=len(result['tokens']), discovered_wallets=len(result['first_seen']),
                scaled_balance_comparisons=validation['comparisons'])


def publish(output, payload):
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.with_suffix('.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if output.exists():
            if json.loads(output.read_text()) != payload:
                raise ValueError('Existing token replay artifact differs')
            return
        write_json(output, payload)


def reproduce(window_path, state_path, raw_manifest, historical_root, output, reference, reference_sha256):
    window, boundary, jobs, rows, identity = load_inputs(window_path, state_path, raw_manifest, historical_root)
    if checksum(reference) != reference_sha256:
        raise ValueError('Node reference checksum differs')
    expected = json.loads(reference.read_text())
    if (expected['format'] != 'aave-v3-hf-token-replay-v1'
            or any(expected[key] != value for key, value in identity.items())
            or expected['routes']['window_sha256'] != window['window_sha256']
            or not expected['validation']['all_exact']):
        raise ValueError('Node reference does not accept this token input')
    result = replay(window, boundary, historical_root, jobs, rows, expected['routes']['implementations'])
    if result != expected['result']:
        raise ValueError('Offline token replay does not reproduce the Node result')
    evidence = dict(format='aave-v3-hf-token-reproduction-v1', **identity,
                    result_sha256=digest(result), reference_sha256=reference_sha256,
                    raw_events=sum(map(len, rows.values())), token_delta_streams=len(result['tokens']),
                    exact_match=True)
    publish(output, evidence)
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ('window', 'state', 'raw-manifest', 'historical-root', 'output'):
        parser.add_argument('--'+flag, required=True, type=Path)
    parser.add_argument('--reference', type=Path)
    parser.add_argument('--reference-sha256')
    args = parser.parse_args()
    inputs = (args.window, args.state, args.raw_manifest, args.historical_root, args.output)
    if bool(args.reference) != bool(args.reference_sha256):
        parser.error('--reference and --reference-sha256 must be specified together')
    if args.reference:
        evidence = reproduce(*inputs, args.reference, args.reference_sha256)
    else:
        w3 = Web3(HTTPProvider('http://127.0.0.1:8545', request_kwargs={'timeout': 60}))
        evidence = run(*inputs, w3)
    print(json.dumps(evidence))


if __name__ == '__main__':
    main()
