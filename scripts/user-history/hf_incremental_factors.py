"""Assemble the verified Pool, configuration, position and price inputs for HF."""
import argparse
import json
from pathlib import Path
import subprocess
import sys

import pyarrow.parquet as pq
from web3 import Web3

import accounting_node
from accounting_inputs import event_rows, profile_at, profiles
from decoding.pool_decode import EventDecoder
from hf_incremental_positions import GHO_TOKEN, publish
from hf_incremental_token_inputs import ABIS, check_anchors
from hf_incremental_window import validate_window
from native_batch import FLAGS
from oracle_common import POOL
from registry.build_aave_profiles import implementation
from shared.artifacts import checksum

POOL_EVENTS = {*FLAGS, 'ReserveDataUpdated'}
RISK_PROOF = Path('/home/dsc-data/artifacts/aave-risk-inputs-20261114/configurator-abi-proof.json')
GHO_ASSET = '0x40d16fc0246ad3160ccc09b8d0d3a2cd28ae6c2f'


def pinned_json(path, window_sha256):
    record = json.loads(path.read_text())
    if record['window_sha256'] != window_sha256:
        raise ValueError(f'Input does not belong to the pinned HF window: {path}')
    return record


def load_prices(path, window):
    scope = window['identity']
    receipt = pinned_json(path, window['window_sha256'])
    prices = {}
    for item in receipt['receipts']:
        source = Path(item['path'])
        if checksum(source) != item['sha256']:
            raise ValueError('Oracle job receipt checksum differs')
        manifest = json.loads(source.read_text())
        assets = [asset.lower() for asset in manifest['job']['assets']]
        for file in manifest['files']:
            parquet = Path(manifest['decoded_root']) / file['name']
            if checksum(parquet) != file['decoded_sha256']:
                raise ValueError('Oracle file checksum differs')
            for row in pq.read_table(parquet, columns=['block_number', 'prices']).to_pylist():
                block = row['block_number']
                if block in prices or len(row['prices']) != len(assets):
                    raise ValueError('Duplicate Oracle block or invalid price vector')
                prices[block] = dict(assets=assets, values=row['prices'])
    if set(prices) != set(range(scope['start'], scope['stop'])):
        raise ValueError('Oracle block coverage differs')
    return prices


def pool_rows(path, window, abi):
    scope = window['identity']
    receipt = pinned_json(path, window['window_sha256'])
    source = path.with_name('pool-events.parquet')
    if (not receipt['exact_reth_comparison'] or checksum(source) != receipt['sha256']
            or (receipt['start'], receipt['stop']) != (scope['start'], scope['stop'])):
        raise ValueError('Pool input evidence differs')
    by_topic = {}
    for item in abi:
        if item['type'] == 'event' and item['name'] in POOL_EVENTS:
            event = Web3().eth.contract(abi=[item]).get_event_by_name(item['name'])()
            by_topic[event.topic] = EventDecoder([item])
    return sorted(event_rows([source], by_topic, POOL_EVENTS, scope['start'], scope['stop']))


def risk_tasks(w3, address, window, profile, tokens):
    scope = window['identity']
    headers = {h['block']: h['hash'] for h in window['headers']}
    abi = json.loads((ABIS / 'v3-IPoolConfigurator.json').read_text()) + json.loads(RISK_PROOF.read_text())['abi']
    contract = w3.eth.contract(address=address, abi=[])
    events = {}
    for item in abi:
        if item['type'] == 'event':
            event = w3.eth.contract(address=address, abi=[item]).get_event_by_name(item['name'])()
            events[event.topic] = event
    logs = w3.eth.get_logs(dict(address=contract.address, fromBlock=scope['start'], toBlock=scope['stop']-1))
    tasks = {}

    def add(kind, key, block, **kwargs):
        tasks[kind, key, block] = dict(kind=kind, block=block, profile=profile['key'], **kwargs)

    for log in logs:
        block = log['blockNumber']
        if headers.get(block) != log['blockHash'].to_0x_hex().lower():
            raise ValueError('Configurator log is outside the pinned lineage')
        topic = Web3.to_hex(log['topics'][0])
        if topic not in events:
            raise ValueError(f'Configurator event requires a reviewed ABI: {topic}')
        event = events[topic].process_log(log)
        if event.event != 'CollateralConfigurationChanged' and 'EMode' not in event.event:
            continue
        if 'asset' in event.args:
            key = event.args['asset'].lower()
            add('reserve', key, block, asset=key)
        if 'categoryId' in event.args:
            category = int(event.args['categoryId'])
            add('mode', category, block, category=category)
    for row in tokens.get(GHO_TOKEN, {}).get('gho_history', []):
        add('call', row['owner'], row['block'], function='getUserConfiguration', args=[row['owner']])
    return list(tasks.values()), len(logs)


def snapshots(window, profile, tasks, boundary):
    scope = window['identity']
    headers = {h['block']: h['hash'] for h in window['headers']}
    request = dict(operation='snapshots', end_block=scope['stop']-1, end_hash=scope['end_hash'],
                   pool=POOL, abis={profile['key']: profile['abi']}, tasks=tasks)
    if not tasks:
        return []
    response = subprocess.run([sys.executable, accounting_node.__file__],
                              input=json.dumps(request), capture_output=True, text=True, check=True, timeout=900)
    rows = json.loads(response.stdout)
    output = []
    for task, row in zip(tasks, rows, strict=True):
        if any(row[key] != value for key, value in task.items()) or row['block_hash'] != headers[row['block']]:
            raise ValueError('Configuration snapshot identity differs')
        kind = row['kind']
        if kind == 'call':
            reserve_id = int(boundary['reserves'][GHO_ASSET]['id'])
            key = task['args'][0]
            row['borrowing'] = bool((int(row['result'][0]) >> (2*reserve_id)) & 1)
            kind = 'gho_borrowing'
        else:
            key = row['asset'] if kind == 'reserve' else str(row['category'])
        output.append((kind, key, row['block'], row))
    return output


def collect(window_path, state_path, pool_path, oracle_path, token_path, output):
    from hf_incremental_boundary import read_boundary

    window = json.loads(window_path.read_text())
    scope = validate_window(window)
    boundary = read_boundary(state_path, scope['checkpoint_sha256'], scope['start'],
                             scope['predecessor_hash'], scope['release_generation'])
    w3 = accounting_node.web()
    check_anchors(w3, scope)
    tokens = pinned_json(token_path, window['window_sha256'])
    if not tokens['validation']['all_exact'] or tokens['checkpoint_sha256'] != scope['checkpoint_sha256']:
        raise ValueError('Token replay prerequisite was not verified')
    profile = profile_at(profiles(), scope['start']-1)
    if any(implementation(w3, POOL, height).lower() != profile['implementation'].lower()
           for height in (scope['start']-1, scope['stop']-1)):
        raise ValueError('Pool implementation differs from the historical profile')
    events = pool_rows(pool_path, window, profile['abi'])
    prices = load_prices(oracle_path, window)
    if any(set(value['assets']) != {a.lower() for a in tokens['routes']['active_assets']}
           for value in prices.values()):
        raise ValueError('Oracle asset inventory differs from the stable reserve route')
    tasks, config_logs = risk_tasks(w3, tokens['routes']['configurator'], window, profile,
                                    tokens['result']['tokens'])
    configuration = snapshots(window, profile, tasks, boundary)
    check_anchors(w3, scope)
    # Convert indexed prices and row tuples to their exact JSON representation for retry equality.
    evidence = dict(format='aave-v3-hf-incremental-factors-v1',
                    window_sha256=window['window_sha256'], checkpoint_sha256=scope['checkpoint_sha256'],
                    token_sha256=checksum(token_path), pool_sha256=checksum(pool_path),
                    oracle_sha256=checksum(oracle_path), profile=profile['key'],
                    prices={str(block): value for block, value in prices.items()},
                    pool_events=[list(row) for row in events], snapshots=[list(row) for row in configuration],
                    configurator_logs=config_logs, risk_abi_proof_sha256=checksum(RISK_PROOF))
    publish(output, evidence)
    return dict(path=str(output), sha256=checksum(output), pool_events=len(events),
                oracle_blocks=len(prices), configuration_snapshots=len(configuration))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ('window', 'state', 'pool', 'oracle', 'tokens', 'output'):
        parser.add_argument('--'+flag, required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(collect(args.window, args.state, args.pool, args.oracle, args.tokens, args.output)))


if __name__ == '__main__':
    main()
