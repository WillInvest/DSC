"""Verify raw token input and prove an Aave HF window stays on frozen routes."""
from concurrent.futures import ThreadPoolExecutor
import fcntl
import json
import os
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory

import pyarrow.parquet as pq
from eth_utils.abi import event_abi_to_log_topic
from web3 import Web3

from decode_tokens import UPGRADED
from decoding.pool_decode import cryo_event_log
from oracle_common import POOL, POOL_ABI
from registry.build_aave_profiles import implementation
from shared.artifacts import checksum, digest, write_json

ABIS = Path(__file__).resolve().parents[1] / 'reconstruction' / 'abis'
PROVIDER_CHANGES = {'PoolUpdated', 'PoolConfiguratorUpdated', 'AddressSet',
                    'AddressSetAsProxy', 'ProxyCreated'}
TOKEN_CHANGES = {'ReserveInitialized', 'ReserveDropped', 'ATokenUpgraded',
                 'VariableDebtTokenUpgraded', 'StableDebtTokenUpgraded'}
LOG_CHUNK = 128


def log_identity(log, headers, addresses):
    def hex_text(value):
        return value.lower() if isinstance(value, str) else Web3.to_hex(value).lower()

    block, address = log['blockNumber'], log['address'].lower()
    if address not in addresses or headers.get(block) != hex_text(log['blockHash']):
        raise ValueError('Token log does not match the reviewed address and block lineage')
    return (block, log['transactionIndex'], log['logIndex'], address,
            hex_text(log['transactionHash']), tuple(hex_text(topic) for topic in log['topics']),
            hex_text(log['data']))


def collect_raw_tokens(window, jobs, output, w3, cryo):
    """Extract with Cryo and prove completeness, including empty chunks, with RETH."""
    scope = window['identity']
    start, stop = scope['start'], scope['stop']
    headers = {h['block']: h['hash'] for h in window['headers']}
    addresses = sorted(map(Web3.to_checksum_address, jobs))
    receipt_name = 'registered-token-inputs.json'
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.with_suffix('.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if output.exists():
            read_token_inputs(window, output/receipt_name, jobs)
            return json.loads((output/receipt_name).read_text())
        check_anchors(w3, scope)
        with TemporaryDirectory(dir=output.parent) as temporary:
            stage = Path(temporary)/'publish'
            raw, reports = stage/'raw', stage/'reports'
            raw.mkdir(parents=True)
            reports.mkdir()
            command = [str(cryo), 'logs', '--rpc', 'http://127.0.0.1:8545',
                       '--contract', *addresses, '--blocks', f'{start}:{stop}',
                       '--chunk-size', str(LOG_CHUNK), '--inner-request-size', str(LOG_CHUNK),
                       '--max-concurrent-requests', '4', '--include-columns', 'block_hash',
                       '--hex', '--output-dir', str(raw), '--report-dir', str(reports)]
            with (stage/'cryo.log').open('wb') as log:
                subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=900)
            actual, files = [], []
            for path in sorted(raw.glob('*.parquet')):
                parquet = pq.ParquetFile(path)
                files.append(dict(path=str(path.relative_to(stage)), sha256=checksum(path), rows=parquet.metadata.num_rows))
                for batch in parquet.iter_batches():
                    actual.extend(log_identity(cryo_event_log(row), headers, jobs) for row in batch.to_pylist())
            reference = []
            for first in range(start, stop, LOG_CHUNK):
                rows = w3.eth.get_logs(dict(address=addresses, fromBlock=first, toBlock=min(stop, first+LOG_CHUNK)-1))
                reference.extend(log_identity(log, headers, jobs) for log in rows)
            if sorted(actual) != sorted(reference) or len(set(actual)) != len(actual):
                raise ValueError('Cryo token log coverage differs from RETH')
            check_anchors(w3, scope)
            receipt = dict(window_sha256=window['window_sha256'], start=start, stop=stop,
                           addresses=len(addresses), historical_registry_sha256=digest(addresses),
                           historical_registry_only=True, exact_reth_comparison=True, events=len(actual), files=files)
            for item in files:
                with (stage/item['path']).open('rb') as file:
                    os.fsync(file.fileno())
            write_json(stage/receipt_name, receipt)
            stage.rename(output)
            parent = os.open(output.parent, os.O_RDONLY)
            try:
                os.fsync(parent)
            finally:
                os.close(parent)
        return receipt


def read_token_inputs(window, manifest_path, jobs):
    scope = window['identity']
    manifest = json.loads(manifest_path.read_text())
    if (manifest['window_sha256'] != window['window_sha256']
            or (manifest['start'], manifest['stop']) != (scope['start'], scope['stop'])
            or not manifest['exact_reth_comparison'] or manifest['addresses'] != len(jobs)
            or manifest['historical_registry_sha256'] != digest(sorted(map(Web3.to_checksum_address, jobs)))):
        raise ValueError('Raw token manifest scope differs')
    rows = {}
    for item in manifest['files']:
        source = (manifest_path.parent / item['path']).resolve()
        if (not source.is_relative_to(manifest_path.parent.resolve()) or checksum(source) != item['sha256']):
            raise ValueError('Raw token file path or checksum differs')
        parquet = pq.ParquetFile(source)
        if parquet.metadata.num_rows != item['rows']:
            raise ValueError('Raw token file row count differs')
        for batch in parquet.iter_batches():
            for row in batch.to_pylist():
                token = row['address'].lower()
                if token not in jobs:
                    raise ValueError('Unexpected token outside the historical registry')
                rows.setdefault(token, []).append(row)
    if sum(map(len, rows.values())) != manifest['events']:
        raise ValueError('Raw token manifest event count differs')
    return rows


def check_anchors(w3, scope):
    if (w3.eth.chain_id != scope['chain_id'] or w3.eth.get_block('finalized').number < scope['stop']-1
            or w3.eth.get_block(scope['start']-1).hash.to_0x_hex().lower() != scope['predecessor_hash']
            or w3.eth.get_block(scope['stop']-1).hash.to_0x_hex().lower() != scope['end_hash']):
        raise ValueError('HF token input anchors differ from finalized RETH')


def no_changes(w3, address, abi, names, scope):
    topics = [Web3.to_hex(event_abi_to_log_topic(item)) for item in abi
              if item['type'] == 'event' and item['name'] in names]
    rows = w3.eth.get_logs(dict(address=address, fromBlock=scope['start'],
                              toBlock=scope['stop']-1, topics=[topics]))
    if rows:
        raise ValueError(f'Market routing changed at {address}; review the ABI and token registry')


def verify_routes(w3, window, jobs):
    scope = window['identity']
    check_anchors(w3, scope)
    boundaries = (scope['start']-1, scope['stop']-1)
    provider_abi = json.loads((ABIS / 'v3-IPoolAddressesProvider.json').read_text())
    config_abi = json.loads((ABIS / 'v3-IPoolConfigurator.json').read_text())
    pool = w3.eth.contract(address=POOL, abi=POOL_ABI)
    provider, closing_provider = [pool.functions.ADDRESSES_PROVIDER().call(block_identifier=height)
                                  for height in boundaries]
    if provider != closing_provider:
        raise ValueError('Pool address provider changed')
    registry = w3.eth.contract(address=provider, abi=provider_abi)
    config, closing_config = [registry.functions.getPoolConfigurator().call(block_identifier=height)
                              for height in boundaries]
    active, closing_active = [set(pool.functions.getReservesList().call(block_identifier=height))
                              for height in boundaries]
    if config != closing_config or active != closing_active:
        raise ValueError('Configurator routing or reserve list changed')
    if {asset.lower() for asset in active} - {job['asset'].lower() for job in jobs.values()}:
        raise ValueError('Active reserve missing from the historical token registry')
    no_changes(w3, POOL, [UPGRADED], {'Upgraded'}, scope)
    no_changes(w3, provider, provider_abi, PROVIDER_CHANGES, scope)
    no_changes(w3, config, [*config_abi, UPGRADED], TOKEN_CHANGES | {'Upgraded'}, scope)

    def verify_token(token):
        address = Web3.to_checksum_address(token)
        opening, closing = [implementation(w3, address, height).lower() for height in boundaries]
        if opening != closing:
            raise ValueError(f'Token implementation changed: {token}')
        return token, opening

    with ThreadPoolExecutor(max_workers=8) as executor:
        implementations = dict(executor.map(verify_token, sorted(jobs)))
    check_anchors(w3, scope)
    return dict(window_sha256=window['window_sha256'], pool=POOL, provider=provider,
                configurator=config, active_assets=sorted(active), implementations=implementations)
