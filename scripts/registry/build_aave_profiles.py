#!/usr/bin/env python3
"""Build bounded Aave profiles using Blockscout, RETH and Sourcify verified ABIs.

Run with the project's ABI environment. Nothing is approved from an address book
alone: match downloaded runtime bytecode to RETH and validate boundary calls/logs.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys

import requests
from web3 import Web3
from eth_utils.abi import event_abi_to_log_topic

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from protocols.protocol_config import load_protocol
from protocols.deployment import IMPLEMENTATION_SLOT, boundary_event, verify_deployment
from decoding.pool_decode import StateDecoder
from registry.catalog import registry


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + '\n')
    return hashlib.sha256(path.read_bytes()).hexdigest()


def implementation(w3, address, block):
    return Web3.to_checksum_address(bytes(w3.eth.get_storage_at(
        address, IMPLEMENTATION_SLOT, block_identifier=block))[-20:])


def upgrades(w3, address, start, stop, evidence):
    """Discover with Blockscout's index, then verify every boundary on RETH."""
    topic = Web3.to_hex(Web3.keccak(text='Upgraded(address)'))
    url = f'https://eth.blockscout.com/api/v2/addresses/{address}/logs'
    params = {'topic': topic}
    pages, indexed = [], []
    while True:
        response = requests.get(url, params=params, timeout=90)
        response.raise_for_status()
        page = response.json()
        pages.append(page)
        for log in page['items']:
            if log['address']['hash'].lower() != address.lower() or log['topics'][0] != topic:
                raise ValueError('upgrade index returned the wrong address or topic')
            if start <= log['block_number'] < stop:
                indexed.append(log)
        cursor = page['next_page_params']
        if not cursor:
            break
        params = dict(cursor, topic=topic)
    write(evidence / 'upgrade-index.json', dict(url=url, start=start, stop=stop, pages=pages))
    result = []
    for block in sorted({log['block_number'] for log in indexed}):
        canonical = w3.eth.get_logs(dict(address=address, fromBlock=block, toBlock=block, topics=[topic]))
        remote = {(r['index'], r['transaction_hash'], r['block_hash'], tuple(t for t in r['topics'] if t))
                  for r in indexed if r['block_number'] == block}
        local = {(r['logIndex'], Web3.to_hex(r['transactionHash']), Web3.to_hex(r['blockHash']),
                  tuple(Web3.to_hex(t) for t in r['topics'])) for r in canonical}
        if remote != local:
            raise ValueError(f'upgrade index differs from RETH at {block}')
        result.extend(canonical)
    print(f'  {len(result)} indexed upgrades verified on RETH', flush=True)
    return result


def verified_abi(w3, address, block, directory):
    path = directory / (address.lower() + '.json')
    url = f'https://sourcify.dev/server/v2/contract/1/{address}?fields=abi,runtimeBytecode.onchainBytecode,metadata'
    if path.exists():
        record = json.loads(path.read_text())
    else:
        response = requests.get(url, timeout=90)
        response.raise_for_status()
        record = response.json()
    if record.get('runtimeMatch') not in ('match', 'exact_match'):
        raise ValueError(f'No verified runtime for {address}')
    runtime = bytes(w3.eth.get_code(address, block))
    if not runtime or runtime != bytes.fromhex(record['runtimeBytecode']['onchainBytecode'][2:]):
        raise ValueError(f'Sourcify/RETH runtime mismatch for {address}')
    digest = write(path, record)
    return record['abi'], dict(address=address, url=url, record_sha256=digest,
                              runtime_sha256=hashlib.sha256(runtime).hexdigest())


def extraction_abi(current, event_sources):
    events = {}
    for source in event_sources:
        for entry in source:
            if entry['type'] != 'event':
                continue
            topic = Web3.to_hex(event_abi_to_log_topic(entry))
            prior = events.get(topic)
            shape = lambda e: [(i['name'], i['type'], i['indexed']) for i in e['inputs']]
            if prior and shape(prior) != shape(entry):
                raise ValueError(f'Conflicting historical event layout: {entry["name"]}')
            events[topic] = entry
    if len({e['name'] for e in events.values()}) != len(events):
        raise ValueError('Historical event overload requires explicit schema handling')
    return [e for e in current if e['type'] != 'event'] + list(events.values())


def validate_sample(w3, config, profile, block):
    verify_deployment(w3, profile, range(block, block+1))
    abi = json.loads(config['abi'].read_text())
    contract = w3.eth.contract(address=Web3.to_checksum_address(profile['address']), abi=abi)
    decoder = StateDecoder(abi, config['state_getters'])
    assets = contract.functions.getReservesList().call(block_identifier=block)
    for asset in assets:
        call = contract.encode_abi('getReserveData', args=[asset])
        raw = w3.eth.call({'to': contract.address, 'data': call}, block_identifier=block)
        decoder.decode(dict(call_data=call, output_data=Web3.to_hex(raw), block_number=block))
    logs = w3.eth.get_logs(dict(address=contract.address, fromBlock=block, toBlock=block))
    for log in logs:
        contract.get_event_by_topic(Web3.to_hex(log['topics'][0]))().process_log(log)
    return dict(block=block, reserves=len(assets), events=len(logs), block_hash=Web3.to_hex(w3.eth.get_block(block).hash))


def build(args):
    w3 = Web3(Web3.HTTPProvider(args.rpc, request_kwargs={'timeout':120}))
    if w3.eth.chain_id != 1:
        raise ValueError('Ethereum mainnet required')
    head = w3.eth.get_block('finalized')
    cutoff = args.to_block if args.to_block is not None else head.number
    if cutoff > head.number:
        raise ValueError('cutoff must be finalized')
    audit = dict(cutoff=cutoff, cutoff_hash=Web3.to_hex(w3.eth.get_block(cutoff).hash), pools=[])
    for row in registry(args.contracts):
        if row['protocol'] not in ('aave-v2','aave-v3'):
            continue
        address = Web3.to_checksum_address(row['address'])
        config = load_protocol(row['protocol'])
        directory = config['abi'].parent
        evidence = directory / 'history' / 'sources'
        proxy, proxy_source = verified_abi(w3, address, cutoff, evidence)
        print(f'{row["protocol"]}: scanning upgrade logs', flush=True)
        logs = upgrades(w3, address, row['creation_block'], cutoff+1, evidence)
        bounds = sorted({row['creation_block'], cutoff+1, *(l['blockNumber'] for l in logs)})
        pool_audit = dict(protocol=row['protocol'], address=address, proxy_source=proxy_source, profiles=[])
        for index, (start, stop) in enumerate(zip(bounds, bounds[1:]), 1):
            impl = implementation(w3, address, start)
            previous = implementation(w3, address, start-1)
            boundary_logs = [log for log in logs if log['blockNumber'] == start]
            abi, source = verified_abi(w3, impl, start, evidence)
            event_sources = [proxy, abi]
            sources = [source]
            # Events earlier in an upgrade block can come from the old code;
            # include intermediate implementations if upgraded twice in a block.
            others = {previous, *(boundary_event(log)['implementation'] for log in boundary_logs)} - {impl, '0x'+'00'*20}
            for other in sorted(others):
                old_abi, old_source = verified_abi(w3, other, start, evidence)
                event_sources.append(old_abi)
                sources.append(old_source)
            relative = f'history/{start}.json'
            abi_hash = write(directory / relative, extraction_abi(abi, event_sources))
            profile = dict(protocol=row['protocol'], chain_id=1, address=address,
                from_block=start, to_block=stop, implementation=impl,
                previous_implementation=previous, boundary_upgrades=[boundary_event(l) for l in boundary_logs],
                implementation_code_sha256=source['runtime_sha256'], abi=relative,
                abi_sha256=abi_hash, schema_version=index, sources=sources)
            selected = load_protocol(row['protocol'], profile)
            samples = [validate_sample(w3, selected, profile, n) for n in sorted({start, stop-1})]
            name = f'{row["protocol"]}-{start}.json'
            write(args.output / name, profile)
            pool_audit['profiles'].append(dict(file=name, samples=samples))
            print(f'{name}: [{start}, {stop}) {impl}; samples passed', flush=True)
        audit['pools'].append(pool_audit)
    write(args.output / 'evidence' / 'validation.json', audit)
    print(f'Profiles validated through finalized block {cutoff}', flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--contracts', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--rpc', default=os.environ.get('ETH_RPC_URL','http://127.0.0.1:8545'))
    parser.add_argument('--to-block', type=int)
    build(parser.parse_args())
