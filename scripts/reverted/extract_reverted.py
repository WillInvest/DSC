#!/usr/bin/env python3
"""Finalized failed-receipt/call-trace extraction. Run on dsc-node; Web3.py handles RPC."""
import argparse
import concurrent.futures
import fcntl
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import tempfile
import time

OPTIONS = {'tracer': 'callTracer', 'tracerConfig': {'onlyTopCall': False, 'withLog': False}, 'timeout': '120s'}
SCHEMA = 1


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def save(path, payload):
    """One atomic checksummed gzip artifact; interrupted writes never look complete."""
    path.parent.mkdir(parents=True, exist_ok=True)
    envelope = {'sha256': hashlib.sha256(encoded(payload)).hexdigest(), 'payload': payload}
    fd, temp = tempfile.mkstemp(prefix='.partial-', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(gzip.compress(encoded(envelope), mtime=0))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)


def load(path):
    envelope = json.loads(gzip.decompress(path.read_bytes()))
    payload = envelope['payload']
    if hashlib.sha256(encoded(payload)).hexdigest() != envelope['sha256']:
        raise ValueError(f'Checksum mismatch: {path}')
    return payload


def make_rpc(url):
    # Lazy imports keep offline archive selection free of an RPC dependency.
    from requests.exceptions import ConnectionError, HTTPError, Timeout
    from web3 import HTTPProvider
    from web3.providers.rpc.utils import ExceptionRetryConfiguration

    methods = ['eth_chainId', 'web3_clientVersion', 'eth_getBlockByNumber',
               'eth_getBlockReceipts', 'debug_traceTransaction', 'debug_traceBlockByNumber']
    provider = HTTPProvider(url, request_kwargs={'timeout': (10, 150)},
                            exception_retry_configuration=ExceptionRetryConfiguration(
                                errors=(ConnectionError, HTTPError, Timeout),
                                retries=3, backoff_factor=1.0, method_allowlist=methods))

    def call(method, params):
        # Provider-level API preserves raw RPC hex strings and all result fields.
        response = provider.make_request(method, params)
        if response.get('error') is not None or response.get('result') is None:
            raise RuntimeError(f'{method}: RPC error or missing result')
        return response['result']

    return call


def choose(receipts, multiplier=1.0):
    failed = [r for r in receipts if int(r['status'], 16) == 0]
    score = sum(int(r['transactionIndex'], 16) + 1 for r in failed)
    strategy = 'skip' if not failed else ('block' if score > multiplier * len(receipts) else 'individual')
    return failed, score, strategy


def validate_receipts(receipts, header):
    hashes = header['transactions']
    if len(receipts) != len(hashes):
        raise ValueError('Incomplete block receipts')
    if len(set(hashes)) != len(hashes):
        raise ValueError('Duplicate block transaction hashes')
    receipts = sorted(receipts, key=lambda r: int(r['transactionIndex'], 16))
    for i, receipt in enumerate(receipts):
        if (int(receipt['transactionIndex'], 16) != i or receipt['transactionHash'] != hashes[i]
                or receipt['blockHash'] != header['hash'] or receipt['blockNumber'] != header['number']
                or int(receipt['status'], 16) not in (0, 1)):
            raise ValueError('Receipt identity/status mismatch')
    return receipts


def validate_trace(trace):
    if not isinstance(trace, dict) or not all(k in trace for k in ('type', 'from', 'gas', 'gasUsed', 'input')):
        raise ValueError('Missing or invalid callTracer result')
    for child in trace.get('calls', []):
        validate_trace(child)


def validate_archive(payload):
    if payload['schema'] != SCHEMA or payload['tracer_options'] != OPTIONS:
        raise ValueError('Unsupported extraction configuration')
    receipts = validate_receipts(payload['receipts'], payload['header'])
    failed, score, strategy = choose(receipts, payload['multiplier'])
    if score != payload['score'] or strategy != payload['strategy']:
        raise ValueError('Invalid strategy metadata')
    expected = [r['transactionHash'] for r in failed]
    if [r['receipt']['transactionHash'] for r in payload['failed']] != expected:
        raise ValueError('Missing or duplicate failed traces')
    for entry, receipt in zip(payload['failed'], failed):
        if entry['receipt'] != receipt:
            raise ValueError('Failed receipt mismatch')
        validate_trace(entry['trace'])
        if not entry['trace'].get('error'):
            raise ValueError('Failed receipt lacks root trace error')
    return payload


def extract_block(number, output, multiplier, call, chain, client):
    started = time.monotonic()
    header = call('eth_getBlockByNumber', [hex(number), False])
    if int(header['number'], 16) != number:
        raise ValueError('Wrong block returned')
    path = output / 'blocks' / f'{number // 1000:09d}' / f'{number}.json.gz'
    if path.exists():
        previous = validate_archive(load(path))
        if previous['header']['hash'] != header['hash'] or previous['chain_id'] != chain:
            raise ValueError(f'Chain/hash changed for {number}; preserve archive and investigate')
        return {'block': number, 'strategy': previous['strategy'], 'resumed': True}
    receipts = validate_receipts(call('eth_getBlockReceipts', [hex(number)]), header)
    failed, score, strategy = choose(receipts, multiplier)
    cache = output / '.work' / header['hash']
    cache_config = {'schema': SCHEMA, 'chain_id': chain, 'tracer_options': OPTIONS}

    def cached(name, method, params):
        checkpoint = cache / f'{name}.json.gz'
        if checkpoint.exists():
            data = load(checkpoint)
            if data['config'] != cache_config:
                raise ValueError('Checkpoint configuration mismatch')
            return data['result']
        result = call(method, params)
        save(checkpoint, {'config': cache_config, 'result': result})
        return result

    traces = {}
    if strategy == 'block':
        results = cached('block', 'debug_traceBlockByNumber', [hex(number), OPTIONS])
        if len(results) != len(receipts):
            raise ValueError('Incomplete whole-block trace')
        for receipt, item in zip(receipts, results):
            # Some RETH versions omit txHash; ordered block results are then used.
            if item.get('txHash', receipt['transactionHash']) != receipt['transactionHash']:
                raise ValueError('Trace transaction hash mismatch')
            if item.get('error') or 'result' not in item:
                raise ValueError('Tracing failed; this is not a transaction revert')
            if int(receipt['status'], 16) == 0:
                traces[receipt['transactionHash']] = item['result']
    elif strategy == 'individual':
        for receipt in failed:
            tx = receipt['transactionHash']
            traces[tx] = cached(tx, 'debug_traceTransaction', [tx, OPTIONS])
    if call('eth_getBlockByNumber', [hex(number), False])['hash'] != header['hash']:
        raise ValueError('Block hash changed during tracing')
    payload = {'schema': SCHEMA, 'chain_id': chain, 'client_version': client,
               'tracer_options': OPTIONS, 'header': header, 'receipts': receipts,
               'multiplier': multiplier, 'score': score, 'strategy': strategy,
               'elapsed_seconds': time.monotonic() - started,
               'failed': [{'receipt': r, 'trace': traces[r['transactionHash']]} for r in failed]}
    validate_archive(payload)
    save(path, payload)
    # Remove only disposable replay checkpoints after the durable block commit.
    for checkpoint in cache.glob('*.json.gz'):
        checkpoint.unlink()
    if cache.exists():
        cache.rmdir()
    return {'block': number, 'strategy': strategy, 'failed': len(failed),
            'score': score, 'transactions': len(receipts), 'seconds': payload['elapsed_seconds']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('blocks', help='START:END_EXCLUSIVE (finalized, post-Byzantium blocks)')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--multiplier', type=float, default=1.0)
    parser.add_argument('--workers', type=int, default=2, help='Concurrent blocks/RPCs; default 2')
    parser.add_argument('--chain-id', type=int, default=1)
    args = parser.parse_args()
    try:
        start, end = map(int, args.blocks.split(':'))
        if not 0 <= start < end or not math.isfinite(args.multiplier) or args.multiplier <= 0 or args.workers < 1:
            raise ValueError()
    except ValueError:
        parser.error('Require START < END, positive finite multiplier and workers >= 1')
    call = make_rpc(os.environ.get('ETH_RPC_URL', 'http://127.0.0.1:8545'))
    chain = int(call('eth_chainId', []), 16)
    if chain != args.chain_id:
        raise ValueError('Unexpected chain ID')
    finalized = call('eth_getBlockByNumber', ['finalized', False])
    if end - 1 > int(finalized['number'], 16):
        raise ValueError('Requested range extends beyond finalized head')
    client = call('web3_clientVersion', [])
    args.output.mkdir(parents=True, exist_ok=True)
    with (args.output / '.writer.lock').open('w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
            # Submit bounded windows rather than queueing an entire historical range.
            for window in range(start, end, args.workers):
                futures = [pool.submit(extract_block, n, args.output, args.multiplier, call, chain, client)
                           for n in range(window, min(end, window + args.workers))]
                for future in futures:
                    print(json.dumps(future.result()), flush=True)


if __name__ == '__main__':
    main()
