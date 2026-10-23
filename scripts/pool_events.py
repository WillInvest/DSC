#!/usr/bin/env python3
"""Extract finalized Ethereum Uniswap V3 pool logs; Python standard library only."""
import argparse
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import time
import urllib.error
import urllib.request

FACTORY = '0x1f98431c8ad98523631ae4a59f267346ea31f984'
EXAMPLE = '0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8'
EVENTS = {
    'Initialize': [('sqrtPriceX96', 'uint160', False), ('tick', 'int24', False)],
    'Mint': [('sender', 'address', False), ('owner', 'address', True), ('tickLower', 'int24', True), ('tickUpper', 'int24', True), ('amount', 'uint128', False), ('amount0', 'uint256', False), ('amount1', 'uint256', False)],
    'Collect': [('owner', 'address', True), ('recipient', 'address', False), ('tickLower', 'int24', True), ('tickUpper', 'int24', True), ('amount0', 'uint128', False), ('amount1', 'uint128', False)],
    'Burn': [('owner', 'address', True), ('tickLower', 'int24', True), ('tickUpper', 'int24', True), ('amount', 'uint128', False), ('amount0', 'uint256', False), ('amount1', 'uint256', False)],
    'Swap': [('sender', 'address', True), ('recipient', 'address', True), ('amount0', 'int256', False), ('amount1', 'int256', False), ('sqrtPriceX96', 'uint160', False), ('liquidity', 'uint128', False), ('tick', 'int24', False)],
    'Flash': [('sender', 'address', True), ('recipient', 'address', True), ('amount0', 'uint256', False), ('amount1', 'uint256', False), ('paid0', 'uint256', False), ('paid1', 'uint256', False)],
    'IncreaseObservationCardinalityNext': [('observationCardinalityNextOld', 'uint16', False), ('observationCardinalityNextNew', 'uint16', False)],
    'SetFeeProtocol': [(n, 'uint8', False) for n in ('feeProtocol0Old', 'feeProtocol1Old', 'feeProtocol0New', 'feeProtocol1New')],
    'CollectProtocol': [('sender', 'address', True), ('recipient', 'address', True), ('amount0', 'uint128', False), ('amount1', 'uint128', False)],
}


class RPC:
    def __init__(self, url):
        self.url = url

    def __call__(self, method, params):
        payload = json.dumps(dict(jsonrpc='2.0', id=1, method=method, params=params)).encode()
        for attempt in range(4):
            try:
                request = urllib.request.Request(self.url, payload, {'Content-Type': 'application/json'})
                with urllib.request.urlopen(request, timeout=60) as response:
                    result = json.load(response)
                if 'error' in result:
                    raise RuntimeError(f'{method}: RPC error {result["error"]}')
                return result['result']
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                if attempt == 3:
                    raise RuntimeError(f'{method}: transport failed after four attempts') from None
                time.sleep(2**attempt)


def decode(log, fields):
    data = bytes.fromhex(log['data'][2:])
    if len(log['topics']) != 1 + sum(f[2] for f in fields) or len(data) != 32 * sum(not f[2] for f in fields):
        raise ValueError('log does not match event ABI')
    topics = iter(log['topics'][1:])
    words = iter(data[i:i+32] for i in range(0, len(data), 32))
    result = {}
    for name, kind, indexed in fields:
        raw = bytes.fromhex(next(topics)[2:]) if indexed else next(words)
        if len(raw) != 32:
            raise ValueError('invalid ABI word')
        if kind == 'address':
            if any(raw[:12]):
                raise ValueError('invalid address padding')
            value = '0x' + raw[12:].hex()
        else:
            signed = kind.startswith('int')
            bits = int(kind[3:] if signed else kind[4:])
            number = int.from_bytes(raw, 'big', signed=signed)
            lower = -(1 << (bits-1)) if signed else 0
            upper = (1 << (bits-1)) if signed else (1 << bits)
            if not lower <= number < upper:
                raise ValueError('invalid ABI integer padding')
            value = str(number)  # exact even in downstream JavaScript / JSON readers
        result[name] = value
    return result


def atomic_write(path, content):
    temp = path.with_suffix(path.suffix + '.tmp')
    with temp.open('wb') as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temp, path)
    fd = os.open(str(path.parent), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def block(rpc, number):
    result = rpc('eth_getBlockByNumber', [hex(number), False])
    if not result or int(result['number'], 16) != number:
        raise ValueError(f'missing block {number}')
    return result


def extract(rpc, folder, config, topics, end, chunk_size):
    manifest_path = folder / 'manifest.json'
    manifest = {'config': config, 'chunks': []}
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        if manifest['config'] != config:
            raise ValueError('output configuration differs; use another output directory')
    start = config['from_block']
    for chunk in manifest['chunks']:
        if chunk['from_block'] != start:
            raise ValueError('checkpoint contains a coverage gap')
        path = folder / chunk['file']
        if hashlib.sha256(path.read_bytes()).hexdigest() != chunk['sha256']:
            raise ValueError(f'chunk checksum mismatch: {path}')
        start = chunk['to_block'] + 1
    if manifest['chunks']:
        last = manifest['chunks'][-1]
        if block(rpc, last['to_block'])['hash'] != last['end_block_hash']:
            raise ValueError('finalized block hash changed; preserve output and investigate before rebuilding')
    while start <= end:
        stop = min(start + chunk_size - 1, end)
        anchor = block(rpc, stop)
        logs = rpc('eth_getLogs', [{'address': config['address'], 'fromBlock': hex(start), 'toBlock': hex(stop)}])
        headers = {stop: anchor}
        rows = []
        seen = set()
        for log in sorted(logs, key=lambda x: (int(x['blockNumber'], 16), int(x['logIndex'], 16))):
            height = int(log['blockNumber'], 16)
            if not start <= height <= stop or log['address'].lower() != config['address'] or log.get('removed', False):
                raise ValueError('RPC returned an out-of-range, wrong-address, or removed log')
            if height not in headers:
                headers[height] = block(rpc, height)
            if headers[height]['hash'] != log['blockHash']:
                raise ValueError('log block hash mismatch')
            key = (log['blockHash'], log['logIndex'])
            if key in seen:
                raise ValueError('RPC returned duplicate logs')
            seen.add(key)
            name = topics.get(log['topics'][0]) if log['topics'] else None
            rows.append({'schema_version': 1, 'chain_id': config['chain_id'],
                         'address': config['address'], 'block_number': height,
                         'block_timestamp': int(headers[height]['timestamp'], 16),
                         'transaction_hash': log['transactionHash'],
                         'transaction_index': int(log['transactionIndex'], 16),
                         'log_index': int(log['logIndex'], 16), 'block_hash': log['blockHash'],
                         'event': name, 'args': decode(log, EVENTS[name]) if name else None, 'raw': log})
        if block(rpc, stop)['hash'] != anchor['hash']:
            raise ValueError('batch block hash changed during extraction')
        filename = f'{start:012d}-{stop:012d}.jsonl.gz'
        payload = gzip.compress(''.join(json.dumps(row, separators=(',', ':')) + '\n' for row in rows).encode(), mtime=0)
        atomic_write(folder / filename, payload)
        manifest['chunks'].append({'file': filename, 'from_block': start, 'to_block': stop,
                                   'end_block_hash': anchor['hash'], 'rows': len(rows),
                                   'sha256': hashlib.sha256(payload).hexdigest()})
        atomic_write(manifest_path, (json.dumps(manifest, indent=2) + '\n').encode())
        print(f'committed blocks {start}..{stop}: {len(rows)} events', flush=True)
        start = stop + 1
    print(f'complete through {start-1}; {sum(c["rows"] for c in manifest["chunks"])} events in output', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rpc', default=os.environ.get('ETH_RPC_URL', 'http://127.0.0.1:8545'))
    parser.add_argument('--address', default=EXAMPLE)
    parser.add_argument('--from-block', type=int, required=True)
    parser.add_argument('--to-block', default='finalized', help='inclusive block number or finalized')
    parser.add_argument('--chunk-size', type=int, default=1000)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    address = args.address.lower()
    if not re.fullmatch(r'0x[0-9a-f]{40}', address) or args.from_block < 0 or args.chunk_size < 1:
        parser.error('valid address, nonnegative start, and positive chunk size required')
    rpc = RPC(args.rpc)
    if int(rpc('eth_chainId', []), 16) != 1:
        raise ValueError('this extractor currently supports Ethereum mainnet only')
    finalized = rpc('eth_getBlockByNumber', ['finalized', False])
    if not finalized:
        raise ValueError('node has no finalized head')
    finalized_number = int(finalized['number'], 16)
    end = finalized_number if args.to_block == 'finalized' else int(args.to_block)
    if not args.from_block <= end <= finalized_number:
        parser.error('range must be nonempty and end at or before finalized head')
    metadata = {}
    for name, selector in [('factory', '0xc45a0155'), ('token0', '0x0dfe1681'), ('token1', '0xd21220a7'), ('fee', '0xddca3f43')]:
        raw = rpc('eth_call', [{'to': address, 'data': selector}, hex(end)])
        if not re.fullmatch(r'0x[0-9a-fA-F]{64}', raw):
            raise ValueError(f'pool {name} returned invalid ABI data')
        metadata[name] = int(raw, 16) if name == 'fee' else '0x' + raw[-40:].lower()
    if metadata['factory'] != FACTORY:
        raise ValueError('pool factory is not the Ethereum Uniswap V3 factory')
    calldata = '0x1698ee82' + metadata['token0'][2:].zfill(64) + metadata['token1'][2:].zfill(64) + format(metadata['fee'], '064x')
    registered = rpc('eth_call', [{'to': FACTORY, 'data': calldata}, hex(end)])
    if registered.lower() != '0x' + address[2:].zfill(64):
        raise ValueError('factory does not register this pool')
    topics = {}
    for name, fields in EVENTS.items():
        signature = name + '(' + ','.join(f[1] for f in fields) + ')'
        topics[rpc('web3_sha3', ['0x' + signature.encode().hex()])] = name
    config = dict(schema_version=1, chain_id=1, address=address, from_block=args.from_block, pool=metadata)
    args.output.mkdir(parents=True, exist_ok=True)
    with (args.output / '.lock').open('w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        extract(rpc, args.output, config, topics, end, args.chunk_size)


if __name__ == '__main__':
    try:
        main()
    except (ValueError, RuntimeError, OSError) as exc:
        print(f'error: {exc}', file=sys.stderr)
        sys.exit(1)
