#!/usr/bin/env python3
"""Bounded remote operations; all paths are provided by trusted deployment config."""
import base64
import json
import os
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from orchestration.artifacts import seal, verify, write_json

ROOT = Path(__file__).resolve().parents[1]


def execute(args, env=None):
    subprocess.run([str(a) for a in args], check=True, env=env, stdout=sys.stderr)


def rpc():
    from web3 import Web3
    w3 = Web3(Web3.HTTPProvider(os.environ.get('ETH_RPC_URL', 'http://127.0.0.1:8545'), request_kwargs={'timeout': 120}))
    if w3.eth.chain_id != 1:
        raise ValueError('expected Ethereum mainnet')
    return w3


def headers(w3, start, stop):
    from web3 import Web3
    if stop - 1 > w3.eth.get_block('finalized')['number']:
        raise ValueError('nonfinalized range')
    return {str(n): Web3.to_hex(w3.eth.get_block(n)['hash']) for n in range(start, stop)}


def validate_events(output, spec, w3, hashes):
    import pyarrow.parquet as pq
    from web3 import Web3
    expected = {(Web3.to_hex(r['blockHash']), r['logIndex']) for r in w3.eth.get_logs(
        {'address': Web3.to_checksum_address(spec['address']), 'fromBlock': spec['start'], 'toBlock': spec['stop'] - 1})}
    seen = set()
    for path in output.rglob('*.parquet'):
        for batch in pq.ParquetFile(path).iter_batches():
            for r in batch.to_pylist():
                key = (r['block_hash'], r['log_index'])
                if (r['chain_id'] != 1 or r['address'].lower() != spec['address'] or
                        hashes.get(str(r['block_number'])) != r['block_hash'] or key in seen):
                    raise ValueError('invalid or duplicate event coverage')
                seen.add(key)
    if seen != expected:
        raise ValueError('incomplete event coverage, including empty-result verification')


def extract(request):
    spec, output = request['spec'], Path(request['output'])
    from orchestration.coordinator import fingerprint
    if fingerprint(spec['kind'], spec.get('protocol')) != spec.get('code', spec['config']):
        raise ValueError('node code differs from coordinator; synchronize deployment')
    w3 = rpc()
    before = headers(w3, spec['start'], spec['stop'])
    if (output / 'coverage.json').exists():
        return verify(output, spec, before)
    output.mkdir(parents=True, exist_ok=True)
    blocks = f"{spec['start']}:{spec['stop']}"
    if spec['kind'] == 'archive':
        execute([sys.executable, ROOT / 'reverted/extract_reverted.py', blocks, '--output', output])
        from reverted.extract_reverted import load, validate_archive
        for n, h in before.items():
            payload = validate_archive(load(output / 'blocks' / f'{int(n)//1000:09d}' / f'{n}.json.gz'))
            if payload['header']['hash'] != h:
                raise ValueError('archive hash mismatch')
    else:
        args = ['bash', ROOT / f"extraction/extract_pool_{spec['kind']}.sh", spec['protocol'], spec['address'], blocks]
        if spec.get('profile'):
            write_json(output / 'deployment.json', spec['profile'])
            args += ['--deployment', output / 'deployment.json']
        env = dict(os.environ, OUTPUT_DIR=str(output), PYTHON_BIN=sys.executable)
        execute(args, env)
        if spec['kind'] == 'events':
            validate_events(output, spec, w3, before)
    if before != headers(w3, spec['start'], spec['stop']):
        raise ValueError('chain changed during extraction')
    return seal(output, spec, before)


def select(request):
    archive, output = Path(request['archive']), Path(request['output'])
    receipt = verify(archive)
    spec = request['spec']
    if receipt['spec']['config'] != spec['archive_config'] or (receipt['spec']['start'], receipt['spec']['stop']) != (spec['start'], spec['stop']):
        raise ValueError('wrong archive configuration or range')
    if (output / 'coverage.json').exists():
        return verify(output, spec, receipt['headers'])
    output.mkdir(parents=True, exist_ok=True)
    listing = output / 'contracts.txt'
    listing.write_text('\n'.join(spec['contracts']) + '\n')
    execute([sys.executable, ROOT / 'reverted/select_reverted.py', '--archive', archive,
             '--blocks', f"{spec['start']}:{spec['stop']}", '--contracts', listing, '--output', output])
    return seal(output, spec, receipt['headers'])


def archive_store(request):
    """Retain/reuse individual blocks across differently bounded archive batches."""
    from reverted.extract_reverted import load, save, validate_archive
    spec, output, store = request['spec'], Path(request['output']), Path(request['store'])
    expected = request['headers']
    publish = request['action'] == 'archive_index'
    if publish:
        verify(output, spec, expected)
    available = 0
    for n in range(spec['start'], spec['stop']):
        relative = Path('blocks') / f'{n//1000:09d}' / f'{n}.json.gz'
        source, dest = (output/relative, store/relative) if publish else (store/relative, output/relative)
        if not source.exists():
            continue
        payload = validate_archive(load(source))
        if payload['header']['hash'] != expected[str(n)]:
            raise ValueError('retained archive block hash mismatch')
        dest.parent.mkdir(parents=True, exist_ok=True)
        if not dest.exists():
            save(dest, payload)
        elif load(dest) != payload:
            raise ValueError('conflicting archive block')
        available += 1
    if available == spec['stop'] - spec['start']:
        seal(output, spec, expected)
    return {'available': available, 'complete': available == spec['stop'] - spec['start']}


def main():
    request = json.loads(base64.b64decode(sys.argv[1]))
    action = request['action']
    if action == 'preflight':
        import shutil
        root = Path(request['root'])
        root.parent.mkdir(parents=True, exist_ok=True)
        free = shutil.disk_usage(root.parent).free
        if free < 1024**3 or not shutil.which('rsync') or not shutil.which('flock'):
            raise ValueError('need rsync, flock and at least 1 GiB free')
        result = {'free_bytes': free, 'python': sys.version.split()[0]}
    elif action == 'head':
        w3 = rpc()
        result = {'number': w3.eth.get_block('finalized')['number']}
    elif action == 'headers':
        result = headers(rpc(), request['start'], request['stop'])
    elif action == 'extract':
        result = extract(request)
    elif action in ('archive_index', 'archive_reuse'):
        result = archive_store(request)
    elif action == 'select':
        result = select(request)
    elif action == 'verify':
        result = verify(request['output'])
    elif action == 'cleanup':
        import shutil
        path, root = Path(request['output']).resolve(), Path(request['root']).resolve()
        if path == root or root not in path.parents:
            raise ValueError('cleanup outside owned staging')
        if path.exists():
            verify(path, request['spec'])
            shutil.rmtree(path)
        result = {'cleaned': str(path)}
    else:
        raise ValueError('unknown worker action')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
