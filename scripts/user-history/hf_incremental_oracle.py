"""Collect verified Oracle prices for an explicit incremental HF input window."""
import argparse
import fcntl
import json
from pathlib import Path

from shared.artifacts import checksum, digest, write_json
from hf_incremental_window import validate_window


def oracle_plan(window, route_at, output, unit):
    from oracle_common import intervals

    scope = validate_window(window)
    stop = scope['stop']
    headers = window['headers']
    states = []
    routes = []
    previous = None
    for item in headers:
        block = item['block']
        route = route_at(block)
        if route['block_hash'].lower() != item['hash']:
            raise ValueError('Oracle routing block hash differs')
        if not route['assets'] or route['unit'] != unit or len(set(route['assets'])) != len(route['assets']):
            raise ValueError('Oracle market or base currency requires review')
        routes.append(dict(route, block=block))
        values = (route['assets'], route['oracle'], route['unit'])
        if values != previous:
            states.append(dict(route, block=block))
            previous = values
    jobs = intervals(states, stop)
    for job in jobs:
        job['id'] = f"{job['start']}-{job['stop']-1}"
        job['node_dir'] = str(output/'jobs'/job['id'])
    return dict(format='aave-v3-hf-incremental-oracle-v1', window_sha256=window['window_sha256'],
                end_block=stop-1, end_hash=scope['end_hash'], jobs=jobs, routes=routes,
                requests=16, chunk_size=10000)


def collect(window_path, output):
    from hf_f64_contract import CONTRACT
    from oracle_common import POOL, POOL_ABI, PROVIDER_ABI, ORACLE_ABI
    from oracle_node_worker import run
    from web3 import Web3, HTTPProvider

    window = json.loads(window_path.read_text())
    headers = {h['block']: h['hash'] for h in window['headers']}
    w3 = Web3(HTTPProvider('http://127.0.0.1:8545', request_kwargs={'timeout':60}))
    scope = window['identity']
    if (w3.eth.chain_id != 1 or w3.eth.get_block('finalized').number < scope['stop']-1
            or w3.eth.get_block(scope['stop']-1).hash.to_0x_hex().lower() != scope['end_hash']):
        raise ValueError('Incremental Oracle target is no longer the pinned finalized block')
    pool = w3.eth.contract(address=POOL, abi=POOL_ABI)

    def route_at(block):
        provider = pool.functions.ADDRESSES_PROVIDER().call(block_identifier=block)
        registry = w3.eth.contract(address=provider, abi=PROVIDER_ABI)
        oracle = registry.functions.getPriceOracle().call(block_identifier=block)
        oracle_contract = w3.eth.contract(address=oracle, abi=ORACLE_ABI)
        return dict(oracle=oracle,provider=provider,
                    assets=list(pool.functions.getReservesList().call(block_identifier=block)),
                    unit=oracle_contract.functions.BASE_CURRENCY_UNIT().call(block_identifier=block),
                    block_hash=w3.eth.get_block(block).hash.to_0x_hex().lower())

    output.mkdir(parents=True, exist_ok=True)
    with (output/'collection.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        plan = oracle_plan(window,route_at,output,CONTRACT['base_currency_unit'])
        path = output/'plan.json'
        if path.exists():
            if json.loads(path.read_text()) != plan:
                raise ValueError('Existing Oracle plan differs')
        else:
            write_json(path,plan)
        receipts=[]
        for job in plan['jobs']:
            manifest = Path(job['node_dir'])/'manifest.json'
            if not manifest.exists():
                run(plan,job)
            evidence = json.loads(manifest.read_text())
            if (evidence['identity'] != digest(dict(job=job,end_hash=plan['end_hash'],chunk_size=plan['chunk_size']))
                    or evidence['start_hash'].lower() != headers[job['start']]
                    or evidence['end_hash'].lower() != headers[job['stop']-1]
                    or evidence['block_count'] != job['stop']-job['start']
                    or sum(f['rows'] for f in evidence['files']) != evidence['block_count']
                    or not evidence['checks'] or not all(c['exact'] for c in evidence['checks'])):
                raise ValueError('Incremental Oracle coverage or verification differs')
            for item in evidence['files']:
                for layer in ('raw','decoded'):
                    if checksum(Path(evidence[layer+'_root'])/item['name']) != item[layer+'_sha256']:
                        raise ValueError('Oracle payload checksum differs')
            receipts.append(dict(path=str(manifest),sha256=checksum(manifest),
                                 blocks=evidence['block_count'],price_values=evidence['price_values'],
                                 zero_price_values=evidence['zero_price_values']))
        result = dict(window_sha256=plan['window_sha256'],plan_sha256=checksum(path),receipts=receipts,
                      blocks=sum(r['blocks'] for r in receipts),price_values=sum(r['price_values'] for r in receipts))
        write_json(output/'oracle-inputs.json',result)
        return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--window',required=True,type=Path)
    parser.add_argument('--output',required=True,type=Path)
    args=parser.parse_args()
    print(json.dumps(collect(args.window,args.output.resolve())))


if __name__ == '__main__':
    main()
