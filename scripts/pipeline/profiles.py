"""Node-side incremental Aave ABI review, reusing the existing verified profile builder."""
import argparse
import json
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from web3 import Web3
from shared.artifacts import checksum, write_json
from protocols.deployment import verify_deployment
from registry.build_aave_profiles import build
from decoding.pool_decode import cryo_event_log
import pyarrow.parquet as pq


def bulk_upgrades(w3, profile, output, start, stop):
    import importlib.util
    folder=Path(__file__).resolve().parents[1]/'event-cryo-extraction'
    sys.path.insert(0,str(folder))
    spec=importlib.util.spec_from_file_location('cryo_node',folder/'node.py')
    worker=importlib.util.module_from_spec(spec);spec.loader.exec_module(worker)
    root=output/'upgrade-logs'/profile['protocol']
    plan=dict(end_block=stop-1,end_hash=Web3.to_hex(w3.eth.get_block(stop-1).hash),
              chunk_size=10000,concurrency=32,cryo='/home/node/.cargo/bin/cryo',poll_seconds=1)
    job=dict(address=profile['address'],start_block=start,node_dir=str(root),
             topic0=Web3.to_hex(Web3.keccak(text='Upgraded(address)')))
    worker.run(plan,job)
    manifest=json.loads((root/'manifest.json').read_text());logs=[]
    for name in manifest['files']:
        for batch in pq.ParquetFile(root/'encoded'/name).iter_batches():
            logs.extend(cryo_event_log(row) for row in batch.to_pylist())
    return logs


def extend(source, output, start, stop, contracts):
    w3=Web3(Web3.HTTPProvider('http://127.0.0.1:8545',request_kwargs={'timeout':120}))
    if w3.eth.chain_id!=1 or w3.eth.get_block('finalized').number<stop-1:
        raise ValueError('Target must be finalized Ethereum mainnet')
    output.mkdir(parents=True,exist_ok=True)
    receipt=output/'accepted.json'
    identity=dict(start=start,stop=stop,sources={p.name:checksum(p) for p in sorted(source.glob('aave-*.json'))})
    if receipt.exists():
        saved=json.loads(receipt.read_text())
        if saved['identity']!=identity:raise ValueError('Profile preparation identity changed')
        for n,h in saved['files'].items():
            if checksum(output/n)!=h:raise ValueError('Prepared profile changed')
        return
    profiles=[json.loads(p.read_text()) for p in source.glob('aave-*.json')]
    if {p['protocol'] for p in profiles}!={'aave-v2','aave-v3'}:raise ValueError('Missing Aave profiles')
    upgraded=False
    for protocol in ('aave-v2','aave-v3'):
        last=max((p for p in profiles if p['protocol']==protocol),key=lambda p:p['to_block'])
        print(f'{protocol}: Cryo upgrade audit [{start}, {stop})',flush=True)
        logs=bulk_upgrades(w3,last,output,start,stop)
        if last['to_block']>=stop:
            for profile in profiles:
                if profile['protocol']==protocol and profile['from_block']<stop and profile['to_block']>start:
                    lo=max(start,profile['from_block']);hi=min(stop,profile['to_block'])
                    verify_deployment(w3,profile,range(lo,hi),
                                      upgrade_logs=[l for l in logs if lo<=l['blockNumber']<hi])
            continue
        if last['to_block']!=start:raise ValueError('Profile range does not join previous cycle')
        upgraded=upgraded or bool(logs)
        if not logs:
            candidate={**last,'to_block':stop}
            verify_deployment(w3,candidate,range(start,stop),upgrade_logs=logs)
            last['to_block']=stop
    if upgraded:
        build(argparse.Namespace(rpc='http://127.0.0.1:8545',to_block=stop-1,contracts=contracts,output=output,
                                 history_directory=f'history/cycles/{start}-{stop-1}'))
    else:
        for p in profiles:write_json(output/f"{p['protocol']}-{p['from_block']}.json",p)
    write_json(receipt,dict(identity=identity,files={p.name:checksum(p) for p in output.glob('aave-*.json')},
                            upgrade_discovered=upgraded))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--source',required=True,type=Path);p.add_argument('--output',required=True,type=Path)
    p.add_argument('--start',required=True,type=int);p.add_argument('--stop',required=True,type=int)
    p.add_argument('--contracts',required=True,type=Path);a=p.parse_args()
    extend(a.source,a.output,a.start,a.stop,a.contracts)
