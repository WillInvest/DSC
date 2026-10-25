"""One-time Aave boundary seeds and Cryo supplementary logs; no replay checks."""
import json
from pathlib import Path
import subprocess
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import pyarrow.parquet as pq
from web3 import Web3
from reconstruction.bootstrap import snapshot
from reconstruction.routing import change
from protocols.protocol_config import load_protocol
from decoding.pool_decode import EventDecoder
from shared.artifacts import write_json


def collect(request):
    output=Path(request['output']);output.mkdir(parents=True,exist_ok=True)
    if (output/'inputs.json').exists():
        if json.loads((output/'inputs.json').read_text())['request']!=request:
            raise ValueError('Different Aave input scope')
        return
    w3=Web3(Web3.HTTPProvider('http://127.0.0.1:8545',request_kwargs={'timeout':120}))
    protocol=request['protocol'];address=request['address'];version=protocol.split('-')[1]
    stop=request['stop'];seeds={};routes=[];sources=set();extracted=set()
    abi_root=Path(__file__).parents[1]/'reconstruction/abis'

    def extract(source):
        if source in extracted: return
        folder=output/source
        subprocess.run(['/home/node/.cargo/bin/cryo','logs','--rpc','http://127.0.0.1:8545',
            '--contract',source,'--blocks',f"{request['start']}:{stop}",'--chunk-size','10000',
            '--inner-request-size','1000','--max-concurrent-requests','8','--max-concurrent-chunks','8',
            '--include-columns','block_hash','--hex','--output-dir',str(folder)],check=True,timeout=1800)
        completed={p.name for p in folder.glob('*.parquet')}
        for report in (folder/'.cryo/reports').glob('*.json'):
            if report.name.startswith('incomplete_'): continue
            result=json.loads(report.read_text()).get('results') or {}
            completed.update(Path(p).name for p in result.get('completed_paths',[]))
        expected={f"ethereum__logs__{n:08d}_to_{min(n+9999,stop-1):08d}.parquet"
                  for n in range(request['start'],stop,10000)}
        if not expected<=completed: raise ValueError('Unfinished supplementary Cryo chunks')
        extracted.add(source)

    providers={}
    for profile in request['profiles']:
        start=profile['from_block'];end=min(profile['to_block'],stop)
        if start>=stop: continue
        write_json(output/'progress.json',dict(state='boundary_inputs',block=start,protocol=protocol))
        seed_path=output/f'seed-{start}.json'
        if not seed_path.exists():
            write_json(seed_path,snapshot(w3,protocol,address,start,profile))
        seeds[str(start)]=json.loads(seed_path.read_text())
        cfg=load_protocol(protocol,profile)
        pool=w3.eth.contract(address=Web3.to_checksum_address(address),abi=json.loads(cfg['abi'].read_text()))
        getter='getAddressesProvider' if version=='v2' else 'ADDRESSES_PROVIDER'
        provider_address=getattr(pool.functions,getter)().call(block_identifier=start)
        provider_file=version+'-'+('ILendingPoolAddressesProvider' if version=='v2' else 'IPoolAddressesProvider')+'.json'
        abi=json.loads((abi_root/provider_file).read_text())
        provider=w3.eth.contract(address=provider_address,abi=abi)
        getter='getLendingPoolConfigurator' if version=='v2' else 'getPoolConfigurator'
        current=getattr(provider.functions,getter)().call(block_identifier=start).lower()
        key=provider_address.lower()
        if key not in providers:
            extract(key)
            decoder=EventDecoder(abi)
            topics={getattr(provider.events,n)().topic for n in ('AddressSet','ProxyCreated')}
            updates=[]
            for file in (output/key).glob('*.parquet'):
                for batch in pq.ParquetFile(file).iter_batches():
                    for row in batch.to_pylist():
                        if row['topic0'] not in topics: continue
                        name,fields,values,_=decoder.decode(row)
                        new=change(name,dict(zip([f['name'] for f in fields],values)),version)
                        if new is not None:
                            updates.append(dict(block=row['block_number'],log_index=row['log_index'],address=new.lower()))
            providers[key]=sorted(updates,key=lambda e:(e['block'],e['log_index']))
        transitions=[e for e in providers[key] if start<e['block']<end]
        routes.append(dict(start=start,stop=end,initial=current,transitions=transitions))
        config_file=version+'-'+('LendingPoolConfigurator' if version=='v2' else 'IPoolConfigurator')+'.json'
        sources.add((key,provider_file,True));sources.add((current,config_file,False))
        sources.update((e['address'],config_file,False) for e in transitions)
    records=[]
    for source,abi,provider in sorted(sources):
        extract(source)
        records.append(dict(address=source,abi=abi,provider=provider))
    write_json(output/'inputs.json',dict(request=request,seeds=seeds,routes=routes,sources=records,
        validation='none; initial conditions and supplementary events only'))


if __name__=='__main__':
    import base64,fcntl,signal
    def stop(signum,frame): raise TimeoutError('Aave input collector interrupted or timed out')
    for sig in (signal.SIGTERM,signal.SIGHUP,signal.SIGALRM): signal.signal(sig,stop)
    signal.alarm(3200)
    request=json.loads(base64.b64decode(sys.argv[1]));root=Path(request['output']);root.mkdir(parents=True,exist_ok=True)
    with (root/'.lock').open('w') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);collect(request)
