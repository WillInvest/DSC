"""Direct local replay, without RETH comparisons or per-block header retrieval."""
import base64
import json
from pathlib import Path
import shlex
import subprocess
import sys
import time
import os
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import pyarrow.parquet as pq
from web3 import Web3
from automation.common import ROOT, AUTO, PROFILES, paths, read
from shared.artifacts import checksum,digest,write_json
from reconstruction.core import initial,HANDLERS
from reconstruction.direct import read_chunk,replay_table
from decoding.pool_decode import EventDecoder

MODE='event-derived-unchecked-v1'
NODE='node@10.246.103.160'
SSH=['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10','-o','ServerAliveInterval=15','-o','ServerAliveCountMax=3',NODE]


def aave_inputs(job,plan):
    profiles=sorted([read(p) for p in PROFILES.glob(job['protocol']+'-*.json')],key=lambda p:p['from_block'])
    if not profiles or profiles[0]['from_block']!=job['start_block'] or profiles[-1]['to_block']<plan['end_block']+1 or any(a['to_block']!=b['from_block'] for a,b in zip(profiles,profiles[1:])):
        raise ValueError('Missing Aave historical boundary inputs')
    request=dict(protocol=job['protocol'],address=job['address'],start=job['start_block'],stop=plan['end_block']+1,profiles=profiles)
    key=digest(request);root=AUTO/'aave-inputs'/key
    cache=root/'decoded-inputs.json'
    if cache.exists(): return read(cache)
    request['output']='/home/node/dsc-extraction/direct-core-inputs/'+key
    if not (root/'inputs.json').exists():
        encoded=base64.b64encode(json.dumps(request).encode()).decode()
        subprocess.run(SSH+[shlex.join(['/home/node/dsc-extraction/.venv-abi/bin/python',
            '/home/node/dsc-extraction/scripts/automation/aave_inputs.py',encoded])],check=True,timeout=3300)
        root.mkdir(parents=True,exist_ok=True)
        subprocess.run(['rsync','-a','--partial','-e','ssh -o BatchMode=yes -o ConnectTimeout=10',
                        NODE+':'+request['output']+'/',str(root)+'/'],check=True,timeout=300)
    payload=read(root/'inputs.json');events=[]
    for source in payload['sources']:
        if source['provider']: continue
        abi=read(Path(__file__).parents[1]/'reconstruction/abis'/source['abi'])
        contract=Web3().eth.contract(abi=abi);decoder=EventDecoder(abi)
        names={x['name'] for x in abi if x['type']=='event'}
        topics={getattr(contract.events,n)().topic for n in ('ReserveInitialized','ReserveDropped') if n in names}
        for file in (root/source['address']).glob('*.parquet'):
            for batch in pq.ParquetFile(file).iter_batches():
                for row in batch.to_pylist():
                    if row['topic0'] not in topics: continue
                    n=row['block_number']
                    route=next(r for r in payload['routes'] if r['start']<=n<r['stop'])
                    current=route['initial']
                    for change in route['transitions']:
                        if (change['block'],change['log_index'])<(n,row['log_index']): current=change['address']
                    if current!=source['address']: continue
                    name,fields,values,_=decoder.decode(row)
                    events.append({**row,'name':name,**{'arg__'+f['name']:v for f,v in zip(fields,values)}})
    result=dict(seeds=payload['seeds'],events=events,source=str(root/'inputs.json'))
    write_json(cache,result)
    return result


def run(job,plan,budget=30):
    encoded,decoded,base=paths(job);base.mkdir(parents=True,exist_ok=True)
    output=base/MODE;output.mkdir(exist_ok=True)
    status_path=base/'reconstruction-progress.json';progress_path=output/'progress.json'
    identity=dict(mode=MODE,protocol=job['protocol'],address=job['address'],
                  start=job['start_block'],stop=plan['end_block']+1,
                  extraction_receipt=checksum(ROOT/'receipts'/f"{job['address']}.json"),
                  handlers=checksum(Path(__file__).parents[1]/'reconstruction/core.py'))
    old=read(progress_path,{})
    if old and old['identity']!=identity: raise ValueError('Direct replay identity changed')
    previous=read(status_path,{})
    if previous and previous.get('mode')!=MODE and not (base/'checked-progress.json').exists():
        write_json(base/'checked-progress.json',previous)
    start=old.get('next_block',job['start_block']);state=old.get('last_state',initial(job['protocol']))
    begun=time.monotonic();prior=old.get('elapsed_seconds',0)
    progress=dict(identity=identity,mode=MODE,validation='none',protocol=job['protocol'],name=job['name'],
                  state='preparing_inputs',next_block=start,completed_blocks=start-job['start_block'],
                  total_blocks=identity['stop']-job['start_block'],elapsed_seconds=prior,updated_at=time.time())
    def publish():
        progress.update(updated_at=time.time(),elapsed_seconds=prior+time.monotonic()-begun)
        progress['blocks_per_second']=progress['completed_blocks']/max(progress['elapsed_seconds'],0.001)
        write_json(status_path,progress)
    publish()
    extra=aave_inputs(job,plan) if job['protocol'].startswith('aave-') else dict(seeds={},events=[])
    receipt=read(ROOT/'receipts'/f"{job['address']}.json")
    progress['state']='replaying'
    for lower in range(job['start_block'],identity['stop'],plan['chunk_size']):
        stop=min(lower+plan['chunk_size'],identity['stop'])
        if stop<=start: continue
        if lower!=start: raise ValueError('Resume cursor not on a chunk boundary')
        name=f'ethereum__logs__{lower:08d}_to_{stop-1:08d}.parquet'
        if name in receipt['files']:
            events=read_chunk(decoded,name,HANDLERS[job['protocol']])
        elif name in receipt['empty_chunks']: events=[]
        else: raise ValueError('Missing source chunk')
        events.extend(e for e in extra['events'] if lower<=e['block_number']<stop)
        table,state=replay_table(job['protocol'],job['address'],state,events,lower,stop,extra['seeds'])
        target=output/f'{lower}-{stop-1}.parquet';temp=target.with_suffix('.parquet.tmp')
        pq.write_table(table,temp,compression='zstd')
        with temp.open('rb') as stream: os.fsync(stream.fileno())
        temp.replace(target)
        start=stop
        progress.update(next_block=stop,completed_blocks=stop-job['start_block'],
                        state='complete' if stop==identity['stop'] else 'ready')
        publish()
        # The cursor is committed only after the Parquet file was atomically published.
        write_json(progress_path,{**progress,'last_state':state,'last_file':target.name})
        if time.monotonic()-begun>=budget: break
    if start==identity['stop']:
        progress['state']='complete';publish()
        write_json(output/'manifest.json',dict(identity=identity,validation='none',
                   description='Event-derived core state; not compared against node state',
                   blocks=progress['completed_blocks'],supplementary_source=extra.get('source')))


if __name__=='__main__':
    import fcntl
    plan=read(ROOT/'plan.json');job=next(j for j in plan['jobs'] if j['address']==sys.argv[1])
    base=paths(job)[2];base.mkdir(parents=True,exist_ok=True)
    with (base/'.direct.lock').open('w') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        run(job,plan)
