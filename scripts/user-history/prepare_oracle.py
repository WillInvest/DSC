"""Plan an immutable oracle history, reusing retained routing/configurator logs."""
import json,subprocess,sys,time
from pathlib import Path
sys.path.insert(0,'/home/dsc-data/scripts')
import pyarrow.dataset as ds
from web3 import Web3
from decoding.pool_decode import EventDecoder
from shared.artifacts import write_json,checksum
from pipeline.runtime import ssh,NODE,NODE_PYTHON
from oracle_common import START,POOL,ORACLE_ABI,POOL_ABI,PROVIDER_ABI,intervals
sys.path.insert(0,'/home/dsc-data/scripts/event-cryo-extraction')
from common import chunks
from node import inventory,report_successes

ROOT=Path('/home/dsc-data/runs/aave-oracle-v3')

def sync_plan(path,plan):
    node_root='/home/node/dsc-extraction/aave-oracle-v3/'+plan['run_id']
    subprocess.run(ssh(NODE,['mkdir','-p',node_root]),check=True)
    # Resume must also recover a failed initial transfer of the immutable plan.
    subprocess.run(['rsync','-a','--checksum',str(path),NODE+':'+node_root+'/plan.json'],check=True)
    result=subprocess.run(ssh(NODE,['sha256sum',node_root+'/plan.json']),capture_output=True,text=True,check=True)
    if result.stdout.split()[0]!=checksum(path):raise ValueError('Transferred oracle plan hash differs')

def verify_plan_inputs(root,plan):
    for name,key in [('boundary-snapshots.json','boundary_snapshots_sha256'),('routing-events.json','routing_evidence_sha256')]:
        if checksum(root/name)!=plan[key]:raise ValueError('Oracle boundary/routing evidence identity changed')
    for tail in json.loads((root/'routing-events.json').read_text())['tail']:
        folder=root/'routing-tail'/tail['address']
        expected=chunks(dict(start_block=tail['start']),dict(end_block=tail['stop']-1,chunk_size=10000))
        if set(inventory(folder,expected))|report_successes(folder/'.cryo/reports',expected)!=set(expected):raise ValueError('Oracle routing tail coverage incomplete')
        if any(checksum(f['path'])!=f['sha256'] for f in tail['files']):raise ValueError('Oracle routing input checksum changed')

SNAPSHOTS='''import sys,json
from concurrent.futures import ThreadPoolExecutor
from web3 import Web3,HTTPProvider
p=json.load(sys.stdin)
def one(block):
 w=Web3(HTTPProvider('http://127.0.0.1:8545',request_kwargs={'timeout':60}))
 pool=w.eth.contract(address=p['pool'],abi=p['pool_abi'])
 assert pool.functions.ADDRESSES_PROVIDER().call(block_identifier=block).lower()==p['provider'].lower()
 assets=pool.functions.getReservesList().call(block_identifier=block)
 provider=w.eth.contract(address=Web3.to_checksum_address(p['provider']),abi=p['provider_abi'])
 oracle=provider.functions.getPriceOracle().call(block_identifier=block)
 unit=w.eth.contract(address=oracle,abi=p['oracle_abi']).functions.BASE_CURRENCY_UNIT().call(block_identifier=block) if assets else None
 if assets:assert int(oracle,16)>0 and unit>0
 return dict(block=block,block_hash=w.eth.get_block(block).hash.to_0x_hex(),assets=assets,oracle=oracle,unit=unit)
with ThreadPoolExecutor(max_workers=8) as ex:print(json.dumps(list(ex.map(one,p['blocks']))))
'''

def prepare():
    current=json.loads(Path('/home/dsc-data/runs/aave-user-history-v3/current.json').read_text())
    user=Path(current['root']);source=json.loads(Path(current['plan']).read_text())
    root=ROOT/str(source['end_block']);path=root/'plan.json'
    if path.exists():
        plan=json.loads(path.read_text());verify_plan_inputs(root,plan);sync_plan(path,plan)
        write_json(ROOT/'current.json',dict(root=str(root),plan=str(path),run_id=plan['run_id']))
        return path
    root.mkdir(parents=True,exist_ok=True)
    inits=json.loads((user/'reserve-initializations.json').read_text())
    auxiliary=Path(source['reused_auxiliary'][0]['manifest']);aux=json.loads(auxiliary.read_text())
    provider=next(s['address'] for s in aux['sources'] if s['provider'])
    events=[];tail_evidence=[]
    for s in aux['sources']:
        if not int(s['address'],16):continue
        abi=json.loads((Path('/home/dsc-data/scripts/reconstruction/abis')/s['abi']).read_text())
        name='PriceOracleUpdated' if s['provider'] else 'ReserveDropped'
        event_abi=next(a for a in abi if a.get('type')=='event' and a['name']==name)
        c=Web3().eth.contract(abi=[event_abi]);topic=getattr(c.events,name)().topic
        decoder=EventDecoder([event_abi])
        paths=[str(p) for p in (auxiliary.parent/s['address']).glob('*.parquet')]
        rows=ds.dataset(paths,format='parquet').to_table(filter=ds.field('topic0')==topic).to_pylist() if paths else []
        start=aux['request']['stop'];stop=source['end_block']+1
        node_tail='/home/node/dsc-extraction/aave-oracle-v3/'+str(source['end_block'])+'/routing-tail/'+s['address']
        local_tail=root/'routing-tail'/s['address'];local_tail.mkdir(parents=True,exist_ok=True)
        if start<stop:
            preflight="import sys;from pathlib import Path;sys.path.insert(0,'/home/node/dsc-extraction/scripts/event-cryo-extraction');from node import inventory;p=Path(sys.argv[1]);inventory(p,{f.name:0 for f in p.glob('*.parquet')})"
            subprocess.run(ssh(NODE,[NODE_PYTHON,'-c',preflight,node_tail]),check=True)
            command=['/home/node/.cargo/bin/cryo','logs','--rpc','http://127.0.0.1:8545','--contract',s['address'],'--blocks',f'{start}:{stop}','--inner-request-size','1000','--chunk-size','10000','--max-concurrent-requests','32','--max-concurrent-chunks','2','--include-columns','block_hash','--hex','--output-dir',node_tail,'--no-verbose']
            with (root/('routing-'+s['address']+'.log')).open('a') as log:
                subprocess.run(ssh(NODE,command),stdout=log,stderr=subprocess.STDOUT,check=True,timeout=1200)
            subprocess.run(['rsync','-a','-e','ssh -o BatchMode=yes -o ConnectTimeout=10',NODE+':'+node_tail+'/',str(local_tail)+'/'],check=True,stdout=subprocess.DEVNULL)
            expected=chunks(dict(start_block=start),dict(end_block=stop-1,chunk_size=10000))
            found=inventory(local_tail,expected)
            evidence=report_successes(local_tail/'.cryo/reports',expected)
            if set(found)|evidence!=set(expected):raise ValueError('Incomplete routing tail/empty-result evidence')
            tail_paths=[str(p) for p in local_tail.glob('*.parquet')]
            if tail_paths:rows.extend(ds.dataset(tail_paths,format='parquet').to_table(filter=ds.field('topic0')==topic).to_pylist())
            tail_evidence.append(dict(address=s['address'],start=start,stop=stop,files=[dict(path=p,sha256=checksum(p)) for p in tail_paths],empty_chunks=sorted(set(expected)-set(found)),reports=[dict(path=str(p),record=json.loads(p.read_text())) for p in (local_tail/'.cryo/reports').glob('*.json')]))
        for row in rows:
            event,fields,values,_=decoder.decode(row)
            events.append(dict(block=row['block_number'],log_index=row['log_index'],name=event,args=dict(zip([f['name'] for f in fields],values))))
    write_json(root/'routing-events.json',dict(reused_manifest=str(auxiliary),reused_manifest_sha256=checksum(auxiliary),events=events,tail=tail_evidence))
    points=sorted({START,*[i['block'] for i in inits],*[e['block'] for e in events],source['end_block']})
    payload=dict(blocks=points,pool=POOL,provider=provider,pool_abi=POOL_ABI,provider_abi=PROVIDER_ABI,oracle_abi=ORACLE_ABI)
    result=subprocess.run(ssh(NODE,[NODE_PYTHON,'-c',SNAPSHOTS]),input=json.dumps(payload),text=True,capture_output=True,check=True,timeout=600)
    snapshots=json.loads(result.stdout)
    assert snapshots[-1]['block_hash']==source['end_hash']
    assert {a.lower() for a in snapshots[-1]['assets']}=={i['asset'].lower() for i in inits}
    write_json(root/'boundary-snapshots.json',snapshots)
    # Reuse the already verified recent 10,000-block raw benchmark.
    benchmark_start=source['end_block']-9999
    active=[]
    for s in snapshots:
        if not active or any(s[k]!=active[-1][k] for k in ['assets','oracle','unit']):active.append(s)
    jobs=intervals(active,source['end_block']+1,[benchmark_start])
    for j in jobs:
        j['id']=str(j['start'])+'-'+str(j['stop']-1)
        j['node_dir']='/home/node/dsc-extraction/aave-oracle-v3/'+str(source['end_block'])+'/'+j['id']
        j['destination']='/home/dsc-data/data/aave-oracle-v3/'+str(source['end_block'])+'/'+j['id']
        if j['start']==benchmark_start and j['stop']==source['end_block']+1:
            j['reuse_raw']='/home/node/dsc-extraction/oracle-benchmark/20261114-10000-blocks/raw'
    plan=dict(created_at=time.time(),run_id=str(source['end_block']),start_block=START,end_block=source['end_block'],end_hash=source['end_hash'],jobs=jobs,requests=32,chunk_size=10000,scope='All historical Ethereum Aave V3 reserves; direct node end-of-block prices',boundary_snapshots_sha256=checksum(root/'boundary-snapshots.json'),routing_evidence_sha256=checksum(root/'routing-events.json'))
    write_json(path,plan);write_json(ROOT/'current.json',dict(root=str(root),plan=str(path),run_id=plan['run_id']))
    verify_plan_inputs(root,plan);sync_plan(path,plan)
    return path

if __name__=='__main__':print(prepare())
