"""Reuse retained configurator inputs to discover every historical reserve token."""
import json,sys,time,subprocess
from pathlib import Path
sys.path.insert(0,'/home/dsc-data/scripts')
import pyarrow.dataset as ds
from web3 import Web3
from decoding.pool_decode import EventDecoder
from pipeline.runtime import rpc,NODE,NODE_PYTHON,ssh
from pipeline.compaction.reader import snapshot
from shared.artifacts import write_json,checksum

ROOT=Path('/home/dsc-data/runs/aave-user-history-v3')
POOL='0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2'
def prepare():
    ROOT.mkdir(parents=True,exist_ok=True)
    if (ROOT/'current.json').exists():return json.loads((ROOT/'current.json').read_text())
    finalized=rpc('eth_getBlockByNumber',['finalized',False]);end=int(finalized['number'],16);run_id=str(end);root=ROOT/run_id
    root.mkdir(exist_ok=True);decoder_abi=json.loads(Path('/home/dsc-data/scripts/reconstruction/abis/v3-IPoolConfigurator.json').read_text());c=Web3().eth.contract(abi=decoder_abi);decoder=EventDecoder(decoder_abi)
    topic=c.events.ReserveInitialized().topic;events=[];reused=[]
    for directory in Path('/home/dsc-data/runs/event-pipeline/aave-inputs').iterdir():
        f=directory/'inputs.json'
        if not f.exists():continue
        p=json.loads(f.read_text())
        if p['request']['protocol']!='aave-v3' or p['request']['start']!=16291127:continue
        reused.append(dict(manifest=str(f),sha256=checksum(f),start=p['request']['start'],stop=p['request']['stop']))
        for source in p['sources']:
            if source['provider'] or not int(source['address'],16):continue
            # Retained one-time auxiliary logs are outside canonical primary files.
            paths=[str(x) for x in (directory/source['address']).glob('*.parquet')]
            rows=ds.dataset(paths,format='parquet').to_table(filter=ds.field('topic0')==topic).to_pylist()
            for row in rows:
                name,fields,values,_=decoder.decode(row);events.append(dict(block=row['block_number'],log_index=row['log_index'],**dict(zip([x['name'] for x in fields],values))))
            if p['request']['stop']<=end:
                tail=[]
                for first in range(p['request']['stop'],end+1,10000):
                    tail.extend(rpc('eth_getLogs',[dict(address=source['address'],fromBlock=hex(first),toBlock=hex(min(first+9999,end)),topics=[topic])]))
                for row in tail:
                    e=c.events.ReserveInitialized().process_log(dict(row,blockNumber=int(row['blockNumber'],16),logIndex=int(row['logIndex'],16),transactionIndex=int(row['transactionIndex'],16)))
                    events.append(dict(block=e['blockNumber'],log_index=e['logIndex'],**dict(e['args'])))
        break
    if not reused:raise ValueError('No retained full-history Aave V3 configurator input')
    events=sorted({(e['block'],e['log_index']):e for e in events}.values(),key=lambda e:(e['block'],e['log_index']))
    tokens={}
    for e in events:
        for kind in ['aToken','stableDebtToken','variableDebtToken']:
            address=e[kind].lower()
            if not int(address,16):continue
            tokens.setdefault(address,dict(address=address,kind=kind,asset=e['asset'].lower(),initialized_block=e['block']))
    if not tokens:raise ValueError('No reserve tokens discovered')
    pool_abi=[dict(type='function',name='getReservesList',stateMutability='view',inputs=[],outputs=[dict(name='',type='address[]')])]
    call=Web3().eth.contract(abi=pool_abi).functions.getReservesList()._encode_transaction_data()
    encoded=rpc('eth_call',[dict(to=POOL,data=call),hex(end)])
    current_assets=set(Web3().codec.decode(['address[]'],bytes.fromhex(encoded[2:]))[0])
    if not {a.lower() for a in current_assets}<={e['asset'].lower() for e in events}:raise ValueError('Historical initialization registry misses current reserves')
    # Existing verified deployment helper executes on node; no hosted chain data.
    code="""import json,sys
from concurrent.futures import ThreadPoolExecutor
from web3 import Web3,HTTPProvider
sys.path.insert(0,'/home/node/dsc-extraction/scripts')
from registry.build_registry import deployment_block
tokens=json.loads(sys.argv[1]);end=int(sys.argv[2])
def one(t):
 w=Web3(HTTPProvider('http://127.0.0.1:8545',request_kwargs={'timeout':60}));a=Web3.to_checksum_address(t['address']);n=deployment_block(w,a,end)
 assert n<=t['initialized_block']
 return dict(t,start_block=n,deployment_hash=w.eth.get_block(n).hash.to_0x_hex())
with ThreadPoolExecutor(max_workers=8) as ex:print(json.dumps(list(ex.map(one,tokens))))
"""
    result=subprocess.run(ssh(NODE,[NODE_PYTHON,'-c',code,json.dumps(list(tokens.values())),str(end)]),capture_output=True,text=True,check=True,timeout=1200)
    jobs=json.loads(result.stdout);base=Path('/home/dsc-data/data/aave-user-history-v3')/run_id;node_root='/home/node/dsc-extraction/aave-user-history-v3/'+run_id
    for j in jobs:j.update(node_dir=node_root+'/'+j['address'],destination=str(base/'staging'/j['address']))
    # Inventory existing Pool files only through the mandatory shared-lock API.
    with snapshot('/home/dsc-data/data/compacted') as files:pool_files=[dict(x) for x in files if x['stream'].startswith('encoded/aave-v3/'+POOL+'/')]
    write_json(root/'pool-reuse.json',dict(pool=POOL,canonical_snapshot=pool_files,baseline_plan='/home/dsc-data/runs/sequential-events/plan.json',note='Reuse accepted Pool events; no Pool re-extraction. Snapshot is an inventory, not a new acceptance claim.'))
    write_json(root/'reserve-initializations.json',events)
    plan=dict(run_id=run_id,end_block=end,end_hash=finalized['hash'],chunk_size=10000,cryo='/home/node/.cargo/bin/cryo',node_plan=node_root+'/plan.json',jobs=sorted(jobs,key=lambda j:(j['start_block'],j['kind'],j['address'])),reused_auxiliary=reused,pool_reuse=str(root/'pool-reuse.json'),output=str(base),scope='Ethereum Aave V3 core all historical reserve tokens; supplemental raw input stage',next_stages=['Historical oracle/rate/configuration source registry and extraction','ABI/version-aware user position replay','Historical oracle synthesis','HF evaluation and independent stratified node validation'],created_at=time.time())
    write_json(root/'plan.json',plan)
    subprocess.run(ssh(NODE,['mkdir','-p',node_root]),check=True)
    subprocess.run(['rsync','-a',str(root/'plan.json'),NODE+':'+plan['node_plan']],check=True)
    current=dict(run_id=run_id,root=str(root),plan=str(root/'plan.json'),progress=str(root/'progress.json'),stage='supplemental_token_events',user_history_complete=False)
    write_json(ROOT/'current.json',current);write_json(root/'progress.json',dict(state='ready',stage=current['stage'],completed_addresses=0,total_addresses=len(jobs),updated_at=time.time()))
    return current
if __name__=='__main__':print(json.dumps(prepare()))
