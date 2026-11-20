"""Incremental four-asset token history decoding and position-owner discovery."""
import argparse,bisect,fcntl,json,os,sqlite3,subprocess,sys,time
from pathlib import Path
sys.path.insert(0,'/home/dsc-data/scripts')
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from web3 import Web3
from decoding.pool_decode import EventDecoder,PoolDecoder
from pipeline.runtime import ssh,NODE,NODE_PYTHON
from shared.artifacts import checksum,digest,write_json
from run import selected_jobs

ROOT=Path('/home/dsc-data/runs/aave-user-history-v3')
UPGRADED={'type':'event','name':'Upgraded','anonymous':False,'inputs':[dict(name='implementation',type='address',indexed=True)]}
def profiles(plan,job,receipt,root):
    destination=root/'token-profiles';path=destination/(job['address']+'.profiles.json')
    if path.exists():return json.loads(path.read_text())
    paths=[str(Path(job['destination'])/n) for n in receipt['files']]
    topic=Web3().eth.contract(abi=[UPGRADED]).events.Upgraded().topic
    rows=ds.dataset(paths,format='parquet').to_table(filter=ds.field('topic0')==topic).to_pylist() if paths else []
    decoder=EventDecoder([UPGRADED]);upgrades=[]
    for row in rows:
        name,fields,values,_=decoder.decode(row)
        upgrades.append(dict(block=row['block_number'],log_index=row['log_index'],implementation=values[0].lower(),block_hash=row['block_hash'],transaction_hash=row['transaction_hash']))
    request=dict(address=job['address'],start_block=job['start_block'],end_block=plan['end_block'],end_hash=plan['end_hash'],receipt_sha256=checksum(root/'receipts'/(job['address']+'.json')),upgrades=upgrades)
    node_root='/home/node/dsc-extraction/aave-user-history-v3/'+plan['run_id']+'/token-profiles'
    result=subprocess.run(ssh(NODE,[NODE_PYTHON,'/home/node/dsc-extraction/scripts/user-history/token_profiles_node.py',json.dumps(request),node_root]),capture_output=True,text=True,timeout=1800)
    if result.returncode:raise RuntimeError('Node token profile verification failed: '+result.stderr[-3500:])
    record=json.loads(result.stdout);destination.mkdir(exist_ok=True)
    subprocess.run(['rsync','-a','-e','ssh -o BatchMode=yes -o ConnectTimeout=10',NODE+':'+node_root+'/',str(destination)+'/'],check=True,capture_output=True,text=True,timeout=600)
    saved=json.loads(path.read_text())
    if saved!=record or saved['request_digest']!=digest(request):raise ValueError('Transferred token profile differs')
    return saved

def owners(decoded,fields):
    if decoded.name not in ['Mint','Burn','Transfer','BalanceTransfer']:return []
    return [decoded.values['arg__'+f['name']] for f in fields if f['type']=='address' and f['name'] in ['from','to','onBehalfOf','user','account'] and int(decoded.values['arg__'+f['name']],16)]

def decode_job(plan,job,root):
    receipt_path=root/'receipts'/(job['address']+'.json');receipt=json.loads(receipt_path.read_text());profile=profiles(plan,job,receipt,root)
    out=Path(plan['output'])/'decoded-staging'/job['address'];out.mkdir(parents=True,exist_ok=True)
    database=root/'position-owners.sqlite';db=sqlite3.connect(database,timeout=60);db.execute('pragma journal_mode=WAL');db.execute('pragma synchronous=FULL')
    db.executescript('''create table if not exists owners(asset text,owner text,first_block integer,last_block integer,primary key(asset,owner));
    create table if not exists decoded_files(token text,file text,source_sha256 text,rows integer,receipt text,primary key(token,file));''')
    decoders=[]
    for p in profile['profiles']:
        decoder=PoolDecoder('events',root/'token-profiles'/p['abi'],job['address'],[],schema_version=p['version'])
        decoders.append(decoder)
    boundaries=[(p['block'],p['log_index']) for p in profile['profiles']];completed=0
    def status(state,**extra):
        owner_count=db.execute('select count(*) from owners where asset=?',(job['asset'],)).fetchone()[0]
        write_json(root/'decode-progress.json',dict(state=state,stage='four_asset_token_history',address=job['address'],asset=job['asset'],kind=job['kind'],completed_files=completed,total_files=len(receipt['files']),owners_for_asset=owner_count,updated_at=time.time(),positions_complete=False,**extra))
    try:
        for name,info in sorted(receipt['files'].items(),key=lambda x:x[1]['start']):
            old=db.execute('select source_sha256,rows,receipt from decoded_files where token=? and file=?',(job['address'],name)).fetchone()
            if old:
                if old[0]!=info['sha256']:raise ValueError('Accepted raw source identity changed')
                output_receipt=json.loads(Path(old[2]).read_text())
                if any(not (out/f['path']).exists() or (out/f['path']).stat().st_size!=f['bytes'] for f in output_receipt['outputs']):raise ValueError('Decoded output missing; cannot silently skip accepted checkpoint')
                completed+=1;continue
            source=Path(job['destination'])/name
            if checksum(source)!=info['sha256']:raise ValueError('Raw token checksum differs')
            outputs=[];written=0;seen=set();owner_rows={};writers={};pending_paths={}
            try:
                for batch in pq.ParquetFile(source).iter_batches(batch_size=10000):
                    groups={}
                    for row in batch.to_pylist():
                        index=bisect.bisect_right(boundaries,(row['block_number'],row['log_index']))-1
                        if index<0:raise ValueError('No historical ABI for event')
                        decoder=decoders[index];decoded=decoder.decode_row(row,name);decoded.values['protocol']='aave-v3-user-tokens'
                        if decoded.key in seen:raise ValueError('Duplicate event key')
                        seen.add(decoded.key);key=(index,decoded.name);groups.setdefault(key,[]).append(decoded.values)
                        fields=decoder.decoder.event_by_topic(row['topic0']).abi['inputs']
                        for owner in owners(decoded,fields):
                            old_bounds=owner_rows.get(owner,(row['block_number'],row['block_number']));owner_rows[owner]=(min(old_bounds[0],row['block_number']),max(old_bounds[1],row['block_number']))
                    for (index,event),values in groups.items():
                        key=(index,event);schema=decoders[index].schemas[event]
                        if key not in writers:
                            relative=f"schema-{index+1}/{event}/{name}";target=out/relative;target.parent.mkdir(parents=True,exist_ok=True);temp=target.with_suffix('.parquet.tmp');writers[key]=pq.ParquetWriter(temp,schema,compression='zstd');pending_paths[key]=(relative,target,temp)
                        writers[key].write_table(pa.Table.from_pylist(values,schema=schema));written+=len(values)
            finally:
                for writer in writers.values():writer.close()
            if written!=info['rows']:raise ValueError('Decoded row count differs')
            for key,(relative,target,temp) in pending_paths.items():
                if pq.read_metadata(temp).num_rows<=0:raise ValueError('Invalid decoded table')
                os.replace(temp,target);outputs.append(dict(path=relative,sha256=checksum(target),bytes=target.stat().st_size,rows=pq.read_metadata(target).num_rows))
            file_receipt=out/'receipts'/(name+'.json');write_json(file_receipt,dict(source=str(source),source_sha256=info['sha256'],rows=written,outputs=outputs,profile_sha256=checksum(root/'token-profiles'/(job['address']+'.profiles.json'))))
            with db:
                db.executemany('insert into owners values(?,?,?,?) on conflict(asset,owner) do update set first_block=min(first_block,excluded.first_block),last_block=max(last_block,excluded.last_block)',[(job['asset'],owner,first,last) for owner,(first,last) in owner_rows.items()])
                db.execute('insert into decoded_files values(?,?,?,?,?)',(job['address'],name,info['sha256'],written,str(file_receipt)))
            completed+=1
            if completed%10==0:status('decoding')
        status('token_history_decoded')
        return dict(state='token_history_decoded',address=job['address'],asset=job['asset'],kind=job['kind'],files=completed,rows=sum(x['rows'] for x in receipt['files'].values()),profiles=len(profile['profiles']),positions_complete=False,finished_at=time.time())
    finally:db.close()

def run(once=False):
    current=json.loads((ROOT/'current.json').read_text());root=Path(current['root']);plan=json.loads(Path(current['plan']).read_text());state_path=root/'decode-jobs.json';states=json.loads(state_path.read_text()) if state_path.exists() else {}
    with (root/'decode.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        while True:
            policy=json.loads((root/'priority.json').read_text());jobs=selected_jobs(plan,policy);raw=json.loads((root/'jobs.json').read_text())
            for job in jobs:
                if raw.get(job['address'],{}).get('state')!='raw_accepted' or states.get(job['address'],{}).get('state')=='token_history_decoded':continue
                states[job['address']]=dict(state='processing',address=job['address'],asset=job['asset'],kind=job['kind'],updated_at=time.time());write_json(state_path,states)
                write_json(root/'decode-progress.json',dict(state='preparing_historical_profiles',address=job['address'],stage='four_asset_token_history',positions_complete=False,updated_at=time.time()))
                try:states[job['address']]=decode_job(plan,job,root)
                except Exception as exc:
                    states[job['address']]=dict(state='failed',error=type(exc).__name__+': '+str(exc),updated_at=time.time());write_json(state_path,states);raise
                write_json(state_path,states)
            completed=sum(states.get(j['address'],{}).get('state')=='token_history_decoded' for j in jobs)
            write_json(root/'decode-progress.json',dict(state='focused_token_history_decoded' if completed==len(jobs) else 'waiting_for_inputs',stage='four_asset_token_history',completed_tokens=completed,total_tokens=len(jobs),positions_complete=False,updated_at=time.time(),next_stage='Version-aware scaled position replay and independent node comparisons'))
            if once or completed==len(jobs):return
            time.sleep(30)
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--once',action='store_true');run(parser.parse_args().once)
