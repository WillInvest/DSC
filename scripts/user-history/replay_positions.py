"""Reconstruct sparse scaled positions from versioned token events, resumably."""
import argparse,fcntl,json,sqlite3,subprocess,sys,time
from pathlib import Path
sys.path.insert(0,'/home/dsc-data/scripts')
import pyarrow as pa
import pyarrow.parquet as pq
from shared.artifacts import checksum,write_json,digest
from run import selected_jobs
from validate_positions import validate

ROOT=Path('/home/dsc-data/runs/aave-user-history-v3')
def modern_versions(profile,root):
    versions=[]
    for p in profile['profiles']:
        abi=json.loads((root/'token-profiles'/p['abi']).read_text());mint=next((x for x in abi if x.get('type')=='function' and x.get('name')=='mint'),None)
        if mint and any('scaled' in x['name'].lower() for x in mint['inputs']):versions.append(p['version'])
    return versions

class Math:
    def __init__(self):self.child=subprocess.Popen(['node',str(Path(__file__).with_name('math_worker.mjs'))],stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True,bufsize=1)
    def call(self,payload):
        self.child.stdin.write(json.dumps(payload)+'\n');self.child.stdin.flush();line=self.child.stdout.readline()
        if not line:raise RuntimeError('Original Aave math worker stopped')
        result=json.loads(line)
        if result.get('error'):raise ValueError(result['error'])
        return result
    def close(self):
        self.child.stdin.close()
        try:self.child.wait(timeout=10)
        except subprocess.TimeoutExpired:self.child.terminate();self.child.wait(timeout=10)

def replay_job(plan,job,root,limit=None):
    if job['address'].lower() == '0x786dbff3f1292ae8f92ea68cf93c30b34b1ed04b':
        try:
            from gho_replay import replay_job as replay_gho
        except ImportError as exc:
            raise ValueError('GHO discount debt requires the verified historical adapter; generic replay is forbidden') from exc
        return replay_gho(plan,job,root,limit)
    profile=json.loads((root/'token-profiles'/(job['address']+'.profiles.json')).read_text());modern=modern_versions(profile,root)
    raw=json.loads((root/'receipts'/(job['address']+'.json')).read_text());decoded_root=Path(plan['output'])/'decoded-staging'/job['address']
    out=Path(plan['output'])/'positions-staging'/job['address'];out.mkdir(parents=True,exist_ok=True)
    db=sqlite3.connect(root/'scaled-positions.sqlite',timeout=60);db.execute('pragma journal_mode=WAL');db.execute('pragma synchronous=FULL')
    db.executescript('''create table if not exists positions(token text,asset text,kind text,owner text,scaled text,last_block integer,primary key(token,owner));
    create table if not exists replayed_files(token text,file text,decoded_receipt_sha256 text,history_receipt text,primary key(token,file));''')
    math=Math();complete=0;total_events=0;last_block=job['start_block']-1
    try:
        for name,info in sorted(raw['files'].items(),key=lambda x:x[1]['start']):
            decoded_receipt=decoded_root/'receipts'/(name+'.json')
            if not decoded_receipt.exists():break
            receipt_hash=checksum(decoded_receipt);old=db.execute('select decoded_receipt_sha256,history_receipt from replayed_files where token=? and file=?',(job['address'],name)).fetchone()
            if old:
                if old[0]!=receipt_hash or not Path(old[1]).exists():raise ValueError('Replay input/checkpoint identity differs')
                complete+=1;last_block=info['stop']-1
                if limit and complete>=limit:break
                continue
            receipt=json.loads(decoded_receipt.read_text());events=[]
            for output in receipt['outputs']:
                event=Path(output['path']).parent.name
                if event not in ['Mint','Burn','BalanceTransfer']:continue
                path=decoded_root/output['path']
                if checksum(path)!=output['sha256']:raise ValueError('Decoded table hash differs')
                events.extend(pq.read_table(path).to_pylist())
            if job['kind']=='stableDebtToken' and events:raise ValueError('Stable-debt activity requires a separately verified replay adapter')
            events.sort(key=lambda r:(r['block_number'],r['transaction_index'],r['log_index']))
            owners=set()
            for row in events:
                for field in ['arg__onBehalfOf','arg__from','arg__to']:
                    # Hydrate every actual ledger holder across chunks. A zero
                    # address can hold scaled aTokens after BalanceTransfer.
                    if row.get(field):owners.add(row[field])
            initial={};owner_list=list(owners)
            for offset in range(0,len(owner_list),500):
                group=owner_list[offset:offset+500];sql='select owner,scaled from positions where token=? and owner in ('+','.join('?' for _ in group)+')'
                initial.update(db.execute(sql,[job['address'],*group]).fetchall())
            result=math.call(dict(initial=initial,events=events,modern_versions=modern,debt=job['kind']=='variableDebtToken')) if events else dict(balances={},history=[])
            history=result['history'];target=out/(name.removesuffix('.parquet')+'.changes.parquet');history_output=None
            if history:
                schema=pa.schema([('owner',pa.string()),('before',pa.string()),('after',pa.string()),('delta',pa.string()),('block',pa.uint64()),('transaction_index',pa.uint64()),('log_index',pa.uint64()),('event',pa.string()),('index',pa.string())])
                temp=target.with_suffix('.parquet.tmp');table=pa.Table.from_pylist(history,schema=schema);pq.write_table(table,temp,compression='zstd')
                if pq.read_table(temp).to_pylist()!=table.to_pylist():raise ValueError('Position history readback differs')
                temp.replace(target);history_output=dict(path=str(target),rows=len(history),sha256=checksum(target))
            history_receipt=out/'receipts'/(name+'.json');write_json(history_receipt,dict(source_receipt=str(decoded_receipt),source_sha256=receipt_hash,profile_sha256=checksum(root/'token-profiles'/(job['address']+'.profiles.json')),math_kernel_sha256=checksum(Path(__file__).with_name('math-kernel.json')),math_worker_sha256=checksum(Path(__file__).with_name('math_worker.mjs')),replay_worker_sha256=checksum(Path(__file__)),history=history_output,scaled_positions_validated=False))
            with db:
                db.executemany('insert into positions values(?,?,?,?,?,?) on conflict(token,owner) do update set scaled=excluded.scaled,last_block=excluded.last_block',[(job['address'],job['asset'],job['kind'],owner,balance,info['stop']-1) for owner,balance in result['balances'].items()])
                db.execute('insert into replayed_files values(?,?,?,?)',(job['address'],name,receipt_hash,str(history_receipt)))
            complete+=1;total_events+=len(events);last_block=info['stop']-1
            if complete%100==0 or any(info['start']<=p['block']<info['stop'] for p in profile['profiles'][1:]):
                validate(root,job,dict(through_block=last_block),limit=16)
            if complete%10==0:write_json(root/'position-progress.json',dict(state='replaying',stage='four_asset_scaled_positions',address=job['address'],asset=job['asset'],kind=job['kind'],completed_files=complete,total_files=len(raw['files']),through_block=last_block,positions_validated=False,updated_at=time.time()))
            if limit and complete>=limit:break
        result=dict(state='scaled_replay_complete' if complete==len(raw['files']) else 'partial_scaled_replay',address=job['address'],asset=job['asset'],kind=job['kind'],completed_files=complete,total_files=len(raw['files']),through_block=last_block,owners=db.execute('select count(*) from positions where token=?',(job['address'],)).fetchone()[0],processed_events_this_run=total_events,modern_versions=modern,node_validated=False,updated_at=time.time())
        return result
    finally:math.close();db.close()

def run(once=False,limit=None):
    current=json.loads((ROOT/'current.json').read_text());root=Path(current['root']);plan=json.loads(Path(current['plan']).read_text());status_path=root/'position-jobs.json';states=json.loads(status_path.read_text()) if status_path.exists() else {}
    with (root/'positions.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        while True:
            jobs=selected_jobs(plan,json.loads((root/'priority.json').read_text()));decoded=json.loads((root/'decode-jobs.json').read_text()) if (root/'decode-jobs.json').exists() else {}
            for job in jobs:
                if states.get(job['address'],{}).get('state')=='scaled_replay_complete':continue
                if decoded.get(job['address'],{}).get('state')!='token_history_decoded':continue
                try:
                    states[job['address']]=replay_job(plan,job,root,limit)
                    states[job['address']]['node_scaled_sample']=validate(root,job,states[job['address']])
                except Exception as exc:states[job['address']]=dict(state='failed',error=type(exc).__name__+': '+str(exc),updated_at=time.time());write_json(status_path,states);raise
                write_json(status_path,states)
                if limit:return
            complete=sum(states.get(j['address'],{}).get('state')=='scaled_replay_complete' for j in jobs)
            write_json(root/'position-progress.json',dict(state='focused_scaled_replay_complete' if complete==len(jobs) else 'waiting_for_decoded_history',stage='four_asset_scaled_positions',completed_tokens=complete,total_tokens=len(jobs),positions_validated=False,updated_at=time.time()))
            if once or complete==len(jobs):return
            time.sleep(30)
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--once',action='store_true');p.add_argument('--limit',type=int);a=p.parse_args();run(a.once,a.limit)
