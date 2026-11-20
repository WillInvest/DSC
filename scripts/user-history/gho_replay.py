"""Historical GHO debt replay: original discount math plus persisted per-user state."""
import json, sqlite3, subprocess, time, os
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from shared.artifacts import checksum, write_json
from validate_positions import validate

STANDARD_IMPLEMENTATIONS = {'0x9b2b73f9ddd830f82d61520388ccf4fc048f9953','0x1b147fc485c1952b7e76fa99e9af0d5cb2ba9a79', '0xc4bea6ff17879e27f266909397e5e0ad3d301946'}

class GhoMath:
    def __init__(self):
        here=Path(__file__).parent
        command=['node',str(here/'gho_worker.mjs'),'--artifact',str(here/'gho-kernel.json'),'--math-kernel',str(here/'math-kernel.json')]
        if os.environ.get('AAVE_NODE_RUNTIME'):command += ['--runtime-root',os.environ['AAVE_NODE_RUNTIME']]
        self.child=subprocess.Popen(command,stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True,bufsize=1)
    def call(self,payload):
        self.child.stdin.write(json.dumps(payload)+'\n');self.child.stdin.flush();line=self.child.stdout.readline()
        if not line:raise RuntimeError('Original GHO math worker stopped')
        result=json.loads(line)
        if result.get('error'):raise ValueError(result['error'])
        return result
    def close(self):
        self.child.stdin.close()
        try:self.child.wait(timeout=10)
        except subprocess.TimeoutExpired:self.child.terminate();self.child.wait(timeout=10)

def replay_job(plan,job,root,limit=None):
    from replay_positions import modern_versions
    root=Path(root);here=Path(__file__).parent;kernel=here/'gho-kernel.json'
    if not kernel.exists():raise ValueError('GHO discount adapter artifact unavailable; generic replay forbidden')
    profile_path=root/'token-profiles'/(job['address']+'.profiles.json')
    profile=json.loads(profile_path.read_text());supported=set(json.loads(kernel.read_text())['implementations'])|STANDARD_IMPLEMENTATIONS
    routes={str(p['version']):p['implementation'].lower() for p in profile['profiles']}
    unknown=set(routes.values())-supported
    if unknown:raise ValueError('Unverified GHO historical implementation: '+','.join(sorted(unknown)))
    modern=modern_versions(profile,root)
    raw=json.loads((root/'receipts'/(job['address']+'.json')).read_text());decoded_root=Path(plan['output'])/'decoded-staging'/job['address']
    out=Path(plan['output'])/'positions-staging'/job['address'];out.mkdir(parents=True,exist_ok=True)
    db=sqlite3.connect(root/'scaled-positions.sqlite',timeout=60);db.execute('pragma journal_mode=WAL');db.execute('pragma synchronous=FULL')
    db.executescript('''create table if not exists positions(token text,asset text,kind text,owner text,scaled text,last_block integer,primary key(token,owner));
    create table if not exists replayed_files(token text,file text,decoded_receipt_sha256 text,history_receipt text,primary key(token,file));
    create table if not exists gho_user_state(token text,owner text,previous_index text,discount_percent text,primary key(token,owner));''')
    math=GhoMath();complete=0;total_events=0;last_block=job['start_block']-1
    try:
        for name,info in sorted(raw['files'].items(),key=lambda x:x[1]['start']):
            decoded_receipt=decoded_root/'receipts'/(name+'.json')
            if not decoded_receipt.exists():break
            receipt_hash=checksum(decoded_receipt)
            old=db.execute('select decoded_receipt_sha256,history_receipt from replayed_files where token=? and file=?',(job['address'],name)).fetchone()
            if old:
                if old[0]!=receipt_hash or not Path(old[1]).exists():raise ValueError('GHO replay checkpoint identity differs')
                old_receipt=json.loads(Path(old[1]).read_text())
                if not old_receipt.get('gho_worker_sha256'):raise ValueError('Generic GHO checkpoint is not a verified discount replay; explicit recovery required')
                complete+=1;last_block=info['stop']-1
                if limit and complete>=limit:break
                continue
            receipt=json.loads(decoded_receipt.read_text());events=[]
            for output in receipt['outputs']:
                event=Path(output['path']).parent.name
                if event not in ['Mint','Burn','BalanceTransfer','DiscountPercentUpdated']:continue
                path=decoded_root/output['path']
                if checksum(path)!=output['sha256']:raise ValueError('Decoded GHO table hash differs')
                events.extend(pq.read_table(path).to_pylist())
            events.sort(key=lambda r:(r['block_number'],r['transaction_index'],r['log_index']))
            owners={str(row[field]).lower() for row in events for field in ['arg__onBehalfOf','arg__from','arg__user'] if row.get(field)}
            initial={}
            for offset in range(0,len(owners),500):
                group=sorted(owners)[offset:offset+500];marks=','.join('?' for _ in group)
                balances=dict(db.execute('select owner,scaled from positions where token=? and owner in ('+marks+')',[job['address'],*group]))
                extra={a:(i,d) for a,i,d in db.execute('select owner,previous_index,discount_percent from gho_user_state where token=? and owner in ('+marks+')',[job['address'],*group])}
                for owner in group:
                    if owner in balances and owner not in extra:raise ValueError('Existing GHO shares lack discount state; explicit recovery required')
                    index,discount=extra.get(owner,('0','0'));initial[owner]=dict(shares=balances.get(owner,'0'),previous_index=index,discount_percent=discount)
            result=math.call(dict(operation='replay',initial=initial,events=events,profiles=routes,modern_versions=modern)) if events else dict(balances={},states={},history=[],gho_history=[])
            def table_file(rows,suffix,schema):
                if not rows:return None
                target=out/(name.removesuffix('.parquet')+suffix+'.parquet');temp=target.with_suffix('.parquet.tmp');table=pa.Table.from_pylist(rows,schema=schema);pq.write_table(table,temp,compression='zstd')
                if pq.read_table(temp).to_pylist()!=table.to_pylist():raise ValueError('GHO history readback differs')
                temp.replace(target);return dict(path=str(target),rows=len(rows),sha256=checksum(target))
            keys=[('owner',pa.string()),('block',pa.uint64()),('transaction_index',pa.uint64()),('log_index',pa.uint64())]
            history=table_file(result['history'],'.changes',pa.schema([*keys,('before',pa.string()),('after',pa.string()),('delta',pa.string()),('event',pa.string()),('index',pa.string())]))
            gho_history=table_file(result['gho_history'],'.gho-state',pa.schema([*keys,('previous_index',pa.string()),('discount_percent',pa.string())]))
            history_receipt=out/'receipts'/(name+'.json');write_json(history_receipt,dict(source_receipt=str(decoded_receipt),source_sha256=receipt_hash,profile_sha256=checksum(profile_path),math_kernel_sha256=checksum(here/'math-kernel.json'),gho_kernel_sha256=checksum(kernel),gho_worker_sha256=checksum(here/'gho_worker.mjs'),replay_worker_sha256=checksum(Path(__file__)),history=history,gho_history=gho_history,scaled_positions_validated=False))
            with db:
                db.executemany('insert into positions values(?,?,?,?,?,?) on conflict(token,owner) do update set scaled=excluded.scaled,last_block=excluded.last_block',[(job['address'],job['asset'],job['kind'],owner,balance,info['stop']-1) for owner,balance in result['balances'].items()])
                db.executemany('insert into gho_user_state values(?,?,?,?) on conflict(token,owner) do update set previous_index=excluded.previous_index,discount_percent=excluded.discount_percent',[(job['address'],owner,s['previous_index'],s['discount_percent']) for owner,s in result['states'].items()])
                db.execute('insert into replayed_files values(?,?,?,?)',(job['address'],name,receipt_hash,str(history_receipt)))
            complete+=1;total_events+=len(events);last_block=info['stop']-1
            if complete%100==0 or any(info['start']<=p['block']<info['stop'] for p in profile['profiles'][1:]):validate(root,job,dict(through_block=last_block),limit=16)
            if complete%10==0:write_json(root/'position-progress.json',dict(state='replaying',stage='gho_discount_scaled_positions',address=job['address'],asset=job['asset'],kind=job['kind'],completed_files=complete,total_files=len(raw['files']),through_block=last_block,positions_validated=False,updated_at=time.time()))
            if limit and complete>=limit:break
        return dict(state='scaled_replay_complete' if complete==len(raw['files']) else 'partial_scaled_replay',address=job['address'],asset=job['asset'],kind=job['kind'],completed_files=complete,total_files=len(raw['files']),through_block=last_block,owners=db.execute('select count(*) from positions where token=?',(job['address'],)).fetchone()[0],processed_events_this_run=total_events,modern_versions=modern,gho_discount_adapter=True,node_validated=False,updated_at=time.time())
    finally:math.close();db.close()
