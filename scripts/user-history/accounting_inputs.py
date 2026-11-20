"""Resumable factorized nominal/HF inputs; consume accepted DSC data, never globs of canonical files."""
import json,sqlite3,sys,time,subprocess,re
from pathlib import Path
sys.path.insert(0,'/home/dsc-data/scripts')
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from web3 import Web3
from decoding.pool_decode import EventDecoder
from pipeline.compaction.reader import snapshot
from pipeline.runtime import ssh,NODE,NODE_PYTHON,NODE_SCRIPTS,run
from shared.artifacts import checksum,write_json
from oracle_common import POOL,START
ROOT=Path('/home/dsc-data/runs/aave-user-history-v3')
SCRIPTS=Path('/home/dsc-data/scripts')
def read(path):return json.loads(Path(path).read_text())
def connect(root):
    db=sqlite3.connect(Path(root)/'accounting.sqlite',timeout=60);db.execute('pragma journal_mode=WAL');db.execute('pragma synchronous=FULL')
    db.executescript('''create table if not exists files(path text primary key,sha256 text,rows integer);
    create table if not exists headers(block integer primary key,timestamp integer,hash text);
    create table if not exists pool_events(block integer,tx integer,log integer,name text,args text,primary key(block,log));
    create table if not exists risk_events(block integer,tx integer,log integer,name text,args text,primary key(block,log));
    create table if not exists snapshots(kind text,key text,block integer,data text,primary key(kind,key,block));
    create table if not exists changes(token text,owner text,block integer,tx integer,log integer,scaled text,primary key(token,owner,block,tx,log));
    create table if not exists gho_states(token text,owner text,block integer,tx integer,log integer,previous_index text,discount_percent text,primary key(token,owner,block,tx,log));
    create index if not exists change_lookup on changes(owner,token,block desc,tx desc,log desc);
    create table if not exists validations(id text primary key,data text);
    create table if not exists nominal_tokens(token text primary key,receipt text);
    create index if not exists pool_by_reserve on pool_events(name,json_extract(args,'$.reserve') collate nocase,block desc,log desc);
    create index if not exists pool_by_user on pool_events(name,json_extract(args,'$.user') collate nocase,block desc,log desc);
    ''');return db

def profiles():
    result=[]
    for path in sorted((SCRIPTS.parent/'contracts/ethereum/aave-profiles').glob('aave-v3-*.json')):
        p=read(path);p['key']=str(p['from_block']);abi_path=SCRIPTS/'protocols/lending/aave/v3'/p['abi']
        if checksum(abi_path)!=p['abi_sha256']:raise ValueError('Reviewed Pool ABI identity differs: '+str(abi_path))
        p['abi']=read(abi_path);result.append(p)
    extra=SCRIPTS.parent/'artifacts/aave-interest-2026-11-14/26392162.profile.json'
    if extra.exists():
        p=read(extra);p['key']=str(p['from_block']);result.append(p)
    result=sorted(result,key=lambda p:p['from_block'])
    if not result or result[0]['from_block']!=START:raise ValueError('Computation profiles do not cover Pool deployment')
    return result
def profile_at(profs,b):return max((p for p in profs if p['from_block']<=b),key=lambda p:p['from_block'])
def node(plan,payload):
    p=dict(payload,end_block=plan['end_block'],end_hash=plan['end_hash'],pool=POOL)
    result=subprocess.run(ssh(NODE,[NODE_PYTHON,'-B',NODE_SCRIPTS+'/user-history/accounting_node.py']),input=json.dumps(p),capture_output=True,text=True,check=True,timeout=900)
    return json.loads(result.stdout)
def import_file(db,path,table,rows):
    path=Path(path);sha=checksum(path);old=db.execute('select sha256 from files where path=?',(str(path),)).fetchone()
    if old:
        if old[0]!=sha:raise ValueError('Accepted accounting source changed: '+str(path))
        return False
    with db:
        if table=='headers':db.executemany('insert into headers values(?,?,?)',[(r['block_number'],r['timestamp'],r['block_hash']) for r in rows])
        elif table=='changes':db.executemany('insert into changes values(?,?,?,?,?,?)',rows)
        elif table=='gho_states':db.executemany('insert into gho_states values(?,?,?,?,?,?,?)',rows)
        else:db.executemany('insert into '+table+' values(?,?,?,?,?)',rows)
        db.execute('insert into files values(?,?,?)',(str(path),sha,len(rows)))
    return True

def indexed(db,path,expected=None):
    old=db.execute('select sha256 from files where path=?',(str(path),)).fetchone()
    if not old:return False
    actual=checksum(path)
    if actual!=old[0] or expected is not None and actual!=expected:raise ValueError('Indexed accounting input identity changed: '+str(path))
    return True

def prepare_headers(root,plan,status):
    target=Path(plan['output'])/'accounting/headers';receipt=root/'accounting-header-receipt.json'
    if receipt.exists():
        m=read(receipt)
        if m['end_hash']!=plan['end_hash']:raise ValueError('Header cutoff mismatch')
        return m
    node_root='/home/node/dsc-extraction/aave-user-history-v3/'+plan['run_id']+'/accounting-headers'
    p=dict(operation='headers',start_block=START,end_block=plan['end_block'],end_hash=plan['end_hash'],output=node_root)
    log=root/'accounting-headers.log';command=ssh(NODE,[NODE_PYTHON,'-B',NODE_SCRIPTS+'/user-history/accounting_node.py'])
    # Payload file is immutable auxiliary job input, separate from primary coverage.
    request=root/'accounting-header-request.json';write_json(request,p)
    with request.open() as inp,log.open('a') as out:
        child=subprocess.Popen(command,stdin=inp,stdout=out,stderr=subprocess.STDOUT)
        last_value=None;last_change=time.monotonic()
        try:
            while child.poll() is None:
                progress=subprocess.run(ssh(NODE,['cat',node_root+'/progress.json']),capture_output=True,text=True,timeout=30)
                live=json.loads(progress.stdout) if progress.returncode==0 else {}
                value=live.get('completed_blocks')
                if value!=last_value:last_value=value;last_change=time.monotonic()
                if time.monotonic()-last_change>3600:raise TimeoutError('Block timestamp extraction made no progress for one hour')
                status('collecting_block_times',waiting=['Own-node block timestamp extraction'],current=dict(stage='Cryo block headers',**live));time.sleep(10)
            if child.returncode:raise RuntimeError('Header Cryo failed; see '+str(log))
        finally:
            if child.poll() is None:
                cleanup=dict(p,operation='stop_headers')
                subprocess.run(command,input=json.dumps(cleanup),text=True,capture_output=True,timeout=60)
                child.terminate();child.wait(timeout=60)
    target.mkdir(parents=True,exist_ok=True)
    subprocess.run(['rsync','-a','--partial',NODE+':'+node_root+'/',str(target)+'/'],check=True,stdout=subprocess.DEVNULL)
    # This is newly collected, dedicated staging, not a canonical catalog.
    files=[];through=START-1
    for path in sorted(target.glob('*.parquet')):
        t=pq.read_table(path);rows=t.to_pylist();blocks=[r['block_number'] for r in rows]
        if blocks!=list(range(through+1,through+1+len(blocks))):raise ValueError('Header block gap/duplicate')
        through=blocks[-1];files.append(dict(path=str(path),sha256=checksum(path),start=blocks[0],stop=blocks[-1]+1,rows=len(rows)))
    if through!=plan['end_block']:raise ValueError('Header history incomplete')
    final=pq.read_table(files[-1]['path']).to_pylist()[-1]
    if final['block_hash'].lower()!=plan['end_hash'].lower():raise ValueError('Header ending hash mismatch')
    m=dict(start=START,stop=through+1,end_hash=plan['end_hash'],files=files,source='Own RETH via Cryo blocks; every block timestamp/hash',created_at=time.time());write_json(receipt,m);return m

def event_rows(paths,decoders,names,start,stop):
    topics=list(decoders);out=[]
    for path in paths:
        rows=ds.dataset(str(path),format='parquet').to_table(filter=(ds.field('block_number')>=start)&(ds.field('block_number')<stop)&ds.field('topic0').isin(topics)).to_pylist()
        for row in rows:
            name,fields,values,_=decoders[row['topic0']].decode(row)
            if name in names:out.append((row['block_number'],row['transaction_index'],row['log_index'],name,json.dumps(dict(zip([f['name'] for f in fields],values)),default=str)))
    return out

def ingest_pool(db,root,plan,profs,status):
    names={'ReserveDataUpdated','ReserveUsedAsCollateralEnabled','ReserveUsedAsCollateralDisabled','UserEModeSet','Upgraded'}
    upgrade=dict(type='event',name='Upgraded',anonymous=False,inputs=[dict(name='implementation',type='address',indexed=True)])
    event=Web3().eth.contract(abi=[upgrade]).events.Upgraded()
    bytopic={event.topic:EventDecoder([upgrade])}
    for p in profs:
        for a in p['abi']:
            if a.get('type')=='event' and a['name'] in names:
                event=Web3().eth.contract(abi=[a]).get_event_by_name(a['name'])();bytopic[event.topic]=EventDecoder([a])
    reuse=read(root/'pool-reuse.json');baseline=read(reuse['baseline_plan']);job=next(j for j in baseline['jobs'] if j['address'].lower()==POOL.lower());manifest=read(Path(reuse['baseline_plan']).parent/'receipts'/(job['address']+'.json'))
    boundary=START+((baseline['end_block']-START)//10000)*10000
    count=0
    for name in sorted(manifest['files']):
        match=re.search(r'__(\d+)_to_(\d+)\.parquet$',name)
        if not match:raise ValueError('Unexpected baseline filename')
        start,end=map(int,match.groups())
        if start>=boundary:continue
        path=Path(job['destination'])/name
        accepted=manifest['files'][name]
        if indexed(db,path,accepted.get('sha256')):continue
        if path.stat().st_size!=accepted['bytes'] or pq.read_metadata(path).num_rows!=accepted['rows']:raise ValueError('Baseline Pool receipt metadata differs')
        if accepted.get('sha256') and checksum(path)!=accepted['sha256']:raise ValueError('Baseline Pool receipt hash differs')
        rows=event_rows([path],bytopic,names,start,min(end+1,plan['end_block']+1));import_file(db,path,'pool_events',rows);count+=len(rows)
        status('indexing_pool_events',waiting=['Indexing reused Pool event history'],current={'file':name,'rows':count})
    with snapshot('/home/dsc-data/data/compacted') as files:
        selected=[f for f in files if f['stream'].startswith('encoded/aave-v3/'+POOL.lower()+'/') and f['start']>=boundary and f['start']<=plan['end_block']]
        cursor=boundary
        for f in sorted(selected,key=lambda f:f['start']):
            if f['start']!=cursor:raise ValueError('Canonical Pool tail interval gap/overlap')
            cursor=f['stop']
        if cursor<=plan['end_block']:raise ValueError('Canonical Pool tail does not reach cutoff')
        for f in selected:
            path=Path(f['absolute_path'])
            if checksum(path)!=f['sha256']:raise ValueError('Canonical source hash differs')
            if not f['sealed'] or f['stop']>plan['end_block']+1:
                folder=root/'accounting-pool-pins';folder.mkdir(exist_ok=True)
                pin=folder/(str(f['start'])+'-'+str(min(f['stop'],plan['end_block']+1))+'.json')
                if indexed(db,pin):continue
                if not pin.exists():write_json(pin,dict(source=f,cutoff=plan['end_block'],rows=event_rows([path],bytopic,names,f['start'],min(f['stop'],plan['end_block']+1))))
                import_file(db,pin,'pool_events',read(pin)['rows'])
            elif not indexed(db,path,f['sha256']):
                rows=event_rows([path],bytopic,names,f['start'],min(f['stop'],plan['end_block']+1));import_file(db,path,'pool_events',rows)
    if not selected or max(f['stop'] for f in selected)<=plan['end_block']:raise ValueError('Canonical Pool tail missing fixed cutoff')
    actual={b:json.loads(args)['implementation'].lower() for b,args in db.execute("select block,args from pool_events where name='Upgraded' order by block,log")}
    validate_pool_upgrades(profs,actual)

def validate_pool_upgrades(profs,actual):
    # The initial implementation is a reviewed deployment seed. The proxy
    # initializer need not emit Upgraded; subsequent implementation changes must.
    expected={p['from_block']:p['implementation'].lower() for p in profs}
    initial=expected.pop(START)
    actual=dict(actual)
    if START in actual and actual.pop(START)!=initial:
        raise ValueError('Initial Pool implementation event differs from reviewed deployment')
    if actual!=expected:
        raise ValueError('Historical Pool upgrade events differ from reviewed computation profiles: '+str(actual))

def ingest_risk(db,root,plan,proof_path):
    abi=read(SCRIPTS/'reconstruction/abis/v3-IPoolConfigurator.json')+read(proof_path)['abi'];bytopic={};names=set()
    for a in abi:
        if a.get('type')=='event' and (a['name']=='CollateralConfigurationChanged' or 'EMode' in a['name']):
            event=Web3().eth.contract(abi=[a]).get_event_by_name(a['name'])();bytopic[event.topic]=EventDecoder([a]);names.add(a['name'])
    auxiliary=Path(plan['reused_auxiliary'][0]['manifest']);aux=read(auxiliary);tail=read('/home/dsc-data/runs/aave-oracle-v3/'+plan['run_id']+'/routing-events.json')
    for source in aux['sources']:
        if source['provider'] or not int(source['address'],16):continue
        paths=[auxiliary.parent/source['address']/('ethereum__logs__'+str(b)+'_to_'+str(min(b+9999,aux['request']['stop']-1))+'.parquet') for b in range(aux['request']['start'],aux['request']['stop'],10000)]
        extra=next(t for t in tail['tail'] if t['address']==source['address']);paths.extend(Path(f['path']) for f in extra['files'])
        if any(checksum(f['path'])!=f['sha256'] for f in extra['files']):raise ValueError('Risk routing-tail receipt hash differs')
        for path in paths:
            if indexed(db,path):continue
            rows=event_rows([path],bytopic,names,START,plan['end_block']+1);import_file(db,path,'risk_events',rows)

def snapshots(db,root,plan,profs,status):
    inits=read(root/'reserve-initializations.json');tasks={}
    def add(kind,key,b):
        if not db.execute('select 1 from snapshots where kind=? and key=? and block=?',(kind,key,b)).fetchone():tasks[kind,key,b]=dict(kind=kind,asset=key,category=int(key) if kind=='mode' else None,block=b,profile=profile_at(profs,b)['key'])
    categories={}
    for b,args in db.execute('select block,args from risk_events order by block,tx,log'):
        a=json.loads(args)
        if 'asset' in a:add('reserve',a['asset'].lower(),b)
        cat=a.get('categoryId')
        if cat is not None:categories.setdefault(str(cat),b);add('mode',str(cat),b)
    for x in inits:add('reserve',x['asset'].lower(),x['block'])
    for p in profs[1:]:
        b=p['from_block']
        for x in inits:
            if x['block']<=b:add('reserve',x['asset'].lower(),b)
        for cat,initial in categories.items():
            if initial<=b:add('mode',cat,b)
    values=list(tasks.values())
    for offset in range(0,len(values),64):
        chunk=values[offset:offset+64];status('collecting_configuration',waiting=['Historical risk/eMode and upgrade boundary snapshots'],current={'completed':offset,'total':len(values)})
        out=node(plan,dict(operation='snapshots',abis={p['key']:p['abi'] for p in profs},tasks=chunk))
        with db:
            for row in out:
                key=row['asset'] if row['kind']=='reserve' else str(row['category']);db.execute('insert into snapshots values(?,?,?,?)',(row['kind'],key,row['block'],json.dumps(row)))

def import_changes(db,root,plan,status):
    state=read(root/'position-jobs.json');available=[]
    for job in plan['jobs']:
        if state.get(job['address'],{}).get('state')!='scaled_replay_complete':continue
        raw=read(root/'receipts'/(job['address']+'.json'));folder=Path(plan['output'])/'positions-staging'/job['address']
        for name in raw['files']:
            p=folder/'receipts'/(name+'.json');receipt=read(p)
            for key,table in [('history','changes'),('gho_history','gho_states')]:
                hist=receipt.get(key)
                if hist is None:continue
                path=Path(hist['path'])
                if indexed(db,path,hist['sha256']):continue
                if checksum(path)!=hist['sha256']:raise ValueError('Share/GHO state history hash differs')
                rows=pq.read_table(path).to_pylist()
                values=[(job['address'],r['owner'],r['block'],r['transaction_index'],r['log_index'],*([r['after']] if table=='changes' else [r['previous_index'],r['discount_percent']])) for r in rows]
                import_file(db,path,table,values)
        available.append(job)
        status('indexing_share_history',current={'token':job['address'],'completed_tokens':len(available)})
    return available

def gho_borrowing_flags(db,plan,profs,status):
    """GHO legacy full-repayment dust can outlive its Pool borrowing flag.

    Use sparse authoritative Pool user-configuration snapshots at GHO actions
    and upgrades, instead of equating positive dust shares with borrowing.
    These getter values are production supplements, not comparison references.
    """
    token='0x786dbff3f1292ae8f92ea68cf93c30b34b1ed04b';asset='0x40d16fc0246ad3160ccc09b8d0d3a2cd28ae6c2f'
    points=set(db.execute('select distinct owner,block from gho_states where token=?',(token,)))
    firsts=db.execute('select owner,min(block) from gho_states where token=? group by owner',(token,)).fetchall()
    for p in profs:
        points.update((owner,p['from_block']) for owner,first in firsts if first<=p['from_block']<=plan['end_block'])
    pending=[(owner,b) for owner,b in sorted(points,key=lambda x:(x[1],x[0])) if not db.execute("select 1 from snapshots where kind='gho_borrowing' and key=? and block=?",(owner,b)).fetchone()]
    for offset in range(0,len(pending),64):
        group=pending[offset:offset+64];status('collecting_configuration',waiting=['GHO historical borrowing flags'],current={'stage':'GHO action/upgrade user configuration','completed':offset,'total':len(pending)})
        tasks=[dict(kind='call',function='getUserConfiguration',args=[owner],block=b,profile=profile_at(profs,b)['key']) for owner,b in group]
        selected={t['profile'] for t in tasks}
        values=node(plan,dict(operation='snapshots',abis={p['key']:p['abi'] for p in profs if p['key'] in selected},tasks=tasks))
        if len(values)!=len(group):raise ValueError('GHO borrowing snapshot count differs')
        with db:
            for (owner,b),value in zip(group,values):
                row=db.execute("select data from snapshots where kind='reserve' and key=? and block<=? order by block desc limit 1",(asset,b)).fetchone()
                if row is None:raise ValueError('Missing GHO reserve ID at flag snapshot')
                reserve_id=int(json.loads(row[0])['reserve']['id']);configuration=int(value['result'][0])
                record=dict(owner=owner,block=b,block_hash=value['block_hash'],configuration=str(configuration),reserve_id=reserve_id,borrowing=bool((configuration>>(2*reserve_id))&1),source='Own RETH Pool.getUserConfiguration; production input for GHO action/upgrade borrowing state')
                db.execute('insert into snapshots values(?,?,?,?)',('gho_borrowing',owner,b,json.dumps(record)))
