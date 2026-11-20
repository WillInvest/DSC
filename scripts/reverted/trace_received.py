"""Trace accepted receipt batches on dsc-revert with resumable per-block evidence."""
import argparse
import concurrent.futures as cf
from contextlib import ExitStack
import fcntl
import gzip
import io
import json
import os
from pathlib import Path
import shutil
import signal
import sqlite3
import sys
import time
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from reverted.common import OPTIONS,save,load,validate_trace
from reverted.common import called_addresses
from shared.artifacts import digest,checksum,write_json


class RpcFailure(Exception):
    def __init__(self,message,delay=30,disabled=False):
        super().__init__(message);self.delay=delay;self.disabled=disabled


class DrainRequested(Exception):
    """Stop between RPCs after saving the response already in flight."""


class Account:
    def __init__(self,entry,provider=None):
        from web3 import HTTPProvider
        self.name=entry['name'];self.kind='reth' if provider is not None else 'drpc'
        self.secret=Path(entry['key_file']).read_text().strip() if provider is None else ''
        self.interval=1 if self.kind=='drpc' else 0
        self.chain_checked=self.kind=='drpc'
        self.session=None
        if provider is None:
            from requests import Session
            from requests.adapters import HTTPAdapter
            self.session=Session()
            adapter=HTTPAdapter(pool_connections=1,pool_maxsize=1,pool_block=True,max_retries=0)
            self.session.mount('https://',adapter);self.session.mount('http://',adapter)
        # One in-flight task per key makes its explicit session safe across threads.
        # Without this, Web3 caches one session per key AND executor thread.
        self.provider=provider or HTTPProvider('https://lb.drpc.live/ethereum/'+self.secret,session=self.session,
            request_kwargs={'timeout':(10,150)},exception_retry_configuration=None)
        self.last=0;self.requests=0
    def rpc(self,method,params):
        from requests.exceptions import HTTPError,RequestException
        if not self.chain_checked and method!='eth_chainId':
            if self.rpc('eth_chainId',[])!='0x1':raise RpcFailure('RETH must be Ethereum mainnet',disabled=True)
            self.chain_checked=True
        # dRPC is paced per key; RETH uses bounded concurrent slots.
        time.sleep(max(0,self.interval-(time.monotonic()-self.last)));self.last=time.monotonic();self.requests+=1
        try:response=self.provider.make_request(method,params)
        except HTTPError as error:
            code=error.response.status_code
            delay=60
            try:delay=max(60,float(error.response.headers.get('Retry-After',60)))
            except (ValueError,TypeError):pass
            raise RpcFailure(f'HTTP {code}',delay,code in (401,402,403)) from None
        except RequestException as error:raise RpcFailure(type(error).__name__,30) from None
        if response.get('error'):
            message=self.redact(response['error']);lower=message.lower()
            disabled=any(s in lower for s in ('insufficient balance','quota exceeded','quota exhausted','credits exhausted','unauthorized','invalid key','payment required'))
            delay=60 if any(s in lower for s in ('rate','limit','too many')) else 30
            raise RpcFailure(message,delay,disabled)
        if response.get('result') is None:raise RpcFailure('Missing RPC result')
        return response['result']
    def redact(self,message):
        text=str(message)
        return (text.replace(self.secret,'[redacted]') if self.secret else text)[:500]


def strategy(row):
    failed=row['failed_receipts']
    if len(failed)!=row['failed_transaction_count']:raise ValueError('Failed receipt count mismatch')
    if any(r['status']!='0x0' or int(r['blockNumber'],16)!=row['block_number'] or r['blockHash']!=row['block_hash'] for r in failed):raise ValueError('Failed receipt identity mismatch')
    indices=[int(r['transactionIndex'],16) for r in failed]
    if len(set(indices))!=len(indices) or any(i<0 or i>=row['transaction_count'] for i in indices):raise ValueError('Invalid failed indices')
    score=sum(i+1 for i in indices)
    if score!=row['failed_index_sum']:raise ValueError('Index sum mismatch')
    return 'skip' if not failed else 'block' if score>row['transaction_count'] else 'individual'


def trace_block(row,account,work,identity,stop_requested=lambda:False):
    number=row['block_number'];target=work/f'{number}.json.gz';method=strategy(row)
    if target.exists():
        result=load(target)
        if result['identity']!=identity or result['receipt_summary']!=row:raise ValueError('Changed checkpoint identity')
        return result
    failed=row['failed_receipts'];entries=[];provider=getattr(account,'kind','drpc')
    if method=='block':
        # Hash pins the exact block already accepted in the receipt archive.
        if stop_requested():raise DrainRequested()
        traces=account.rpc('debug_traceBlockByHash',[row['block_hash'],OPTIONS])
        if not isinstance(traces,list) or len(traces)!=row['transaction_count']:raise ValueError('Incomplete block trace')
        for receipt in failed:
            item=traces[int(receipt['transactionIndex'],16)]
            if item.get('txHash')!=receipt['transactionHash'] or item.get('error') or 'result' not in item:raise ValueError('Failed transaction trace identity/error')
            entries.append(dict(receipt=receipt,trace=item['result'],provider=provider))
    elif method=='individual':
        for receipt in failed:
            cache=work/str(number)/(receipt['transactionHash']+'.json.gz')
            if cache.exists():
                saved=load(cache)
                if saved['identity']!=identity:raise ValueError('Changed trace checkpoint')
                trace=saved['trace'];origin=saved.get('provider','drpc')
            else:
                if stop_requested():raise DrainRequested()
                trace=account.rpc('debug_traceTransaction',[receipt['transactionHash'],OPTIONS]);origin=provider
                validate_trace(trace)
                if not trace.get('error'):raise ValueError('Failed receipt without root error')
                save(cache,dict(identity=identity,trace=trace,provider=origin),compresslevel=1)
            entries.append(dict(receipt=receipt,trace=trace,provider=origin))
    for entry in entries:
        validate_trace(entry['trace'])
        if not entry['trace'].get('error'):raise ValueError('Failed receipt without root error')
    origins={e['provider'] for e in entries}
    result=dict(schema=2,identity=identity,chain_id=1,receipt_summary=row,strategy=method,
                tracer_options=OPTIONS,provider=next(iter(origins)) if len(origins)==1 else 'mixed' if origins else None,account=account.name if account else None,failed=entries)
    save(target,result,compresslevel=1);return result


def select(result,contracts):
    selected=[]
    for entry in result['failed']:
        matched=sorted(contracts & called_addresses(entry['trace']))
        if matched:selected.append({**entry,'matched_contracts':matched})
    row=result['receipt_summary']
    return dict(schema=2,block_number=row['block_number'],block_hash=row['block_hash'],failed=selected)


def pack(path,records):
    path.parent.mkdir(parents=True,exist_ok=True);temp=path.with_suffix('.tmp')
    with temp.open('wb') as raw:
        with gzip.GzipFile(fileobj=raw,mode='wb',compresslevel=1,mtime=0) as compressed:
            with io.TextIOWrapper(compressed,encoding='utf-8') as stream:
                for row in records:stream.write(json.dumps(row,separators=(',',':'))+'\n')
    with temp.open('rb') as stream:os.fsync(stream.fileno())
    temp.replace(path)


def database(path):
    db=sqlite3.connect(path);db.row_factory=sqlite3.Row
    db.execute('pragma journal_mode=WAL');db.execute('pragma synchronous=FULL')
    db.executescript('''CREATE TABLE IF NOT EXISTS chunks(start INTEGER PRIMARY KEY,stop INTEGER,failed INTEGER,selected INTEGER,block_mode INTEGER,individual_mode INTEGER,skipped INTEGER);
    CREATE TABLE IF NOT EXISTS blocks(number INTEGER PRIMARY KEY,failed INTEGER,selected INTEGER,method TEXT);
    CREATE TABLE IF NOT EXISTS retries(number INTEGER PRIMARY KEY,attempts INTEGER,next_at REAL,error TEXT);
    CREATE TABLE IF NOT EXISTS accounts(name TEXT PRIMARY KEY,next_at REAL DEFAULT 0,disabled INTEGER DEFAULT 0,error TEXT,requests INTEGER DEFAULT 0);''')
    columns={r[1] for r in db.execute('pragma table_info(retries)')}
    if 'preferred_provider' not in columns:
        db.execute("alter table retries add column preferred_provider TEXT DEFAULT 'reth'")
        # Legacy timeout retries have no provider history; try the local node next.
        db.execute("update retries set next_at=min(next_at,?) where error='HTTP 408'",(time.time(),))
    db.execute("""create table if not exists retry_history(
        id INTEGER PRIMARY KEY,number INTEGER,block_hash TEXT,account TEXT,provider TEXT,
        attempt INTEGER,started REAL,finished REAL,outcome TEXT,error TEXT)""")
    db.commit()
    return db


def choose_block(pending,retries,account,enabled_kinds,now):
    for n in pending:
        retry=retries.get(n)
        if retry:
            if retry['next_at']>now:continue
            preferred=retry['preferred_provider']
            if preferred in enabled_kinds and account.kind!=preferred:continue
        return n
    return None


def run(args,stop_requested=lambda:False):
    root=args.output;work=root/'.work';work.mkdir(parents=True,exist_ok=True)
    contracts={x.strip().lower() for x in args.contracts.read_text().splitlines() if x.strip()}
    if len(contracts)!=102:raise ValueError('Expected current 102-contract selection')
    receipt_plan=json.loads((args.receipts/'plan.json').read_text())
    config=dict(receipts=receipt_plan,tracer=OPTIONS,multiplier=1)
    identity=digest(config);selection=digest(sorted(contracts));config['selection']=selection
    if (root/'plan.json').exists() and json.loads((root/'plan.json').read_text())!=config:raise ValueError('Changed run configuration')
    write_json(root/'plan.json',config)
    db=database(root/'tracker.sqlite');accounts=[Account(e) for e in json.loads(args.accounts.read_text())['accounts']]
    if args.reth_workers:
        from web3 import HTTPProvider
        provider=HTTPProvider(args.reth_rpc,request_kwargs={'timeout':(10,150)},exception_retry_configuration=None)
        accounts.extend(Account({'name':f'reth-{i+1:02d}'},provider) for i in range(args.reth_workers))
    for account in accounts:db.execute('insert or ignore into accounts(name) values(?)',(account.name,))
    db.commit();begun=time.monotonic();fresh=0;published=0;state='waiting_receipts';current=None;active={};sealed=0
    def publish(force=False):
        nonlocal published
        if not force and time.monotonic()-published<3:return
        published=time.monotonic()
        totals=db.execute('select coalesce(sum(stop-start),0),coalesce(sum(failed),0),coalesce(sum(selected),0),coalesce(sum(block_mode),0),coalesce(sum(individual_mode),0),coalesce(sum(skipped),0) from chunks').fetchone()
        partial=db.execute('select count(*),coalesce(sum(failed),0),coalesce(sum(selected),0) from blocks').fetchone()
        key_states=[dict(r) for r in db.execute('select * from accounts')]
        errors=[dict(r) for r in db.execute('select * from retries order by next_at limit 5')]
        providers={kind:{'workers':sum(a.kind==kind for a in accounts),'active':sum(a.kind==kind for a,n in active.values())} for kind in ('drpc','reth')}
        progress=dict(providers=providers,state=state,total_blocks=receipt_plan['stop']-receipt_plan['start'],completed_blocks=totals[0]+partial[0],archived_blocks=totals[0],failed_transactions=totals[1]+partial[1],selected_transactions=totals[2]+partial[2],block_mode=totals[3],individual_mode=totals[4],skipped=totals[5],blocks_per_second=fresh/max(.001,time.monotonic()-begun),current_chunk=current,active_requests=len(active),accounts=key_states,errors=errors,updated_at=time.time())
        write_json(root/'progress.json',progress)
    def mark(result):
        nonlocal fresh
        n=result['receipt_summary']['block_number'];chosen=select(result,contracts)
        db.execute('insert or ignore into blocks values(?,?,?,?)',(n,len(result['failed']),len(chosen['failed']),result['strategy']))
        db.execute('delete from retries where number=?',(n,));db.commit();fresh+=1
    batches={};pending={};started={}
    max_open=1 if args.once else getattr(args,'max_open_batches',4)
    def load_batch(info):
        if shutil.disk_usage(root).free<20*1024**3:raise RuntimeError('Trace archive disk free space below 20 GiB')
        source=args.receipts/'chunks'/f"{info['start']}-{info['stop']-1}.jsonl.gz"
        if checksum(source)!=info['sha256']:raise ValueError('Receipt source checksum mismatch')
        with gzip.open(source,'rt') as stream:rows=[json.loads(line) for line in stream]
        if [r['block_number'] for r in rows]!=list(range(info['start'],info['stop'])):raise ValueError('Incomplete receipt batch')
        if any(info['start']<other['stop'] and other['start']<info['stop'] for other,_ in batches.values()):
            raise ValueError('Overlapping receipt batches')
        if db.execute('select 1 from chunks where start<? and stop>?',(info['stop'],info['start'])).fetchone():
            raise ValueError('Receipt batch overlaps completed coverage')
        done={r[0] for r in db.execute('select number from blocks where number>=? and number<?',(info['start'],info['stop']))}
        for row in rows:
            n=row['block_number']
            if n in done:continue
            if (work/f'{n}.json.gz').exists() or strategy(row)=='skip':mark(trace_block(row,None,work,identity))
            else:pending[n]=row
        batches[info['start']]=(info,rows)

    def seal_batch(info,rows):
        name=f"{info['start']}-{info['stop']-1}"
        archive=root/'archive'/(name+'.jsonl.gz');selected=root/'selected/chunks'/(name+'.jsonl.gz')
        def results():
            for row in rows:yield load(work/f"{row['block_number']}.json.gz")
        pack(archive,results());pack(selected,(select(result,contracts) for result in results()))
        bounds=(info['start'],info['stop'])
        counts=db.execute('select coalesce(sum(failed),0),coalesce(sum(selected),0) from blocks where number>=? and number<?',bounds).fetchone()
        methods={r[0]:r[1] for r in db.execute('select method,count(*) from blocks where number>=? and number<? group by method',bounds)}
        metadata=dict(schema=2,total_blocks=receipt_plan['stop']-receipt_plan['start'],start=info['start'],stop=info['stop'],blocks=len(rows),identity=identity,selection=selection,failed_transactions=counts[0],selected_transactions=counts[1],sha256=checksum(selected),archive_sha256=checksum(archive))
        write_json(root/'selected/metadata'/(name+'.json'),metadata)
        db.execute('insert into chunks values(?,?,?,?,?,?,?)',(info['start'],info['stop'],counts[0],counts[1],methods.get('block',0),methods.get('individual',0),methods.get('skip',0)))
        db.execute('delete from blocks where number>=? and number<?',bounds)
        db.execute('delete from retries where number>=? and number<?',bounds);db.commit()
        for row in rows:
            n=row['block_number'];(work/f'{n}.json.gz').unlink(missing_ok=True);shutil.rmtree(work/str(n),ignore_errors=True)

    with ExitStack() as sessions, cf.ThreadPoolExecutor(max_workers=len(accounts)) as pool:
        for account in accounts:
            if getattr(account,'session',None) is not None:sessions.callback(account.session.close)
        while True:
            limited=bool((args.once and sealed) or (args.max_chunks and sealed>=args.max_chunks))
            draining=stop_requested() or limited
            if not draining and len(batches)<max_open:
                accepted=json.loads((args.receipts/'accepted.json').read_text())
                completed={r[0] for r in db.execute('select start from chunks')}
                available=sorted((v for v in accepted.values() if v['start'] not in completed and v['start'] not in batches),key=lambda v:v['start'])
                for info in available[:max_open-len(batches)]:
                    if stop_requested():break
                    load_batch(info)
            current=min(batches,default=None)
            now=time.time();busy={a.name for a,n in active.values()}
            retries={r['number']:r for r in db.execute('select * from retries')}
            keys={r['name']:r for r in db.execute('select * from accounts')}
            enabled_kinds={a.kind for a in accounts if not keys[a.name]['disabled']}
            if not draining and not stop_requested():
                for account in accounts:
                    if stop_requested():break
                    key=keys[account.name]
                    if account.name in busy or key['disabled'] or key['next_at']>now:continue
                    n=choose_block(pending,retries,account,enabled_kinds,now)
                    if n is None:continue
                    row=pending.pop(n);started[n]=time.time()
                    active[pool.submit(trace_block,row,account,work,identity,stop_requested)]=(account,n)
            state='draining' if draining or stop_requested() else 'tracing' if active else 'paused_or_retrying'
            publish()
            if active:
                finished,_=cf.wait(active,timeout=2,return_when=cf.FIRST_COMPLETED)
                for future in finished:
                    account,n=active.pop(future)
                    row=next(r for info,rows in batches.values() if info['start']<=n<info['stop'] for r in rows if r['block_number']==n)
                    old=db.execute('select attempts from retries where number=?',(n,)).fetchone()
                    attempts=(old[0] if old else 0)+1;error_text=None;outcome='success'
                    try:
                        mark(future.result());db.execute('update accounts set error=NULL where name=?',(account.name,))
                    except DrainRequested:
                        outcome='drained';pending[n]=row
                    except Exception as error:
                        outcome='failure';error_text=account.redact(error)
                        delay=min(900,30*2**min(attempts-1,5));disabled=False
                        if isinstance(error,RpcFailure):delay=max(delay,error.delay);disabled=error.disabled
                        preferred='reth' if account.kind=='drpc' else 'drpc'
                        db.execute('insert or replace into retries(number,attempts,next_at,error,preferred_provider) values(?,?,?,?,?)',(n,attempts,time.time()+delay,error_text,preferred))
                        db.execute('update accounts set next_at=?,disabled=?,error=? where name=?',(time.time()+delay,int(disabled),error_text,account.name))
                        pending[n]=row
                    if old or outcome=='failure':
                        db.execute('insert into retry_history(number,block_hash,account,provider,attempt,started,finished,outcome,error) values(?,?,?,?,?,?,?,?,?)',(n,row['block_hash'],account.name,account.kind,attempts,started[n],time.time(),outcome,error_text))
                    started.pop(n)
                    db.execute('update accounts set requests=requests+? where name=?',(account.requests,account.name));account.requests=0;db.commit();publish()
            for batch_start,(info,rows) in list(batches.items()):
                count=db.execute('select count(*) from blocks where number>=? and number<?',(info['start'],info['stop'])).fetchone()[0]
                if count!=len(rows):continue
                seal_batch(info,rows);del batches[batch_start];sealed+=1;publish(True)
                if args.once or (args.max_chunks and sealed>=args.max_chunks):break
            limited=bool((args.once and sealed) or (args.max_chunks and sealed>=args.max_chunks))
            if (stop_requested() or limited) and not active:
                state='stopped' if stop_requested() else 'batch_limit';publish(True);return
            if not batches:
                total=db.execute('select coalesce(sum(stop-start),0) from chunks').fetchone()[0]
                state='complete' if total==receipt_plan['stop']-receipt_plan['start'] else 'waiting_receipts';publish(True)
                if state=='complete' or args.once:return
            if not active and not limited and not stop_requested():time.sleep(2)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--receipts',type=Path,default=Path('/home/dsc-revert/data/receipts/historical-v1'))
    p.add_argument('--output',type=Path,default=Path('/home/dsc-revert/data/archive/reverted/drpc-v1'))
    p.add_argument('--accounts',type=Path,default=Path.home()/'.config/dsc/drpc-accounts.json')
    p.add_argument('--contracts',type=Path,default=Path('/home/dsc-revert/data/contracts-current-102.txt'))
    p.add_argument('--reth-rpc',help='Private RETH HTTP endpoint; replay executes on dsc-node')
    p.add_argument('--reth-workers',type=int,default=0)
    p.add_argument('--max-open-batches',type=int,default=4,help='Bound incomplete receipt batches while allowing later work')
    p.add_argument('--once',action='store_true');p.add_argument('--max-chunks',type=int,default=0)
    args=p.parse_args()
    if not 0<=args.reth_workers<=32 or (args.reth_workers and not args.reth_rpc):p.error('RETH requires an endpoint and 1–32 workers')
    if not 1<=args.max_open_batches<=16:p.error('Open batches must be between 1 and 16')
    shutdown=False
    def request_shutdown(signum,frame):
        global shutdown
        shutdown=True
    signal.signal(signal.SIGTERM,request_shutdown);signal.signal(signal.SIGINT,request_shutdown)
    signal.signal(signal.SIGHUP,request_shutdown)
    args.output.mkdir(parents=True,exist_ok=True)
    with (args.output/'.lock').open('w') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        try:run(args,stop_requested=lambda:shutdown)
        except Exception as error:
            path=args.output/'progress.json'
            previous=json.loads(path.read_text()) if path.exists() else {}
            write_json(path,{**previous,'state':'error','error':type(error).__name__,'updated_at':time.time()})
            raise
