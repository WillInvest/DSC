"""Resumable numerical whole-account HF export, independent of factor readiness.

Reuse accounting_query.Evaluator (original Aave kernels), PyArrow ParquetWriter,
SQLite read-only snapshots, existing own-node comparison and atomic receipts.
No canonical catalog or existing acceptance tracker is modified.
"""
import argparse,bisect,fcntl,json,multiprocessing,os,shutil,sqlite3,sys,time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
sys.path.insert(0,'/home/dsc-data/scripts')
START=16291127
SVB_START,SVB_STOP=16800000,16807201
ZERO='0x0000000000000000000000000000000000000000'
FIELDS=['totalCollateralBase','totalDebtBase','availableBorrowsBase','currentLiquidationThreshold','ltv','healthFactor']
RESERVE_BYTES=50*2**30

class CapacityWait(RuntimeError):pass

def recover_unaccepted_output(directory,receipt):
    """Preserve an interrupted rename/sidecar pair without touching acceptance."""
    directory=Path(directory);output=directory/'accounts.parquet';sidecar=directory/'output.json'
    if output.exists()==sidecar.exists():return
    if Path(receipt).exists():raise ValueError('Accepted numerical output identity is incomplete; recovery requires inspection')
    stamp=str(time.time_ns())
    for path in [output,sidecar]:
        if path.exists():path.rename(directory/('orphan-'+stamp+'-'+path.name))

def load(path):return json.loads(Path(path).read_text())
def readonly(path):
    db=sqlite3.connect('file:'+str(Path(path).resolve())+'?mode=ro',uri=True,timeout=60)
    db.execute('pragma query_only=on');db.execute('begin');return db

def layout(start,stop,chunk=100):
    if not start<stop or chunk<1:raise ValueError('Invalid numeric job bounds')
    points=set(range(start,stop,chunk))|{stop}
    # A one-block first SVB job yields an honest measured row/byte baseline.
    points.update(b for b in [SVB_START,SVB_START+1,SVB_STOP] if start<b<stop)
    ordered=sorted(points)
    jobs=[dict(id=str(a)+'-'+str(b-1),start=a,stop=b) for a,b in zip(ordered,ordered[1:])]
    return sorted(jobs,key=lambda j:(0 if SVB_START<=j['start']<SVB_STOP else 1,j['start']))

def owner_universe(root,stop):
    db=readonly(Path(root)/'position-owners.sqlite')
    try:
        rows=db.execute('select lower(owner),min(first_block) from owners where lower(owner)!=? group by lower(owner) having min(first_block)<? order by min(first_block),lower(owner)',(ZERO,stop)).fetchall()
        return [(str(a),int(b)) for a,b in rows]
    finally:db.close()

def row_count(owners,start,stop):return sum(max(0,stop-max(start,first)) for _,first in owners)

def counts(receipts,scope_start,scope_stop):
    ranges=sorted((r['start'],r['stop']) for r in receipts)
    end=scope_start;through=scope_start-1
    for a,b in ranges:
        if not scope_start<=a<b<=scope_stop or a<end:raise ValueError('Overlapping or out-of-scope numerical coverage')
        if a==through+1:through=b-1
        end=b
    return dict(completed_blocks=sum(b-a for a,b in ranges),contiguous_through_block=through,
                processed_rows=sum(r['rows'] for r in receipts),accepted_bytes=sum(r['bytes'] for r in receipts),
                validated_samples=sum(r['validated_samples'] for r in receipts),completed_jobs=len(receipts))

def ensure_capacity(path,projected=0,reserve=RESERVE_BYTES):
    free=shutil.disk_usage(path).free
    if free-projected<reserve:raise CapacityWait('Numerical HF output needs '+str(projected)+' projected bytes with '+str(reserve)+' bytes reserved; currently '+str(free)+' bytes free. Accepted files preserved; no downsampling.')
    return free

def factor_input(root,plan,generation):
    from shared.artifacts import checksum
    path=Path(root)/'hf-input-progress.json'
    if not path.exists():return None,'Waiting for whole-account factor input report'
    status=load(path)
    if status.get('state')=='failed':return None,'Factor preparation failed: '+str(status.get('error'))
    receipt=status.get('receipt') or status.get('receipt_path')
    identity=status.get('receipt_sha256')
    if not receipt or not identity:return None,'Factor report has not published a checksummed coverage receipt'
    if checksum(receipt)!=identity:raise ValueError('Factor coverage receipt identity differs')
    r=load(receipt)
    if status.get('generation')!=generation or r.get('generation')!=generation:return None,'Waiting for factor inputs validated with current calculation generation'
    if r.get('start')!=START or not START<=r.get('stop',0)<=plan['end_block']+1:raise ValueError('Factor coverage bounds differ')
    if r.get('fixed_cutoff')!=plan['end_block']:raise ValueError('Factor cutoff differs')
    if int(status.get('completed_blocks',0))<r['stop']-START:return None,'Factor coverage publication is still in progress'
    return dict(path=str(receipt),sha256=identity,through=r['stop']-1,generation=generation),None

def check_receipt(path,job,generation,root,owners=None):
    import pyarrow.parquet as pq
    from shared.artifacts import checksum,digest
    r=load(path)
    if any(r.get(k)!=v for k,v in [('generation',generation),('start',job['start']),('stop',job['stop']),('job_id',job['id'])]):raise ValueError('Numerical receipt identity/bounds differ: '+str(path))
    out=Path(r['output']);factor=r['factor_input']
    if not out.is_relative_to(Path(load(Path(root)/'plan.json')['output']).resolve()):raise ValueError('Numerical output escapes owned data output')
    if checksum(out)!=r['sha256'] or out.stat().st_size!=r['bytes'] or pq.read_metadata(out).num_rows!=r['rows']:raise ValueError('Accepted numerical output differs: '+str(out))
    if checksum(factor['path'])!=factor['sha256']:raise ValueError('Accepted numerical factor receipt differs')
    source=load(factor['path'])
    if source.get('generation')!=r['factor_generation'] or source.get('stop',0)<job['stop']:raise ValueError('Numerical receipt factor coverage differs')
    owners=owners if owners is not None else owner_universe(root,job['stop'])
    if digest(owners)!=r['owner_first_seen_sha256'] or row_count(owners,job['start'],job['stop'])!=r['rows']:raise ValueError('Accepted numerical user universe differs')
    if not r.get('all_exact') or r.get('mismatches')!=0:raise ValueError('Numerical node validation was not successful')
    if checksum(r['validation'])!=r['validation_sha256']:raise ValueError('Numerical validation report identity differs')
    return r

def run_job(payload):
    import pyarrow as pa
    import pyarrow.dataset as ds
    import pyarrow.parquet as pq
    from shared.artifacts import checksum,digest,write_json
    from accounting_inputs import node,profiles,profile_at
    from accounting_query import Evaluator
    root=Path(payload['root']);plan=payload['plan'];job=payload['job'];generation=payload['generation'];factor=payload['factor'];directory=Path(payload['directory']);directory.mkdir(parents=True,exist_ok=True)
    started=time.monotonic();owners=owner_universe(root,job['stop']);owner_hash=digest(owners);expected=row_count(owners,job['start'],job['stop']);firsts=[x[1] for x in owners]
    row_group_rows=payload.get('row_group_rows',1000)
    if not isinstance(row_group_rows,int) or not 1000<=row_group_rows<=100000:raise ValueError('Invalid bounded Parquet row group size')
    report_path=directory/'progress.json';output=directory/'accounts.parquet';sidecar=directory/'output.json';temporary=directory/'accounts.parquet.tmp';db=ev=None;rows=0;mismatches=0;samples={};checkpoints={job['start'],(job['start']+job['stop']-1)//2,job['stop']-1};last_heartbeat=0
    def status(state,error=None):
        write_json(report_path,dict(state=state,id=job['id'],start=job['start'],stop=job['stop'],rows=rows,expected_rows=expected,mismatches=mismatches,elapsed_seconds=time.monotonic()-started,updated_at=time.time(),error=error))
    try:
        ensure_capacity(directory)
        if checksum(factor['path'])!=factor['sha256'] or load(factor['path']).get('generation')!=payload['factor_generation']:raise ValueError('Factor input changed before numerical job')
        status('materializing')
        recover_unaccepted_output(directory,payload['receipt'])
        if output.exists():
            if not sidecar.exists():raise ValueError('Existing unaccepted output has no completed input identity')
            saved=load(sidecar)
            if any(saved.get(k)!=v for k,v in [('generation',generation),('owner_first_seen_sha256',owner_hash),('rows',expected),('sha256',checksum(output))]):raise ValueError('Unaccepted numerical output identity differs')
            rows=expected;samples=saved['samples']
        else:
            if temporary.exists():temporary.rename(directory/('interrupted-'+str(time.time_ns())+'.parquet.tmp'))
            db=readonly(root/'accounting.sqlite');ev=Evaluator(root,db,plan)
            if ev.generation!=payload['factor_generation']:raise ValueError('Calculation generation changed during numerical job')
            schema=pa.schema([('user',pa.string()),('block_number',pa.uint32()),('block_hash',pa.string()),('timestamp',pa.uint64()),('profile',pa.string()),('emode',pa.uint8()),('user_configuration',pa.string()),('has_debt',pa.bool_()),*[(field,pa.string()) for field in FIELDS]])
            buffer=[]
            with pq.ParquetWriter(temporary,schema,compression='zstd',use_dictionary=['user','block_hash','profile']) as writer:
                for block in range(job['start'],job['stop']):
                    for owner,_ in owners[:bisect.bisect_right(firsts,block)]:
                        value=ev.account(owner,block,include_balances=False)
                        if len(value['account'])!=6:raise ValueError('Incomplete numerical account result')
                        raw=[str(int(v)) for v in value['account']]
                        if any(int(v)<0 for v in raw):raise ValueError('Negative Aave unsigned account result')
                        row=dict(user=owner,block_number=block,block_hash=value['block_hash'],timestamp=value['timestamp'],profile=value['profile'],emode=value['emode'],user_configuration=value['user_configuration'],has_debt=int(raw[1])>0,**dict(zip(FIELDS,raw)))
                        buffer.append(row);rows+=1
                        if block in checkpoints:
                            kind='debt' if row['has_debt'] else 'no_debt';samples.setdefault(str(block)+':'+kind,dict(user=owner,block=block,profile=value['profile']))
                        if len(buffer)>=row_group_rows:
                            ensure_capacity(directory);writer.write_table(pa.Table.from_pylist(buffer,schema=schema));buffer=[]
                        if time.monotonic()-last_heartbeat>=10:
                            status('materializing');last_heartbeat=time.monotonic()
                    # Completed-factor ranges are immutable; release each block's
                    # read snapshot so a slow job does not pin the WAL indefinitely.
                    db.commit();db.execute('begin')
                if buffer:writer.write_table(pa.Table.from_pylist(buffer,schema=schema))
            if rows!=expected or pq.read_metadata(temporary).num_rows!=expected:raise ValueError('Numerical output does not cover every discovered user at every block')
            with temporary.open('rb') as durable:os.fsync(durable.fileno())
            temporary.replace(output)
            write_json(sidecar,dict(generation=generation,owner_first_seen_sha256=owner_hash,rows=rows,sha256=checksum(output),samples=samples,created_at=time.time()))
        status('validating')
        selected=list(samples.values());profs=profiles()
        tasks=[dict(kind='call',function='getUserAccountData',args=[s['user']],block=s['block'],profile=s['profile']) for s in selected]
        # Empty numerical intervals still bind their two endpoint block identities.
        if not tasks:tasks=[dict(kind='call',function='getReservesList',args=[],block=b,profile=profile_at(profs,b)['key']) for b in sorted({job['start'],job['stop']-1})]
        abis={p['key']:p['abi'] for p in profs if p['key'] in {t['profile'] for t in tasks}}
        refs=node(plan,dict(operation='snapshots',abis=abis,tasks=tasks))
        if len(refs)!=len(tasks):raise ValueError('Node comparison omitted requested samples')
        checks=[];dataset=ds.dataset(str(output),format='parquet') if selected else None
        if db is None:db=readonly(root/'accounting.sqlite')
        for s,reference in zip(selected,refs):
            stored=dataset.to_table(filter=(ds.field('block_number')==s['block'])&(ds.field('user')==s['user'])).to_pylist()
            expected_header=db.execute('select hash from headers where block=?',(s['block'],)).fetchone()
            actual=[str(v) for v in reference['result']];predicted=[stored[0][field] for field in FIELDS] if len(stored)==1 else None
            checks.append(dict(user=s['user'],block=s['block'],profile=s['profile'],stored=predicted,node=actual,stored_block_hash=expected_header[0] if expected_header else None,node_block_hash=reference['block_hash'],exact=predicted==actual and expected_header is not None and reference['block_hash'].lower()==expected_header[0].lower()))
        if not selected:
            for task,reference in zip(tasks,refs):
                header=db.execute('select hash from headers where block=?',(task['block'],)).fetchone();checks.append(dict(block=task['block'],field='empty_interval_endpoint_block_hash',exact=header is not None and reference['block_hash'].lower()==header[0].lower()))
        report=dict(job_id=job['id'],generation=generation,created_at=time.time(),checks=checks,all_exact=all(c['exact'] for c in checks),account_getter_samples=len(selected),empty_interval_endpoint_checks=0 if selected else len(checks),scope='Exact six-field node comparisons of selected produced Parquet rows, not exhaustive numerical correctness')
        mismatches=sum(not c['exact'] for c in checks)
        validation=directory/('validation-'+str(time.time_ns())+'.json');write_json(validation,report)
        if not report['all_exact']:raise ValueError('Numerical full-account node mismatch: '+str(validation))
        receipt=dict(job_id=job['id'],generation=generation,factor_generation=payload['factor_generation'],start=job['start'],stop=job['stop'],output=str(output.resolve()),sha256=checksum(output),bytes=output.stat().st_size,rows=rows,expected_rows=expected,owner_count=len(owners),owner_first_seen_sha256=owner_hash,owner_source=str(root/'position-owners.sqlite'),factor_input=factor,validation=str(validation.resolve()),validation_sha256=checksum(validation),validated_samples=len(selected),empty_interval_endpoint_checks=0 if selected else len(checks),mismatches=0,all_exact=True,elapsed_seconds=time.monotonic()-started,created_at=time.time(),representation='Numerical all-user end-of-block six-field account rows; zero-address excluded; no-debt HF uint256 maximum preserved')
        destination=Path(payload['receipt']);write_json(destination,receipt);status('accepted');return receipt
    except Exception as exc:status('awaiting_user_decision' if isinstance(exc,CapacityWait) else 'failed',type(exc).__name__+': '+str(exc));raise
    finally:
        if ev is not None:ev.close()
        if db is not None:db.close()

def run(workers=32,once=False):
    from shared.artifacts import checksum,digest,write_json
    from accounting_inputs import ROOT
    from accounting_query import Evaluator
    current=load(ROOT/'current.json');root=Path(current['root']);plan=load(current['plan']);jobs=layout(START,plan['end_block']+1);started=time.monotonic();accepted={};generation=None;factor_generation=None;directory=None;receipt_directory=None;base_rows=0
    def status(state,waiting=None,error=None,active=None):
        c=counts(list(accepted.values()),START,plan['end_block']+1);elapsed=max(0.001,time.monotonic()-started);fresh=max(0,c['processed_rows']-base_rows);active=active or []
        live=[]
        for j in active:
            p=directory/'jobs'/j['id']/'progress.json'
            if p.exists():live.append(load(p))
        progress=dict(state=state,run_id=plan['run_id'],generation=generation,factor_generation=factor_generation,start_block=START,end_block=plan['end_block'],total_blocks=plan['end_block']-START+1,total_jobs=len(jobs),**c,numeric_rows_materialized=c['processed_rows'],inflight_rows=sum(x.get('rows',0) for x in live),mismatches=sum(x.get('mismatches',0) for x in live),updated_at=time.time(),error=error,waiting_for=waiting or [],current={'jobs':live,'workers':workers},representation='Numerical whole-account HF rows; all users seen by each block, including no-debt accounts',elapsed_seconds=elapsed,rows_per_second=fresh/elapsed if fresh else None,full_eta_seconds=None,free_bytes=shutil.disk_usage(plan['output']).free,reserved_free_bytes=RESERVE_BYTES)
        write_json(root/'hf-progress.json',progress)
    with (root/'hf-numeric.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        try:
            while True:
                if not (root/'accounting.sqlite').exists():
                    status('waiting_for_inputs',['Factorized accounting database not yet prepared'])
                    if once:return
                    time.sleep(30);continue
                if factor_generation is None:
                    db=readonly(root/'accounting.sqlite');ev=None
                    try:ev=Evaluator(root,db,plan);factor_generation=ev.generation
                    finally:
                        if ev is not None:ev.close()
                        db.close()
                    generation=digest(dict(calculation=factor_generation,materializer=checksum(__file__),plan=checksum(current['plan']),chunk_size=100,priority=[SVB_START,SVB_STOP],all_users=True))
                    directory=Path(plan['output'])/'accounting/hf-numeric'/generation;(directory/'jobs').mkdir(parents=True,exist_ok=True);receipt_directory=root/'hf-numeric'/generation/'receipts';receipt_directory.mkdir(parents=True,exist_ok=True)
                    jobmap={j['id']:j for j in jobs}
                    for path in sorted(receipt_directory.glob('*.json')):
                        if path.stem not in jobmap:raise ValueError('Unrecognized accepted numerical interval '+str(path))
                        accepted[path.stem]=check_receipt(path,jobmap[path.stem],generation,root)
                    base_rows=sum(r['rows'] for r in accepted.values())
                factor,waiting=factor_input(root,plan,factor_generation)
                if factor is None:
                    status('waiting_for_inputs',[waiting])
                    if once:return
                    time.sleep(30);continue
                ready=[j for j in jobs if j['id'] not in accepted and j['stop']<=factor['through']+1]
                if not ready:
                    done=len(accepted)==len(jobs);status('complete' if done else 'waiting_for_inputs',[] if done else ['Waiting for additional whole-account factor coverage'])
                    if done or once:return
                    time.sleep(30);continue
                # Calibrate one real job before projecting disk for a parallel batch.
                batch=ready[:workers if any(r['rows'] for r in accepted.values()) else 1];owners=owner_universe(root,max(j['stop'] for j in batch))
                ratios=[r['bytes']/r['rows'] for r in list(accepted.values())[-8:] if r['rows']]
                projected=sum(max(8192,int(row_count(owners,j['start'],j['stop'])*max(ratios))) for j in batch) if ratios else 0
                ensure_capacity(plan['output'],projected)
                status('materializing',active=batch)
                payloads=[dict(root=str(root),plan=plan,job=j,generation=generation,factor_generation=factor_generation,factor=factor,directory=str(directory/'jobs'/j['id']),receipt=str(receipt_directory/(j['id']+'.json'))) for j in batch]
                with ProcessPoolExecutor(max_workers=min(workers,len(batch)),mp_context=multiprocessing.get_context('spawn')) as pool:
                    pending=[(j,pool.submit(run_job,payload)) for j,payload in zip(batch,payloads)]
                    while pending:
                        for j,future in pending[:]:
                            if future.done():accepted[j['id']]=future.result();pending.remove((j,future))
                        status('materializing' if pending else 'validating',active=[j for j,_ in pending])
                        if pending:time.sleep(5)
                if once:status('running');return
        except CapacityWait as exc:status('awaiting_user_decision',[str(exc)],error=str(exc),active=batch if 'batch' in locals() else None)
        except Exception as exc:status('failed',error=type(exc).__name__+': '+str(exc),active=batch if 'batch' in locals() else None);raise

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--workers',type=int,default=32);parser.add_argument('--once',action='store_true');args=parser.parse_args()
    if not 1<=args.workers<=32:parser.error('--workers must be between 1 and 32')
    run(args.workers,args.once)
