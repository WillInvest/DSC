"""Data-owned joint acceptance ledger for user/reserve/protocol float64 vectors."""
import argparse,fcntl,sqlite3,time,asyncio
from pathlib import Path
from distributed import Client
from shared.artifacts import checksum,digest,write_json
from vector_coordinator import security
from hf_f64_compute import task
from hf_f64_tasks import accept_remote,inspect,capacity
from hf_f64_contract import CONTRACT,LOGICAL_VALUES,ORIGIN,STOP,source_assignments
import materialize_accounts as m

def wait_result(future,paused,pulse,timeout=120,clock=time.monotonic):
    deadline=clock()+timeout
    while True:
        if paused():raise InterruptedError('Paused by user')
        remaining=deadline-clock()
        if remaining<=0:raise TimeoutError('Remote resource/result deadline exceeded')
        try:return future.result(timeout=min(5,remaining))
        except (TimeoutError,asyncio.TimeoutError):pulse()

def validate(plan):
    if digest(plan['identity'])!=plan['generation'] or plan['identity']['contract']!=CONTRACT:raise ValueError('Manifest generation differs')
    for field in ['jobs','sources','source_roles']:
        if digest(plan[field])!=plan['identity'][field+'_sha256']:raise ValueError('Unbound job/source assignment: '+field)
    if digest(plan.get('checkpoints',[]))!=plan['identity'].get('checkpoints_sha256',digest([])):raise ValueError('Checkpoint assignment differs')
    if checksum(plan['source_manifest'])!=plan['identity']['source_manifest_sha256']:raise ValueError('Frozen reconciliation manifest changed')
    expected=source_assignments(m.load(plan['source_manifest']))
    if any(plan[field]!=value for field,value in zip(['jobs','sources','source_roles'],expected)):raise ValueError('Task sources differ from frozen reconciliation evidence')
    jobs=sorted(plan['jobs'],key=lambda j:j['start'])
    if not jobs or jobs[0]['start']!=ORIGIN or jobs[-1]['stop']!=STOP or any(a['stop']!=b['start'] for a,b in zip(jobs,jobs[1:])):raise ValueError('Coverage gap/overlap')
    if len({j['id'] for j in jobs})!=len(jobs) or sum(j['expected_rows'] for j in jobs)!=LOGICAL_VALUES:raise ValueError('Wrong population')
    if not 0<plan['max_inflight']<=sum(plan['workers'].values()) or plan['max_inflight_bytes']<=0:raise ValueError('Unbounded queue')

def run(path,base,limit=0):
    path=Path(path);base=Path(base);plan=m.load(path);validate(plan);base.mkdir(parents=True,exist_ok=True)
    for name,sha in plan['identity']['sources'].items():
        if checksum(Path(__file__).with_name(name))!=sha:raise ValueError('Controller code differs: '+name)
    lock=(base/'queue.lock').open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    gen=plan['generation'];jobs={j['id']:j for j in plan['jobs']};done={};active={};submitted={};started=time.monotonic();storage={};info={};processing={}
    db=sqlite3.connect(base/'tracker.sqlite');db.execute('pragma journal_mode=WAL');db.execute('pragma synchronous=FULL')
    db.execute('create table if not exists accepted(generation text,job_id text,receipt text,sha256 text,primary key(generation,job_id))');db.commit()
    public=base/'public';public.mkdir(exist_ok=True);receipts=base/'receipts'/gen;receipts.mkdir(parents=True,exist_ok=True)
    def commit(value):
        r=value['receipt'];key=r['job_id']
        if key not in jobs or r['job']!=jobs[key] or r['generation']!=gen or not r['within_error_budget'] or not r['structural_exact'] or r['rows']!=jobs[key]['expected_rows']:raise ValueError('Acceptance differs')
        source=plan['sources'].get(key)
        if r.get('retained_source_sha256')!=(digest(source) if source else None) or r['source_kind']!=('integer_cast' if source else 'float64_batch'):raise ValueError('Accepted HF provenance differs from assigned source')
        p=receipts/(key+'.json')
        if p.exists():
            if checksum(p)!=value['sha256']:raise ValueError('Preserved Data receipt differs')
        else:write_json(p,r)
        if checksum(p)!=value['sha256']:raise ValueError('Data/node receipt differs')
        old=db.execute('select sha256 from accepted where generation=? and job_id=?',(gen,key)).fetchone()
        if old and old[0]!=value['sha256']:raise ValueError('Duplicate job acceptance conflicts')
        db.execute('insert or ignore into accepted values(?,?,?,?)',(gen,key,str(p),value['sha256']));db.commit()
        done[key]={k:r[k] for k in ['job_id','role','rows','bytes','start','stop','validated_samples','timing','source_kind']}
    def publish(state,error=None):
        continuous=ORIGIN
        for j in sorted(jobs.values(),key=lambda j:j['start']):
            if j['start']!=continuous or j['id'] not in done:break
            continuous=j['stop']
        elapsed=time.monotonic()-started;new_values=sum(v['rows'] for v in done.values())-initial_values
        rate=new_values/elapsed if elapsed>30 and new_values else None
        live=[dict(id=j['id'],start=j['start'],stop=j['stop'],stage='compute_transfer_validate',reserved_bytes=reserved) for j,_,reserved in active.values()]
        for role in plan['workers']:
            own=[v for v in done.values() if v['role']==role];values=sum(v['rows'] for v in own)
            v=dict(role=role,display_role='dsc-compute' if role=='web' else 'dsc-'+role,generation=gen,manifest_sha256=checksum(path),factor_generation=CONTRACT['factor_generation'],epoch='20261116-float64-v2',run_id='26407881',state=state,error=error,updated_at=time.time(),start_block=ORIGIN,end_block=STOP-1,total_blocks=STOP-ORIGIN,total_jobs=len(jobs),processed_rows=values,new_rows=values,recovered_rows=0,completed_jobs=len(own),completed_blocks=sum(x['stop']-x['start'] for x in own),user_values_accepted=values,reserve_blocks_accepted=sum(x['stop']-x['start'] for x in own),protocol_blocks_accepted=sum(x['stop']-x['start'] for x in own),continuous_end_block=continuous-1,highest_accepted_block=max((x['stop']-1 for x in done.values()),default=ORIGIN-1),gap_jobs=sum(j['id'] not in done for j in jobs.values()),validated_samples=sum(x['validated_samples'] for x in own),mismatches=0,inflight_expected_rows=sum(j['expected_rows'] for j,_,_ in active.values()),inflight_reserved_bytes=sum(x[2] for x in active.values()),workers_configured=plan['workers'][role],active_compute_processes=sum(len(processing.get(a,[])) for a,w in info.items() if str(w['name']).startswith('f64-'+role)),free_bytes=storage.get('free_bytes'),reserved_free_bytes=m.RESERVE_BYTES,elapsed_seconds=elapsed,current=dict(jobs=live),storage_role='node',coordinator_role='data',bytes=sum(x['bytes'] for x in own),rate_user_values_per_second=rate,conditional_eta_seconds=(LOGICAL_VALUES-sum(x['rows'] for x in done.values()))/rate if rate else None,cloud_state='not_uploaded',validation_kind='float64_model',representation='Joint float64 user HF vectors and debt-weighted reserve/protocol research indicators; original exact sources retained')
            write_json(public/(role+'.json'),v)
    initial_values=0
    client=Client(plan['scheduler'],security=security(plan['private']),set_as_default=False)
    try:
        publish('reconciling');client.wait_for_workers(sum(plan['workers'].values())+1,timeout=240)
        retained=list(db.execute('select job_id,sha256 from accepted where generation=?',(gen,)))
        inspection=client.submit(inspect,list(jobs.values()),plan['storage_base'],gen,retained,pure=False,resources={'f64_accept':1})
        for value in wait_result(inspection,lambda:(base/'PAUSED_BY_USER').exists(),lambda:publish('reconciling'),timeout=1800):commit(value)
        initial_values=sum(x['rows'] for x in done.values());priority=plan['first_jobs'];ordered=sorted((j for j in jobs.values() if j['id'] not in done),key=lambda j:(j['id'] not in priority,j['start']))
        if limit:ordered=ordered[:limit]
        queue=iter(ordered);pending=next(queue,None);last_storage=0
        while True:
            if (base/'PAUSED_BY_USER').exists():publish('paused_by_user');break
            info=client.scheduler_info(n_workers=-1)['workers'];processing=client.processing()
            for accepted in list(active):
                if accepted.done():
                    job,future,_=active[accepted];commit(wait_result(accepted,lambda:(base/'PAUSED_BY_USER').exists(),lambda:publish('validating'),timeout=60));del active[accepted];submitted.pop(accepted);future.release();accepted.release()
                elif time.monotonic()-submitted[accepted]>plan.get('max_job_seconds',1200):raise TimeoutError('Compute/transfer/acceptance deadline exceeded: '+active[accepted][0]['id'])
            if time.monotonic()-last_storage>30:
                request=client.submit(capacity,plan['storage_base'],pure=False,resources={'f64_accept':1},priority=100)
                storage=wait_result(request,lambda:(base/'PAUSED_BY_USER').exists(),lambda:publish('waiting_for_inputs'),timeout=60);last_storage=time.monotonic()
            while pending and len(active)<plan['max_inflight']:
                estimate=max(1_000_000,pending['expected_rows']*8)
                if estimate>plan['max_inflight_bytes']:raise ValueError('Single job exceeds declared byte budget')
                if estimate+sum(v[2] for v in active.values())>plan['max_inflight_bytes']:break
                role=plan['source_roles'].get(pending['id'],priority.get(pending['id']))
                opts={}
                if role:
                    opts['workers']=[a for a,w in info.items() if str(w['name']).startswith('f64-'+role)]
                    if not opts['workers']:raise ValueError('Required source host unavailable: '+role)
                payload=dict(job=pending,generation=gen,identity=plan['identity'],checkpoints=plan.get('checkpoints',[]))
                if pending['id'] in plan['sources']:payload['hf_source']=plan['sources'][pending['id']]
                future=client.submit(task,payload,pure=False,resources={'f64_compute':1},retries=1,**opts)
                directory=plan['storage_base']+'/outputs/'+gen+'/jobs/'+pending['id'];accepted=client.submit(accept_remote,future,directory,pure=False,resources={'f64_accept':1},retries=0)
                active[accepted]=(pending,future,estimate);submitted[accepted]=time.monotonic();pending=next(queue,None)
            if pending is None and not active:publish('complete' if len(done)==len(jobs) else 'canary_complete');break
            publish('materializing');time.sleep(5)
    except InterruptedError:publish('paused_by_user')
    except Exception as exc:
        publish('awaiting_user_decision',type(exc).__name__+': '+str(exc));write_json(base/('failure-'+str(time.time_ns())+'.json'),dict(error=str(exc),active=[j['id'] for j,_,_ in active.values()],created_at=time.time()));raise
    finally:
        client.cancel(list(active)+[v[1] for v in active.values()]);client.close();db.close();lock.close()

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--manifest',required=True);p.add_argument('--base',required=True);p.add_argument('--limit',type=int,default=0);a=p.parse_args();run(a.manifest,a.base,a.limit)
