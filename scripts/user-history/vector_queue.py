"""Data-owned HF ledger and queue; Dask moves vector buffers directly to node.

Node is the declared bulk output location. Only compact acceptance receipts pass
through Data; the application database and other pipeline roles are unchanged.
"""
import argparse
import fcntl
import os
import sqlite3
import time
from pathlib import Path
from distributed import Client
import pyarrow as pa
import pyarrow.parquet as pq
from shared.artifacts import checksum, digest, write_json
import materialize_accounts as m
from vector_compute import task, universe
from vector_coordinator import security
from vector_tasks import accept_remote, inspect_receipts, storage_status


def validate_plan(plan):
    if digest(plan['identity']) != plan['generation']:
        raise ValueError('Calculation identity differs')
    scope = sorted(plan['coverage'], key=lambda j: j['start'])
    if not scope or scope[0]['start'] != 16291127 or scope[-1]['stop'] != 26407882:
        raise ValueError('Wrong finalized coverage')
    if any(a['stop'] != b['start'] for a, b in zip(scope, scope[1:])):
        raise ValueError('Interval overlap/gap')
    pending = {j['id'] for j in plan['jobs']}
    if pending & plan['recovered'].keys() or pending | plan['recovered'].keys() != {j['id'] for j in scope}:
        raise ValueError('Incomplete unique ownership')
    rows = sum(j['expected_rows'] for j in plan['jobs']) + sum(v['receipt']['rows'] for v in plan['recovered'].values())
    if rows != 910310934885 or rows != plan['planned_rows']:
        raise ValueError('Changed all-user population')


def run(path, base):
    path=Path(path);base=Path(base);plan=m.load(path);validate_plan(plan)
    for name, sha in plan['controller_sources'].items():
        if checksum(Path(__file__).with_name(name)) != sha:
            raise ValueError('Controller code identity differs: '+name)
    gen=plan['generation'];manifest_sha=checksum(path);roles=list(plan['workers'])
    lock=(base/'queue.lock').open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    db=sqlite3.connect(base/'tracker.sqlite');db.execute('pragma journal_mode=WAL');db.execute('pragma synchronous=FULL')
    db.execute('create table if not exists accepted(generation text,job_id text,receipt text,sha256 text,primary key(generation,job_id))');db.commit()
    public=base/'public';public.mkdir(exist_ok=True);receipts=base/'receipts'/gen;receipts.mkdir(parents=True,exist_ok=True)
    jobs={j['id']:j for j in plan['jobs']};done={};active={};info={};processing={};storage={};started=time.monotonic()
    recovered={role:[v['receipt'] for v in plan['recovered'].values() if v['owner']==role] for role in roles}
    def commit(value):
        r=value['receipt'];key=r['job_id']
        if key not in jobs or r['job']!=jobs[key] or r['generation']!=gen or not r['all_exact'] or r['mismatches']:
            raise ValueError('Remote acceptance scope differs')
        p=receipts/(key+'.json')
        if p.exists():
            if checksum(p)!=value['sha256']:raise ValueError('Preserved Data receipt differs')
        else:write_json(p,r)
        if checksum(p)!=value['sha256']:raise ValueError('Data/node receipt differs')
        db.execute('insert or ignore into accepted values(?,?,?,?)',(gen,key,str(p),value['sha256']));db.commit()
        done[key]={k:r[k] for k in ['job_id','role','rows','bytes','start','stop','validated_samples']}
    def publish(state,error=None):
        for role in roles:
            new=[r for r in done.values() if r['role']==role];combined=recovered[role]+new
            addresses=[a for a,w in info.items() if str(w['name']).startswith('vector-'+role)]
            value=dict(role=role,generation=gen,manifest_sha256=manifest_sha,factor_generation=plan['identity']['factor_generation'],state=state,error=error,updated_at=time.time(),epoch='20261116-data-vector',run_id='26407881',start_block=16291127,end_block=26407881,total_blocks=10116755,total_jobs=len(plan['coverage']),processed_rows=sum(r['rows'] for r in combined),new_rows=sum(r['rows'] for r in new),recovered_rows=sum(r['rows'] for r in recovered[role]),completed_blocks=sum(r['stop']-r['start'] for r in combined),completed_jobs=len(combined),validated_samples=sum(r['validated_samples'] for r in combined),mismatches=0,inflight_rows=0,inflight_expected_rows=sum(j['expected_rows'] for j,_ in active.values()),workers_configured=plan['workers'][role],active_compute_processes=sum(len(processing.get(a,[])) for a in addresses),free_bytes=storage.get('free_bytes'),reserved_free_bytes=m.RESERVE_BYTES,storage_role='node',coordinator_role='data',elapsed_seconds=time.monotonic()-started,current=dict(jobs=[dict(id=j['id'],start=j['start'],stop=j['stop'],state='compute_transfer_or_validate') for j,_ in active.values()]),representation='Data owns acceptance ledger; four-host exact HF-only computation; node stores validated vectors; recovered full-row outputs and complete factors retained')
            write_json(public/(role+'.json'),value)
    client=Client(plan['scheduler'],security=security(plan['private']),set_as_default=False)
    try:
        publish('reconciling')
        client.wait_for_workers(sum(plan['workers'].values())+plan['validation_workers'],timeout=240)
        # Scope and SHA checks precede every recovery registration.
        retained=[]
        for key,p,sha in db.execute('select job_id,receipt,sha256 from accepted where generation=?',(gen,)):
            if key not in jobs or checksum(p)!=sha:raise ValueError('Data catalog receipt changed')
            retained.append((key,sha))
        restored=client.submit(inspect_receipts,list(jobs.values()),plan['storage_base'],gen,retained,pure=False,resources={'vector_accept':1}).result()
        for value in restored:commit(value)
        del restored
        owners=universe()
        if digest(owners)!=plan['identity']['registry_digest']:raise ValueError('User dictionary differs')
        registry=base/'users.parquet';table=pa.table({'user_id':pa.array(range(len(owners)),type=pa.uint32()),'user':[a for a,_ in owners],'first_offset':pa.array([b-16291127 for _,b in owners],type=pa.uint32())})
        if not registry.exists():
            temp=registry.with_suffix('.incoming');pq.write_table(table,temp,compression='zstd',compression_level=9)
            with temp.open('rb') as h:os.fsync(h.fileno())
            temp.replace(registry)
        if not pq.read_table(registry).equals(table):raise ValueError('Preserved user dictionary differs')
        write_json(base/'dataset.json',dict(generation=gen,identity=plan['identity'],registry=str(registry),registry_sha256=checksum(registry),registry_rows=len(owners),storage_base=plan['storage_base'],coordinator='data',block_origin=16291127,end_block=26407881,raw_hf_scale=str(10**18),other_quantities='Reconstruct from retained complete historical factors',recovered_formats='Original receipts preserved in manifest; project healthFactor from original14-column outputs'))
        priority=plan.get('first_jobs',{})
        queue=iter(sorted((j for j in jobs.values() if j['id'] not in done),key=lambda j:(j['id'] not in priority,j['start'])))
        exhausted=False;last_storage=0
        while True:
            info=client.scheduler_info(n_workers=-1)['workers'];processing=client.processing()
            for accepted in list(active):
                if accepted.done():
                    job,computed=active.pop(accepted)
                    commit(accepted.result());computed.release();accepted.release()
            if time.monotonic()-last_storage>30:
                storage=client.submit(storage_status,plan['storage_base'],pure=False,resources={'vector_accept':1},priority=100).result();last_storage=time.monotonic()
            while not exhausted and len(active)<plan['max_inflight']:
                job=next(queue,None)
                if job is None:exhausted=True;break
                options={}
                if job['id'] in priority:
                    role=priority[job['id']];options['workers']=[a for a,w in info.items() if str(w['name']).startswith('vector-'+role)]
                    if not options['workers']:raise ValueError('Required initial compute host unavailable: '+role)
                future=client.submit(task,dict(job=job,generation=gen,identity=plan['identity']),pure=False,resources={'vector_compute':1},retries=1,**options)
                directory=plan['storage_base']+'/outputs/'+gen+'/jobs/'+job['id']
                accepted=client.submit(accept_remote,future,directory,pure=False,resources={'vector_accept':1},retries=0)
                active[accepted]=(job,future)
            if exhausted and not active:publish('complete');break
            publish('materializing' if info else 'waiting_for_inputs');time.sleep(5)
    except Exception as exc:
        error=type(exc).__name__+': '+str(exc);publish('awaiting_user_decision',error)
        write_json(base/('failure-'+str(time.time_ns())+'.json'),dict(error=error,active=[j['id'] for j,_ in active.values()],created_at=time.time()))
        # Only this client's new-format tasks are cancelled; no legacy worker is stopped.
        client.cancel(list(active)+[f for _,f in active.values()]);raise
    finally:client.close();db.close();lock.close()


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--manifest',required=True);p.add_argument('--base',required=True);a=p.parse_args();run(a.manifest,a.base)
