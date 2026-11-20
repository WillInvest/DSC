"""Temporary node-owned HF vector queue while data is offline; app DB unchanged."""
import argparse,fcntl,json,socket,sqlite3,time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from distributed import Client,Security
from shared.artifacts import checksum,digest,write_json
import materialize_accounts as m
from vector_compute import task,universe
from vector_accept import accept,check_receipt

def security(private):
    p=Path(private)
    return Security(require_encryption=True,tls_ca_file=str(p/'ca.pem'),tls_client_cert=str(p/'host-cert.pem'),tls_client_key=str(p/'host-key.pem'))

def data_online():
    try:
        with socket.create_connection(('10.246.103.151',22),timeout=1):return True
    except OSError:return False

def retained_check(path,job,generation):
    """Previously committed value validation is retained, not claimed fresh."""
    r=m.load(path)
    if r['generation']!=generation or r['job']!=job or r['rows']!=job['expected_rows'] or not r['all_exact']:raise ValueError('Committed receipt scope changed')
    if checksum(r['validation'])!=r['validation_sha256'] or not m.load(r['validation'])['all_exact']:raise ValueError('Retained node report differs')
    if sum(Path(p['path']).stat().st_size for p in r['parts'])!=r['bytes']:raise ValueError('Retained part files absent/size differs')
    return r

def run(manifest_path,base):
    manifest_path=Path(manifest_path);base=Path(base);plan=m.load(manifest_path);gen=plan['generation'];manifest_sha=checksum(manifest_path)
    if digest(plan['identity'])!=gen:raise ValueError('Manifest generation differs')
    for name,value in plan['controller_sources'].items():
        if checksum(Path(__file__).with_name(name))!=value:raise ValueError('Controller source differs')
    lock=(base/'coordinator.lock').open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    db=sqlite3.connect(base/'tracker.sqlite');db.execute('pragma journal_mode=WAL');db.execute('pragma synchronous=FULL')
    db.execute('create table if not exists accepted(generation text,job_id text,receipt text,sha256 text,primary key(generation,job_id))');db.commit()
    jobs={j['id']:j for j in plan['jobs']};done={}
    def compact(r):return {k:r[k] for k in ['job_id','role','rows','bytes','start','stop','validated_samples']}
    for key,path,sha in db.execute('select job_id,receipt,sha256 from accepted where generation=?',(gen,)):
        if key not in jobs or checksum(path)!=sha:raise ValueError('Accepted catalog/receipt conflict')
        done[key]=compact(retained_check(path,jobs[key],gen))
    # Only reconcile uncommitted receipts under this declared generation/job set.
    out=base/'outputs'/gen/'jobs';out.mkdir(parents=True,exist_ok=True)
    for folder in out.iterdir():
        if folder.name in jobs and folder.name not in done and (folder/'receipt.json').exists():
            path=folder/'receipt.json';r=check_receipt(path,jobs[folder.name],gen)
            db.execute('insert into accepted values(?,?,?,?)',(gen,folder.name,str(path),checksum(path)));db.commit();done[folder.name]=compact(r)
    public=base/'public';public.mkdir(exist_ok=True);start=time.monotonic();active={};verifying={};failure=None;online={};processing={}
    recovered={role:[v['receipt'] for v in plan['recovered'].values() if v['owner']==role] for role in ['node','revert','web']}
    queue=iter(sorted((j for j in plan['jobs'] if j['id'] not in done),key=lambda j:j['start']));finished=False
    def publish(state,error=None):
        for role in recovered:
            reused=recovered[role];new=[r for r in done.values() if r['role']==role];combined=reused+new
            live=[dict(id=j['id'],start=j['start'],stop=j['stop'],expected_rows=j['expected_rows'],state='compute_or_transfer') for j in active.values()]
            value=dict(role=role,state=state,error=error,updated_at=time.time(),generation=gen,manifest_sha256=manifest_sha,factor_generation=plan['identity']['factor_generation'],run_id='26407881',epoch='20261116-vector-central',start_block=16291127,end_block=26407881,total_blocks=10116755,processed_rows=sum(r['rows'] for r in combined),new_rows=sum(r['rows'] for r in new),recovered_rows=sum(r['rows'] for r in reused),completed_jobs=len(combined),completed_blocks=sum(r['stop']-r['start'] for r in combined),total_jobs=len(plan['jobs'])+len(plan['recovered']),validated_samples=sum(r['validated_samples'] for r in combined),mismatches=0,inflight_rows=0,inflight_rows_note='Remote intermediate HF counts not persisted; pending expected counts are separate',inflight_expected_rows=sum(j['expected_rows'] for j in active.values()),active_compute_processes=sum(len(processing.get(w,[])) for w in online.get(role,[])),workers_configured=plan['workers'][role],current=dict(jobs=live,validating_jobs=len(verifying)),free_bytes=m.shutil.disk_usage(base).free,reserved_free_bytes=m.RESERVE_BYTES,elapsed_seconds=time.monotonic()-start,storage_role='node',representation='TLS workers compute exact HF in memory; node owns vector acceptance and disk storage; recovered full-row outputs retain original identities. Other account values remain reconstructible from full inputs.')
            write_json(public/(role+'.json'),value)
    publish('starting')
    client=Client(plan['scheduler'],security=security(plan['private']),set_as_default=False)
    # Prime one shared immutable dictionary before parallel validation threads.
    owners=universe()
    import pyarrow as pa
    import pyarrow.parquet as pq
    registry_dir=base/'registry';registry_dir.mkdir(exist_ok=True);registry_file=registry_dir/(plan['identity']['registry_digest']+'.parquet')
    registry_table=pa.table({'user_id':pa.array(range(len(owners)),type=pa.uint32()),'user':[x[0] for x in owners],'first_offset':pa.array([x[1]-16291127 for x in owners],type=pa.uint32())})
    if not registry_file.exists():
        temporary=registry_file.with_suffix('.incoming');pq.write_table(registry_table,temporary,compression='zstd',compression_level=9,row_group_size=100000)
        with temporary.open('rb') as handle:__import__('os').fsync(handle.fileno())
        temporary.replace(registry_file)
    if not pq.read_table(registry_file).equals(registry_table):raise ValueError('Published registry differs')
    write_json(base/('dataset-'+gen+'.json'),dict(format='hf-vector-v1',identity=plan['identity'],registry=str(registry_file),registry_sha256=checksum(registry_file),registry_rows=len(owners),block_origin=16291127,end_block=26407881,raw_hf_scale=str(10**18),entity_namespace='user',other_quantities='Reconstruct from retained complete historical factors',physical_row_count_note='Segments; logical HF count is sum of list lengths'))
    pool=ThreadPoolExecutor(max_workers=plan['validation_workers'])
    try:
        while True:
            if data_online():
                failure='data LAN SSH restored; own vector queue paused for authorized reconciliation';publish('awaiting_user_decision',failure);client.shutdown();break
            info=client.scheduler_info()['workers'];online={role:[a for a,w in info.items() if str(w['name']).startswith('vector-'+role)] for role in recovered};processing=client.processing()
            for future in list(verifying):
                if future.done():
                    job=verifying.pop(future)
                    r=future.result();path=out/job['id']/'receipt.json'
                    db.execute('insert into accepted values(?,?,?,?)',(gen,job['id'],str(path),checksum(path)));db.commit();done[job['id']]=compact(r)
            for future in list(active):
                if future.done() and len(verifying)<plan['validation_workers']:
                    job=active.pop(future);result=future.result()
                    verifying[pool.submit(accept,result,out/job['id'])]=job;future.release()
            slots=min(plan['max_inflight'],len(info))
            while not finished and len(active)+len(verifying)<slots:
                m.ensure_capacity(base)
                job=next(queue,None)
                if job is None:finished=True;break
                payload=dict(job=job,generation=gen,identity=plan['identity'])
                future=client.submit(task,payload,pure=False,resources={'vector_compute':1},retries=1);active[future]=job
            if finished and not active and not verifying:publish('complete');break
            publish('materializing' if info else 'waiting_for_inputs');time.sleep(5)
    except Exception as e:
        failure=type(e).__name__+': '+str(e);publish('awaiting_user_decision',failure)
        write_json(base/('failure-'+str(time.time_ns())+'.json'),dict(error=failure,time=time.time(),generation=gen,active_jobs=[j['id'] for j in active.values()],validation_jobs=[j['id'] for j in verifying.values()]))
        try:client.shutdown()
        except Exception:pass
        raise
    finally:
        pool.shutdown(wait=True,cancel_futures=True);client.close();db.close();lock.close()

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--manifest',required=True);p.add_argument('--base',required=True);a=p.parse_args();run(a.manifest,a.base)
