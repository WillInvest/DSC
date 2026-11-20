"""Float64 adapter for the existing rsync/rclone verified archive pipeline."""
import argparse,json,sqlite3,time,tempfile
from pathlib import Path
from distributed import Client
from shared.artifacts import checksum,digest,write_json
from vector_coordinator import security
from vector_archive import publish_source,close_source,stage_upload,registry_source,resolve_sizes,LIMIT
from distributed_hf import backup_sqlite
import materialize_accounts as m
ROOT_ID='1o2TUruxT2HG67BHyZJTGGe0sjG8pQNMZ'

def upload(value):
    value=dict(value,remote='dsc_hf:26407881/hf-vector-f64-v2',drive_root_id=ROOT_ID)
    names=list(value['manifest']['files']);probes=[]
    for marker in ['/parts-','/aggregate_parts-','metadata/users.parquet']:
        match=next((n for n in names if marker in n),None)
        if match:probes.append(match)
    if len(probes)!=3:raise ValueError('Archive must restore user, aggregate and dictionary')
    value['restore_files']=probes
    return stage_upload(value)

def run(base,pilot=False,direct=False):
    publish_fn,upload_fn,close_fn=publish_source,upload,close_source
    if direct:
        from direct_hf_archive import describe_source,upload_direct
        publish_fn,upload_fn=describe_source,upload_direct
    base=Path(base);plan=m.load(base/'f64-plan.json');gen=plan['generation']
    import fcntl
    lock=(base/'f64-archive.lock').open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    db=sqlite3.connect(base/'f64-archive.sqlite');db.execute('pragma journal_mode=WAL');db.execute('pragma synchronous=FULL')
    db.execute('create table if not exists archived(generation text,job_id text,batch_id text,primary key(generation,job_id))');db.execute('create table if not exists batches(batch_id text primary key,report text)');db.commit()
    client=Client(plan['scheduler'],security=security(plan['private']),set_as_default=False);status=base/'public/archive.json'
    def publish(state,**kw):
        reports=[json.loads(x[0]) for x in db.execute('select report from batches')]
        write_json(status,dict(state=state,generation=gen,updated_at=time.time(),batch_target_bytes=LIMIT,staging_chunk_limit_bytes=0 if direct else 250_000_000,transport='node_direct_rclone' if direct else 'compute_staging',staged_bytes=0,verified_bytes=sum(x['verified_bytes'] for x in reports),error=None,**kw))
    try:
        while True:
            with tempfile.TemporaryDirectory(prefix='archive-snapshot-',dir=base) as folder:
                snapshot=Path(folder)/'ledger.sqlite';backup_sqlite(base/'tracker.sqlite',snapshot)
                with sqlite3.connect('file:'+str(snapshot)+'?mode=ro',uri=True) as snap:rows=snap.execute('select job_id,receipt,sha256 from accepted where generation=? order by job_id',(gen,)).fetchall()
            done={x[0] for x in db.execute('select job_id from archived where generation=?',(gen,))};ready=[];size=0
            for key,path,sha in rows:
                if key in done:continue
                if checksum(path)!=sha:raise ValueError('Data catalog receipt changed')
                r=m.load(path)
                if not r['within_error_budget'] or not r['structural_exact']:raise ValueError('Unaccepted archive job')
                if pilot and not r['parts']:continue
                ready.append((key,path,sha,r));size+=r['bytes']
                if pilot or size>=LIMIT:break
            finished=m.load(base/'public/data.json').get('state')=='complete'
            if not ready or (not pilot and size<LIMIT and not finished):
                publish('collecting',pending_bytes=size,pending_jobs=len(ready));time.sleep(30);continue
            batch=digest([(k,sha) for k,_,sha,_ in ready])[:20]
            metadata={n:(base/n).read_bytes() for n in ['users.parquet','dataset.json','f64-plan.json']}
            metadata['f64-plan-'+checksum(base/'f64-plan.json')+'.json']=metadata.pop('f64-plan.json')
            shared=client.submit(registry_source,metadata,plan['storage_base'],pure=False,resources={'f64_accept':1}).result()
            groups=[];group=[];group_size=0
            for item in ready:
                if group and group_size+item[3]['bytes']>(LIMIT*2 if direct else 250_000_000):groups.append(group);group=[];group_size=0
                group.append(item);group_size+=item[3]['bytes']
            if group:groups.append(group)
            reports=[]
            for index,group in enumerate(groups):
                files=dict(shared)
                for key,path,sha,r in group:
                    for p in r['parts']+r['aggregate_parts']:files[str(Path(p['path']).relative_to(plan['storage_base']))]=dict(bytes=0,sha256=p['sha256'])
                    files[str(Path(r['validation']).relative_to(plan['storage_base']))]=dict(bytes=0,sha256=r['validation_sha256'])
                    files['outputs/'+gen+'/jobs/'+key+'/receipt.json']=dict(bytes=Path(path).stat().st_size,sha256=sha)
                payload=dict(id=batch+'-'+str(index),generation=gen,files=files,rows=sum(x[3]['rows'] for x in group))
                payload=client.submit(resolve_sizes,payload,plan['storage_base'],pure=False,resources={'f64_accept':1}).result()
                value=client.submit(publish_fn,payload,plan['storage_base'],pure=False,resources={'f64_accept':1}).result()
                try:
                    publish('uploading',pending_bytes=size,active_batch=batch,active_chunk=index,total_chunks=len(groups))
                    report=client.submit(upload_fn,value,pure=False,resources={'f64_archive':1}).result();reports.append(report)
                finally:
                    if not direct:client.submit(close_fn,value,pure=False,resources={'f64_accept':1}).result()
            report=dict(id=batch,verified_bytes=sum(x['verified_bytes'] for x in reports),chunks=reports,jobs=[x[0] for x in ready],verified_at=time.time())
            db.execute('insert or ignore into batches values(?,?)',(batch,json.dumps(report,sort_keys=True)))
            for key,_,_,_ in ready:db.execute('insert or ignore into archived values(?,?,?)',(gen,key,batch))
            db.commit();publish('verified',last_batch=batch)
            if pilot:return
    except Exception:
        publish('paused',public_error='Preserve staging; inspect private archive logs');raise
    finally:client.close();db.close();lock.close()

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--base',required=True);p.add_argument('--pilot',action='store_true');a=p.parse_args();run(a.base,a.pilot)
