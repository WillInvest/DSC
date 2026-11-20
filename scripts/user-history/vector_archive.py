"""Data-owned 500 GB archive batches: explicit receipts, rsync, rclone, readback.

Dask TLS carries control and the one-time rsync password. Existing Drive OAuth
stays on web. Node keeps accepted files; only verified web staging is retired.
"""
import argparse
import hashlib
import json
import os
import secrets
import sqlite3
import subprocess
import time
import tempfile
from pathlib import Path
from distributed import Client
from shared.artifacts import checksum, digest, write_json
from vector_coordinator import security
import materialize_accounts as m

WEB='/home/fao/dsc/.ops/runs/drive-upload-20261116'
STAGING='/home/fao/dsc/data/incoming/aave-hf-drive'
RCLONE=WEB+'/tools/extracted/usr/bin/rclone'
CONFIG='/home/fao/.config/rclone/dsc-hf.conf'
REMOTE='dsc_hf:DSC-Aave-V3-HF/26407881/hf-vector-v1'
LIMIT=500_000_000_000
CHUNK=32_000_000_000


def publish_source(payload, base):
    base=Path(base);name=payload['id'];folder=base/'archive-publish'/name;folder.mkdir(parents=True,exist_ok=True)
    stage=folder/'files';stage.mkdir(exist_ok=True)
    files=payload['files']
    for relative,item in files.items():
        src=base/relative;dst=stage/relative
        if not src.resolve().is_relative_to(base.resolve()) or src.is_symlink() or checksum(src)!=item['sha256'] or src.stat().st_size!=item['bytes']:
            raise ValueError('Archive source identity differs')
        dst.parent.mkdir(parents=True,exist_ok=True)
        if dst.exists():
            if not dst.samefile(src):raise ValueError('Preserved source staging conflicts')
        else:os.link(src,dst)
    manifest=dict(id=name,files=files,source_base=str(base),generation=payload['generation'],role='node',new_rows=payload['rows'],recovered_rows=0)
    path=stage/'manifest.json'
    if path.exists():
        if m.load(path)!=manifest:raise ValueError('Archive manifest changed')
    else:write_json(path,manifest)
    password=secrets.token_hex(32);secret=folder/'rsync-secret';secret.write_text('hfarchive:'+password+'\n');secret.chmod(0o600)
    config=folder/'rsync.conf';config.write_text('use chroot = no\nread only = yes\nlist = no\n[accepted]\npath = '+str(stage)+'\nhosts allow = 10.246.103.143\nhosts deny = *\nauth users = hfarchive\nsecrets file = '+str(secret)+'\n')
    unit='dsc-hf-vector-archive-'+name
    subprocess.run(['sudo','systemd-run','--quiet','--collect','--unit='+unit,'--property=User=node','--property=MemoryMax=256M','--property=CPUQuota=100%','--property=RuntimeMaxSec=3600','/usr/bin/rsync','--daemon','--no-detach','--port=18888','--address=10.246.103.160','--config='+str(config)],check=True)
    return dict(manifest=manifest,relative='manifest.json',sha256=checksum(path),manifest_bytes=path.stat().st_size,password=password,unit=unit,secret=str(secret))


def close_source(value):
    subprocess.run(['sudo','systemctl','stop',value['unit']],check=False,capture_output=True)
    Path(value['secret']).unlink(missing_ok=True)


def stage_upload(value):
    """Only manifest-listed files; successful remote MD5 and a SHA256 readback."""
    from backup_hf import needed,verify
    batch=value['manifest']['id'];root=Path(STAGING)/batch;ops=Path(WEB)/'batches'/batch;ops.mkdir(parents=True,exist_ok=True)
    lock=(ops/'lock').open('a')
    import fcntl
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    entries=dict(value['manifest']['files']);entries['manifest.json']=dict(bytes=value['manifest_bytes'],sha256=value['sha256'])
    passwords=ops/'rsync-password';passwords.write_text(value['password']+'\n');passwords.chmod(0o600)
    filelist=ops/'files.txt';filelist.write_text('\n'.join(sorted(entries))+'\n')
    missing=needed(root,value)
    missinglist=ops/'missing.txt';missinglist.write_text('\n'.join(missing['names'])+'\n')
    try:
        subprocess.run(['rsync','-a','--ignore-existing','--files-from='+str(missinglist),'--partial','--partial-dir=.rsync-partial','--contimeout=20','--timeout=300','--password-file='+str(passwords),'rsync://hfarchive@10.246.103.160:18888/accepted/',str(root)+'/'],check=True,capture_output=True)
    finally:passwords.unlink(missing_ok=True)
    verify(root,'manifest.json',value['sha256'])
    dest=value.get('remote',REMOTE)+'/batches/'+batch
    common=[RCLONE,'--config',CONFIG]
    if value.get('drive_root_id'):common+=['--drive-root-folder-id',value['drive_root_id']]
    # Fail rather than silently overwrite a different object in an existing batch.
    subprocess.run(common+['copy',str(root),dest,'--files-from',str(filelist),'--checksum','--immutable','--transfers','4','--checkers','8','--drive-stop-on-upload-limit','--log-file',str(ops/'copy.log'),'--log-level','INFO'],check=True,capture_output=True)
    subprocess.run(common+['check',str(root),dest,'--files-from',str(filelist),'--one-way','--checkers','8','--log-file',str(ops/'check.log'),'--log-level','INFO'],check=True,capture_output=True)
    parquet=next((k for k in entries if '/part-' in k and k.endswith('.parquet')),next((k for k in entries if k.endswith('.parquet')),None))
    probes=value.get('restore_files',[parquet] if parquet else []);samples=[]
    for parquet in probes:
        if parquet not in entries:raise ValueError('Undeclared restore probe')
        reader=subprocess.Popen(common+['cat',dest+'/'+parquet],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        h=hashlib.sha256()
        for data in iter(lambda:reader.stdout.read(2**20),b''):h.update(data)
        reader.stdout.close();err=reader.stderr.read();reader.wait()
        if reader.returncode or h.hexdigest()!=entries[parquet]['sha256']:raise ValueError('Drive SHA256 restore probe failed')
        samples.append(dict(path=parquet,sha256=h.hexdigest(),exact=True))
    # Recheck local staging before its authorized retirement. Node originals stay.
    verify(root,'manifest.json',value['sha256'])
    report=dict(id=batch,state='verified',verified_at=time.time(),files=len(entries),verified_bytes=sum(x['bytes'] for x in entries.values()),remote=dest,all_remote_size_md5_exact=True,source_sha256_manifest=value['sha256'],restore_probe=samples[0] if samples else None,restore_probes=samples,node_originals_retained=True)
    write_json(ops/'verified.json',report);write_json(ops/'manifest-retained.json',value['manifest'])
    for name in entries:(root/name).unlink()
    report['local_staging_released_bytes']=report['verified_bytes'];write_json(ops/'verified.json',report)
    lock.close();return report


def registry_source(data, base):
    """Small immutable metadata transfer, never credentials or active databases."""
    base=Path(base);result={}
    for name,value in data.items():
        p=base/'metadata'/name;p.parent.mkdir(parents=True,exist_ok=True)
        if p.exists():
            if p.read_bytes()!=value:raise ValueError('Preserved archive metadata differs')
        else:
            with p.open('xb') as h:h.write(value);h.flush();os.fsync(h.fileno())
        result[str(p.relative_to(base))]=dict(sha256=checksum(p),bytes=p.stat().st_size)
    return result


def run(base, pilot=False):
    from distributed_hf import backup_sqlite
    base=Path(base);plan=m.load(base/'vector-plan.json');gen=plan['generation'];manifest_sha=checksum(base/'vector-plan.json')
    lock=(base/'archive.lock').open('a');import fcntl;fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    db=sqlite3.connect(base/'archive.sqlite');db.execute('pragma journal_mode=WAL');db.execute('pragma synchronous=FULL');db.execute('create table if not exists archived(job_id text primary key,batch_id text)');db.execute('create table if not exists batches(batch_id text primary key,report text)');db.commit()
    client=Client(plan['scheduler'],security=security(plan['private']),set_as_default=False);status=base/'public/archive.json'
    def publish(state,**kw):
        reports=[json.loads(r[0]) for r in db.execute('select report from batches')]
        write_json(status,dict(state=state,updated_at=time.time(),generation=gen,batch_target_bytes=LIMIT,staging_chunk_limit_bytes=CHUNK,staged_bytes=0,verified_bytes=sum(r['verified_bytes'] for r in reports),error=None,**kw))
    try:
        while True:
            with tempfile.TemporaryDirectory(prefix='archive-snapshot-',dir=base) as folder:
                snapshot=Path(folder)/'ledger.sqlite';backup_sqlite(base/'tracker.sqlite',snapshot)
                with sqlite3.connect('file:'+str(snapshot)+'?mode=ro',uri=True) as snap:rows=snap.execute('select job_id,receipt,sha256 from accepted where generation=? order by job_id',(gen,)).fetchall()
            done={r[0] for r in db.execute('select job_id from archived')};ready=[];size=0
            for key,path,sha in rows:
                if key in done:continue
                if checksum(path)!=sha:raise ValueError('Archive Data receipt identity differs')
                r=m.load(path);ready.append((key,path,sha,r));size+=r['bytes']
                if (pilot and not done) or size>=LIMIT:break
            finished=m.load(base/'public/data.json').get('state')=='complete'
            if not ready or (size<LIMIT and not(pilot and not done) and not finished):
                publish('collecting',pending_bytes=size,pending_jobs=len(ready));time.sleep(60);continue
            batch=digest([(k,sha) for k,_,sha,_ in ready])[:20];chunks=[];group=[];group_bytes=0
            for item in ready:
                if group and group_bytes+item[3]['bytes']>CHUNK:chunks.append(group);group=[];group_bytes=0
                group.append(item);group_bytes+=item[3]['bytes']
            if group:chunks.append(group)
            metadata={p.name:p.read_bytes() for p in [base/'users.parquet',base/'dataset.json',base/'vector-plan.json']}
            metadata['vector-plan-'+manifest_sha+'.json']=metadata.pop('vector-plan.json')
            shared=client.submit(registry_source,metadata,plan['storage_base'],resources={'vector_accept':1},pure=False).result()
            all_reports=[]
            for index,group in enumerate(chunks):
                files=dict(shared)
                for key,path,sha,r in group:
                    for part in r['parts']:
                        relative=str(Path(part['path']).relative_to(plan['storage_base']));files[relative]=dict(bytes=0,sha256=part['sha256'])
                    # Size is metadata on node, resolved there; never use nonexistent Data paths.
                    files[str(Path(r['validation']).relative_to(plan['storage_base']))]=dict(bytes=0,sha256=r['validation_sha256'])
                    files['outputs/'+gen+'/jobs/'+key+'/receipt.json']=dict(bytes=Path(path).stat().st_size,sha256=sha)
                payload=dict(id=batch+'-'+str(index),generation=gen,files=files,rows=sum(item[3]['rows'] for item in group))
                payload=client.submit(resolve_sizes,payload,plan['storage_base'],resources={'vector_accept':1},pure=False).result()
                value=client.submit(publish_source,payload,plan['storage_base'],resources={'vector_accept':1},pure=False).result()
                try:
                    publish('uploading',pending_bytes=size,active_batch=batch,active_chunk=index,total_chunks=len(chunks))
                    result=client.submit(stage_upload,value,resources={'vector_archive':1},pure=False,retries=0).result();all_reports.append(result)
                finally:client.submit(close_source,value,resources={'vector_accept':1},pure=False).result()
            report=dict(id=batch,verified_bytes=sum(r['verified_bytes'] for r in all_reports),chunks=all_reports,verified_at=time.time(),jobs=[x[0] for x in ready])
            db.execute('insert into batches values(?,?)',(batch,json.dumps(report,sort_keys=True)))
            for key,_,_,_ in ready:db.execute('insert into archived values(?,?)',(key,batch))
            db.commit();publish('verified',last_batch=batch)
            if pilot:return
    except Exception as e:
        publish('paused',failure_type=type(e).__name__,public_error='Upload paused; preserve staging and inspect private task logs')
        # Exceptions from children stay in private service logs, never credentials.
        raise
    finally:client.close();db.close();lock.close()


def resolve_sizes(payload, base):
    for relative,item in payload['files'].items():
        p=Path(base)/relative
        if not p.resolve().is_relative_to(Path(base).resolve()) or p.is_symlink():raise ValueError('Archive path escapes declared output root')
        if item['bytes'] and item['bytes']!=p.stat().st_size:raise ValueError('Archive size differs')
        item['bytes']=p.stat().st_size
    return payload


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--base',required=True);p.add_argument('--pilot',action='store_true');a=p.parse_args();run(a.base,a.pilot)
