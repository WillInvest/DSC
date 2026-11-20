"""Bounded TLS file movement and metadata snapshots, independent of production."""
import json
import errno
import shutil
import fcntl
import multiprocessing
import os
import subprocess
import time
import uuid
from contextlib import contextmanager, nullcontext
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from shared.artifacts import checksum, digest
from hf_flow_json import write_json
from hf_flow_artifacts import combine, verify_destination, seal
from hf_flow_queue import LocalLedger, read_snapshot


def recover_published_transfer(path, sha256, aliases, *, durable=True):
    """Recover only named transfer scratch after canonical bytes are verified."""
    path=Path(path)
    if path.is_symlink() or not path.is_file() or checksum(path)!=sha256:
        raise ValueError('Existing transferred file differs')
    present=[]
    for alias in map(Path,aliases):
        if alias.absolute()==path.absolute() or alias.is_symlink():
            raise ValueError('Invalid transfer scratch alias')
        if not alias.exists():continue
        if not alias.is_file() or (not alias.samefile(path) and checksum(alias)!=sha256):
            raise ValueError('Transfer scratch differs; preserved')
        present.append(alias)
    for alias in present:alias.unlink()
    if not durable:return  # The batch caller synchronizes all touched directories.
    for directory in {alias.parent for alias in present}:
        fd=os.open(directory,os.O_RDONLY)
        try:os.fsync(fd)
        finally:os.close(fd)


def capacity_available(config,path):
    free=shutil.disk_usage(path).free
    if config.get('role')!='node':return free
    reservation=Path(config['node_flow_root'])/'capacity-reservation.json'
    try:value=json.loads(reservation.read_text())
    except FileNotFoundError:return free
    if value['generation']!=config['generation'] or value['reserved_bytes']<0:raise ValueError('Invalid node capacity reservation')
    return max(0,free-value['reserved_bytes'])


def sync_transfer_publication(files, directories):
    """Durably sync only declared closed files and touched directory entries."""
    paths=[str(p) for p in sorted(files)]
    paths.extend(str(p) for p in sorted(directories,key=lambda p:(-len(p.parts),str(p))))
    for offset in range(0,len(paths),128):
        # GNU sync with explicit paths invokes fsync, not filesystem-wide syncfs.
        subprocess.run(['/usr/bin/sync',*paths[offset:offset+128]],check=True,timeout=1800)


@contextmanager
def node_capacity(config,reserve_bytes=None):
    root=Path(config['node_flow_root']);root.mkdir(parents=True,exist_ok=True)
    with (root/'capacity.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        reservation=root/'capacity-reservation.json';token=uuid.uuid4().hex
        if reserve_bytes is not None:
            node=config.get('roles',{}).get('node',config)
            safety=node.get('reserve_bytes',50*2**30)+node.get('slots',20)*node.get('inflight_bytes_per_slot',1_000_000_000)
            if shutil.disk_usage(root).free<reserve_bytes+safety:raise OSError(errno.ENOSPC,'Insufficient node capacity for archive reservation')
            write_json(reservation,dict(token=token,reserved_bytes=reserve_bytes,generation=config['generation'],created_at=time.time(),pid=os.getpid()))
        try:yield
        finally:
            if reserve_bytes is not None:
                if json.loads(reservation.read_text())['token']!=token:raise ValueError('Archive capacity reservation owner changed')
                reservation.unlink()


def read_catalog(config, after=0):
    root=Path(config['local_root']); path=root/'local.sqlite'
    if not path.exists():return dict(entries=[],progress={},sequence=after)
    with read_snapshot(path) as db:
        rows=db.execute('select rowid,job_id,value from done where rowid>? order by rowid limit 128',(after,)).fetchall()
    progress=json.loads((root/'progress.json').read_text()) if (root/'progress.json').exists() else {}
    return dict(entries=[dict(sequence=n,job_id=k,value=json.loads(v)) for n,k,v in rows],
                sequence=rows[-1][0] if rows else after,progress=progress)


def _copy_batch_locked(payload, config):
    # A fresh child is a Dask client, not a worker-local fsspec view.
    pre_staged=payload.get('pre_staged',False)
    if pre_staged and not payload.get('rsync_source'):raise ValueError('Pre-staged acceptance requires explicit source scope')
    if not pre_staged:
        import dask
        import fsspec
        from distributed import Client
        from vector_coordinator import security
    target=Path(config['node_bulk'])
    if config.get('node_storage_mount'):
        mount=Path(config['node_storage_mount'])
        if not mount.is_mount() or not target.is_relative_to(mount):
            raise ValueError('Required node storage mount is unavailable or outside target')
    required_staging=config.get('node_staging_mounts',{}).get(payload.get('role'))
    if required_staging and payload.get('rsync_source'):
        stage_mount=Path(payload['rsync_source']['rsync_transport']['node_stage_base'])
        if stage_mount!=Path(required_staging) or not stage_mount.is_mount():
            raise ValueError('Required node staging mount is unavailable or changed')
    target.mkdir(parents=True,exist_ok=True)
    reports=[];verified_certificates=[];publication_files=set();publication_directories=set()
    connection=nullcontext(None) if pre_staged else Client(config['scheduler'],security=security(config['tls_private']),set_as_default=True)
    with connection as client:
        # Keep the live Client out of the serialized filesystem graph. The
        # child has a default TLS Client; workers reconstruct a local view.
        fs=None
        staged=None;retain_staging=False
        if payload.get('rsync_source'):
            from hf_flow_rsync import manifest_entries,stage_source
            source=payload['rsync_source'];needed={}
            stage_base=Path(source['rsync_transport']['node_stage_base'])
            physical=config.get('node_staging_sources',{}).get(payload['role'])
            if physical:
                physical=Path(physical)
                storage=Path(config['node_storage_mount'])
                if (not physical.resolve().is_relative_to(storage.resolve())
                        or not physical.is_dir() or not physical.samefile(stage_base)):
                    raise ValueError('Physical staging alias differs from receiving directory')
                stage_base=physical
            staged=stage_base/payload['id']
            staged.mkdir(parents=True,exist_ok=True)
            retain_staging=staged.stat().st_dev!=target.stat().st_dev
            for value in payload['values']:
                directory=target/'outputs'/value['receipt']['generation']/'jobs'/value['receipt']['job_id']
                for relative,name,sha in manifest_entries(value,source['output_root']):
                    path=directory/name
                    if path.exists():
                        if path.is_symlink() or not path.is_file():raise ValueError('Invalid existing transfer destination')
                    else:needed[relative]=sha
            if needed:
                required=sum(x['bytes'] for n,x in payload['certificate']['files'].items() if n in needed) if payload.get('certificate') else 0
                copy_reserve=4*max(config.get('copy_file_reserve_bytes',2**30),1)
                if capacity_available(config,target)<config['reserve_bytes']+config['slots']*config['inflight_bytes_per_slot']+required+copy_reserve:
                    raise OSError(errno.ENOSPC,'Node rsync staging reserve reached')
                if pre_staged:
                    for name in needed:
                        member=staged/name
                        if member.is_symlink() or not member.is_file():raise ValueError('Pre-staged transfer is incomplete')
                else:
                    pins=[(v['receipt']['job_id'],v['sha256']) for v in payload['values']]
                    future=client.submit(stage_source,source,pins,payload['id'],needed,pure=False,workers=payload['source_workers'],allow_other_workers=False,resources={'flow_io':1})
                    try:copied=future.result()
                    finally:future.release()
                    if copied['manifest_sha256']!=digest(needed):raise ValueError('Staged rsync manifest differs')
        else:
            fs=fsspec.filesystem('dask',target_protocol='file',client=None,skip_instance_cache=True)
        for value in payload['values']:
            r=value['receipt'];key=r['job_id'];directory=target/'outputs'/r['generation']/'jobs'/key
            directory.mkdir(parents=True,exist_ok=True)
            parquet_names={Path(p['path']).name for p in r['parts']+r['aggregate_parts']}
            entries=[(p['path'],Path(p['path']).name,p['sha256']) for p in r['parts']+r['aggregate_parts']]
            entries.extend([(r['validation'],'validation.json',r['validation_sha256']),
                            (value['path'],'origin-receipt.json',value['sha256'])])
            def copy_one(entry):
                source,name,sha=entry;path=directory/name
                copied_stage=False
                if path.is_symlink():raise ValueError('Symlink destination')
                if path.exists():
                    aliases=[path.with_name(path.name+'.transferring')]
                    if staged is not None and not retain_staging:aliases.append(staged/Path(source).relative_to(payload['rsync_source']['output_root']))
                    if name not in parquet_names or any(p.exists() or p.is_symlink() for p in aliases):
                        recover_published_transfer(path,sha,aliases,durable=staged is None)
                    return
                reserve=config['reserve_bytes']+config['slots']*config['inflight_bytes_per_slot']
                # Each of four concurrent copies is bounded by one accepted tile;
                # keep the full numerical in-flight allowance unavailable to IO.
                if capacity_available(config,target)<reserve+4*max(config.get('copy_file_reserve_bytes',2**30),1):
                    raise OSError(errno.ENOSPC,'Node transfer reserve reached')
                temporary=path.with_name(path.name+'.transferring')
                if staged is not None:
                    incoming=staged/Path(source).relative_to(payload['rsync_source']['output_root'])
                    if incoming.is_symlink() or not incoming.is_file():raise ValueError('Invalid staged rsync member')
                    if temporary.exists():
                        if temporary.is_symlink() or not temporary.is_file():raise ValueError('Invalid prior transfer temporary')
                        temporary.unlink()
                    copied_stage=retain_staging
                    if copied_stage:shutil.copyfile(incoming,temporary)
                    else:
                        try:os.link(incoming,temporary)
                        except OSError as exc:
                            if exc.errno!=errno.EXDEV:raise
                            shutil.copyfile(incoming,temporary);copied_stage=True
                else:
                    with dask.annotate(workers=payload['source_workers'],allow_other_workers=False):
                        fs.get_file(source,str(temporary))
                if name not in parquet_names and checksum(temporary)!=sha:raise ValueError('Copied metadata SHA differs')
                if staged is None:
                    with temporary.open('rb') as stream:os.fsync(stream.fileno())
                os.link(temporary,path);temporary.unlink()
                if staged is not None and not copied_stage:incoming.unlink()
            with ThreadPoolExecutor(max_workers=4) as pool:
                for _ in pool.map(copy_one,entries):pass
            if parquet_names:
                if payload.get('certificate') is None:raise ValueError('Missing source Parquet certificate')
                files={name:payload['certificate']['files'][str((directory/name).relative_to(target))] for name in parquet_names}
                if any(files[Path(p['path']).name]['sha256']!=p['sha256'] for p in r['parts']+r['aggregate_parts']):raise ValueError('Receipt and source certificate differ')
                verified_certificates.append(seal(directory,files))
            original=json.loads((directory/'origin-receipt.json').read_text())
            if original!=r:raise ValueError('Transferred original receipt differs')
            moved=dict(r,origin_receipt_sha256=value['sha256'],origin_role=payload['role'])
            for group in ['parts','aggregate_parts']:
                moved[group]=[dict(p,path=str(directory/Path(p['path']).name)) for p in r[group]]
            moved['validation']=str(directory/'validation.json')
            receipt=directory/'receipt.json'
            if receipt.exists():
                if json.loads(receipt.read_text())!=moved:raise ValueError('Canonical node receipt conflict')
            else:write_json(receipt,moved,durable=staged is None)
            reports.append(dict(job_id=key,receipt=moved,path=str(receipt),sha256=checksum(receipt)))
            if staged is not None:
                publication_files.update(directory/name for _,name,_ in entries)
                publication_files.add(receipt)
                touched=[directory]
                if not retain_staging:
                    touched.extend((staged/Path(source).relative_to(payload['rsync_source']['output_root'])).parent for source,_,_ in entries)
                for parent in touched:
                    boundary=target.parent if parent==directory else staged.parent
                    while True:
                        publication_directories.add(parent)
                        if parent==boundary:break
                        parent=parent.parent
        if staged is not None:
            # All staged bytes, canonical names, and receipts must be durable
            # before a proof permits retirement on the source host. A single
            # group barrier avoids one journal commit per small file while
            # leaving unrelated node/RETH files outside the synchronization set.
            sync_transfer_publication(publication_files,publication_directories)
        proof=verify_destination(payload['certificate'],target,destination_host='node',verified_certificates=verified_certificates) if payload.get('certificate') else None
    report=dict(id=payload['id'],role=payload['role'],jobs=reports,all_exact=True,source_certificate=payload.get('certificate'),destination_proof=proof,verified_at=time.time())
    ops=Path(config['node_flow_root'])/'transfers'/payload['id'];ops.mkdir(parents=True,exist_ok=True)
    write_json(ops/'verified.json',report)
    return report



def _copy_batch(payload,config):
    with node_capacity(config):
        return _copy_batch_locked(payload,config)


def copy_batch(payload, config):
    ops=Path(config['node_flow_root'])/'transfers'/payload['id'];ops.mkdir(parents=True,exist_ok=True)
    with (ops/'copy.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        prior=transfer_report(config,payload['id'])
        if prior is not None:return prior
        with multiprocessing.get_context('spawn').Pool(1,maxtasksperchild=1) as pool:
            return pool.apply(_copy_batch,(payload,config))


def transfer_report(config,batch):
    path=Path(config['node_flow_root'])/'transfers'/batch/'verified.json'
    return json.loads(path.read_text()) if path.exists() else None


def retire_local(config, transfer):
    root=Path(config['local_root']);ops=root/'transfers'/transfer['id'];ops.mkdir(parents=True,exist_ok=True)
    if transfer.get('all_exact') is not True:raise ValueError('Unverified transfer')
    cert=transfer['source_certificate'];proof=transfer['destination_proof'];reports=[]
    if cert is not None:
        request=ops/'batch.cleanup.json';value=dict(certificate=cert,proof=proof)
        if request.exists():
            if json.loads(request.read_text())!=value:raise ValueError('Retirement request conflict')
        else:write_json(request,value)
        proc=subprocess.run(['sudo','-n','/usr/bin/python3','/usr/local/libexec/dsc-hf-flow/hf_flow_retire.py',str(request),checksum(request)],check=True,capture_output=True,text=True,timeout=3600)
        reports.append(json.loads(proc.stdout))
    with LocalLedger(root/'local.sqlite',config['generation']) as db:
        db.mark_retired([x['job_id'] for x in transfer['jobs']],digest(transfer))
    report=dict(id=transfer['id'],jobs=reports,source_copies_retired=True,node_copies_verified=True,completed_at=time.time())
    write_json(ops/'retired.json',report)
    return report


def hold(config, reason):
    write_json(Path(config['local_root'])/'HOLD',dict(reason=reason,created_at=time.time()))
    return True


def prepare_transfer(config,keys,batch):
    root=Path(config['local_root']);values=[]
    with read_snapshot(root/'local.sqlite') as db:
        for key in keys:
            row=db.execute('select value from done where job_id=?',(key,)).fetchone()
            if not row:raise ValueError('Unaccepted local transfer input')
            value=json.loads(row[0])
            if checksum(value['path'])!=value['sha256']:raise ValueError('Local receipt changed before transfer')
            values.append(value)
    certs=[v['certificate'] for v in values if v['certificate'] is not None]
    cert=combine(config['output_root'],certs) if certs else None
    payload=dict(id=batch,role=config['role'],keys=keys,values=values,certificate=cert)
    if config.get('rsync_transport'):
        payload['rsync_source']={k:config[k] for k in ['local_root','output_root','rsync_transport']}
    return payload
