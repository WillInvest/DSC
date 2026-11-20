"""Immutable rclone batches with retained restore index and manifest-only cleanup."""
import fcntl,hashlib,json,os,subprocess,time,uuid
from pathlib import Path
from shared.artifacts import checksum,digest,write_json
from hf_flow_artifacts import seal,fast_verify,combine,PROOF_FORMAT
from hf_flow_packing import pack,FORMAT as PACK_FORMAT
from hf_flow_io import node_capacity

TOOL='/home/node/hf-drive/tools/rclone'
CONFIG='/home/node/.config/rclone/dsc-hf.conf'
ROOT_ID='1o2TUruxT2HG67BHyZJTGGe0sjG8pQNMZ'


def _retire(certificate,proof,request):
    value=dict(certificate=certificate,proof=proof)
    if request.exists():
        if json.loads(request.read_text())!=value:raise ValueError('Cloud cleanup request changed')
    else:write_json(request,value)
    p=subprocess.run(['sudo','-n','/usr/bin/python3','/usr/local/libexec/dsc-hf-flow/hf_flow_retire.py',str(request),checksum(request)],check=True,capture_output=True,text=True,timeout=3600)
    return json.loads(p.stdout)


def _common():
    return [TOOL,'--config',CONFIG,'--drive-root-folder-id',ROOT_ID,'--contimeout','20s','--timeout','60s']


def _readback(remote,expected,*,offset=None,count=None,output=None):
    args=_common()+['cat',remote]
    if offset is not None:args+=['--offset',str(offset),'--count',str(count)]
    sha=hashlib.sha256();size=0
    stream=Path(output).open('xb') if output is not None else None
    try:
        with subprocess.Popen(args,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL) as child:
            for data in iter(lambda:child.stdout.read(2**20),b''):
                sha.update(data);size+=len(data)
                if stream is not None:stream.write(data)
            code=child.wait()
        if stream is not None:stream.flush();os.fsync(stream.fileno())
        if code:raise subprocess.CalledProcessError(code,args)
        if size!=expected['bytes'] or sha.hexdigest()!=expected['sha256']:
            raise ValueError('Cloud SHA256/length readback failed')
        return dict(sha256=sha.hexdigest(),bytes=size,exact=True)
    finally:
        if stream is not None:stream.close()


def _probe_names(files):
    names=[]
    for marker in ['parts-','aggregate_parts-','users.parquet']:
        name=next((n for n in files if Path(n).name.startswith(marker)),None)
        if name is None:raise ValueError('Missing three-product/dictionary restore probe')
        names.append(name)
    return names


def _mark_local_retired(payload,config,proof_sha):
    from hf_flow_queue import LocalLedger
    local=Path(config['roles']['node']['local_root'])/'local.sqlite'
    if local.exists():
        with LocalLedger(local,config['generation']) as ledger:
            own=ledger.completed();ledger.mark_retired([k for k,_ in payload['job_receipts'] if k in own],proof_sha)


def _copy_and_check(base,destination,files,ops):
    listing=ops/'files.txt';listing.write_text('\n'.join(sorted(files))+'\n')
    logargs=['--log-file',str(ops/'rclone.log'),'--log-level','INFO']
    subprocess.run(_common()+['copy',str(base),destination,'--files-from',str(listing),'--immutable','--checksum','--transfers','4','--checkers','8','--drive-stop-on-upload-limit']+logargs,check=True,capture_output=True,timeout=86400)
    subprocess.run(_common()+['check',str(base),destination,'--files-from',str(listing),'--one-way','--checkers','8']+logargs,check=True,capture_output=True,timeout=86400)


def _upload_packed(manifest,manifest_path,base,ops,destination,config):
    for cert in manifest['certificates'].values():fast_verify(cert)
    files=dict(manifest['files']);name=str(manifest_path.relative_to(base))
    files[name]=dict(bytes=manifest_path.stat().st_size,sha256=checksum(manifest_path))
    required=sum(v['bytes'] for v in files.values())+len(files)*20480
    with node_capacity(config,reserve_bytes=required):
        index=pack(base,files,ops,target_bytes=config.get('cloud_pack_bytes',2_000_000_000))
    index_path=ops/'pack-index.json';index_sha=checksum(index_path)
    transfer_files=dict(index['packs'])
    transfer_files['pack-index.json']=dict(bytes=index_path.stat().st_size,sha256=index_sha)
    transfer_files['manifest.json']=dict(bytes=manifest_path.stat().st_size,sha256=checksum(manifest_path))
    package_certificate=seal(ops,{n:dict(bytes=v['bytes'],sha256=v['sha256']) for n,v in index['packs'].items()})
    write_json(ops/'package-certificate.json',package_certificate)
    _copy_and_check(ops,destination,transfer_files,ops)
    probes=[]
    for name in _probe_names(manifest['files']):
        member=index['members'][name]
        proof=_readback(destination+'/'+member['pack'],member,offset=member['offset'],count=member['bytes'])
        probes.append(dict(proof,path=name,pack=member['pack'],offset=member['offset']))
    first=next(iter(index['packs']))
    package_probe=dict(_readback(destination+'/'+first,index['packs'][first]),path=first)
    for cert in manifest['certificates'].values():fast_verify(cert)
    return dict(id=manifest['id'],generation=config['generation'],remote=destination,archive_format=PACK_FORMAT,
        manifest_sha256=checksum(manifest_path),index_sha256=index_sha,index_path=str(index_path),
        verified_bytes=sum(v['bytes'] for v in transfer_files.values()),logical_input_bytes=sum(v['bytes'] for v in manifest['files'].values()),
        all_remote_size_md5_exact=True,all_local_tar_members_sha256_exact=True,restore_probes=probes,package_restore_probes=[package_probe],
        verified_at=time.time(),job_receipts=manifest['job_receipts'],scope='Packed exact vectors; every TAR member SHA verified locally, all cloud objects size/MD5 checked, three member and one full TAR SHA readbacks')


def _upload_raw(manifest,manifest_path,base,ops,destination,config):
    for cert in manifest['certificates'].values():fast_verify(cert)
    files=dict(manifest['files']);relative=str(manifest_path.relative_to(base));files[relative]=dict(bytes=manifest_path.stat().st_size,sha256=checksum(manifest_path))
    _copy_and_check(base,destination,files,ops)
    probes=[dict(_readback(destination+'/'+name,files[name]),path=name) for name in _probe_names(files)]
    for cert in manifest['certificates'].values():fast_verify(cert)
    report=dict(id=manifest['id'],generation=config['generation'],remote=destination,manifest_sha256=checksum(manifest_path),verified_bytes=sum(x['bytes'] for x in files.values()),all_remote_size_md5_exact=True,restore_probes=probes,verified_at=time.time(),job_receipts=manifest['job_receipts'],scope='Independent user/reserve/protocol vectors; full size/MD5 plus three SHA256 readbacks')
    return report


def upload_and_retire(payload,config):
    base=Path(config['node_home'])
    keep_archives=config.get('retain_node_archives',False)
    if keep_archives and config.get('cloud_transport','packed')!='packed':
        raise ValueError('Node archive retention requires packed transport')
    archive_root=Path(config.get('node_archive_root',str(Path(config['node_flow_root'])/'cloud')))
    if not archive_root.is_absolute() or not archive_root.resolve().is_relative_to(base.resolve()) or archive_root.is_symlink():
        raise ValueError('Node archive root must remain inside the node home')
    ops=archive_root/payload['id'];ops.mkdir(parents=True,exist_ok=True)
    with (ops/'lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        finished=ops/'complete.json'
        if finished.exists():
            result=json.loads(finished.read_text())
            if result['job_receipts']!=payload['job_receipts']:raise ValueError('Completed cloud batch scope changed')
            _mark_local_retired(payload,config,checksum(ops/'verified.json'))
            return result
        manifest_path=ops/'manifest.json'
        if manifest_path.exists():
            manifest=json.loads(manifest_path.read_text())
            if manifest['job_receipts']!=payload['job_receipts']:raise ValueError('Prepared cloud scope changed')
        else:
            files={};certificates={}
            for value in payload['values']:
                r=value['receipt'];directory=Path(value['path']).parent;parts={}
                for p in r['parts']+r['aggregate_parts']:
                    path=Path(p['path']);name=str(path.relative_to(base));size=path.stat().st_size
                    files[name]=dict(bytes=size,sha256=p['sha256']);parts[path.name]=files[name]
                if parts:certificates[r['job_id']]=seal(directory,parts)
                for path,sha in [(Path(value['path']),value['sha256']),(Path(r['validation']),r['validation_sha256'])]:
                    if checksum(path)!=sha:raise ValueError('Accepted cloud metadata changed')
                    files[str(path.relative_to(base))]=dict(bytes=path.stat().st_size,sha256=sha)
                origin=directory/'origin-receipt.json'
                if r.get('origin_receipt_sha256'):
                    if checksum(origin)!=r['origin_receipt_sha256']:raise ValueError('Original local acceptance changed')
                    files[str(origin.relative_to(base))]=dict(bytes=origin.stat().st_size,sha256=r['origin_receipt_sha256'])
            files.update(payload['metadata'])
            combined=combine(base,list(certificates.values())) if certificates else None
            manifest=dict(id=payload['id'],generation=config['generation'],job_receipts=payload['job_receipts'],files=files,certificates=certificates,retirement_certificate=combined,archive_format=PACK_FORMAT if config.get('cloud_transport','packed')=='packed' else 'raw',created_at=time.time())
            write_json(manifest_path,manifest)
        verified=ops/'verified.json'
        destination=config['cloud_remote']+'/batches/'+payload['id']
        if verified.exists():
            report=json.loads(verified.read_text())
            if report['manifest_sha256']!=checksum(manifest_path):raise ValueError('Saved cloud proof changed')
        else:
            uploader=_upload_packed if manifest.get('archive_format')==PACK_FORMAT else _upload_raw
            report=uploader(manifest,manifest_path,base,ops,destination,config)
            write_json(verified,report)
        # Privileged cleanup requests stay in their existing approved control
        # root; the separately retained primary archive is not a cleanup root.
        cleanup_ops=Path(config['node_flow_root'])/'cloud'/payload['id']
        cleanup_ops.mkdir(parents=True,exist_ok=True)
        cleanup=[];cert=manifest['retirement_certificate']
        if cert is not None:
            probes=[p for p in report['restore_probes'] if p['path'] in cert['files']]
            if not probes:raise ValueError('No exact Parquet readback for retirement manifest')
            proof=dict(format=PROOF_FORMAT,source_certificate_sha256=digest(cert),manifest_sha256=digest(cert['files']),files=cert['files'],all_exact=True,destination=destination,verification='rclone_size_md5_sha256_restore',all_remote_size_md5_exact=True,restore_probes=probes,cloud_report_sha256=checksum(verified),cloud_manifest_sha256=checksum(manifest_path))
            cleanup.append(_retire(cert,proof,cleanup_ops/'batch.cleanup.json'))
        if report.get('archive_format')==PACK_FORMAT and not keep_archives:
            package_certificate=json.loads((ops/'package-certificate.json').read_text())
            proof=dict(format=PROOF_FORMAT,generation=config['generation'],source_certificate_sha256=digest(package_certificate),manifest_sha256=digest(package_certificate['files']),files=package_certificate['files'],all_exact=True,destination=destination,verification='rclone_size_md5_sha256_restore',all_remote_size_md5_exact=True,restore_probes=report['package_restore_probes'],cloud_report_sha256=checksum(verified),cloud_index_sha256=report['index_sha256'])
            cleanup.append(_retire(package_certificate,proof,cleanup_ops/'packages.cleanup.json'))
        packed=report.get('archive_format')==PACK_FORMAT
        report=dict(report,node_result_parquets_retired=True,node_tar_packages_retired=packed and not keep_archives,
                    node_tar_packages_retained=packed and keep_archives,retirement_reports=cleanup,completed_at=time.time())
        _mark_local_retired(payload,config,checksum(verified))
        write_json(finished,report)
        return report


def _safe_member(index,name,sha):
    member=index['members'][name];pack_name=Path(member['pack'])
    if (pack_name.is_absolute() or '..' in pack_name.parts or str(pack_name)!=member['pack']
        or len(pack_name.parts)!=2 or pack_name.parts[0]!='packs' or pack_name.suffix!='.tar'
        or member['pack'] not in index['packs'] or member['sha256']!=sha
        or not isinstance(member['offset'],int) or member['offset']<0 or not isinstance(member['bytes'],int) or member['bytes']<0
        or member['offset']+member['bytes']>index['packs'][member['pack']]['bytes']):
        raise ValueError('Invalid pinned archive member')
    return member


def restore_jobs(payload,config):
    """Restore exact accepted files through a pinned index or legacy raw batch."""
    base=Path(config['node_home']);ops=Path(config['node_flow_root'])/'restores'/payload['id'];ops.mkdir(parents=True,exist_ok=True)
    restored=[];index=None
    if payload.get('archive_format')==PACK_FORMAT:
        index_path=ops/'pack-index.json';incoming=ops/('index-'+uuid.uuid4().hex+'.incoming')
        # The index hash is pinned by Data's accepted cloud report, never taken
        # from an untrusted remote index itself.
        sha=payload['index_sha256'];h=hashlib.sha256()
        with subprocess.Popen(_common()+['cat',payload['remote']+'/pack-index.json'],stdout=subprocess.PIPE,stderr=subprocess.DEVNULL) as child, incoming.open('xb') as stream:
            for data in iter(lambda:child.stdout.read(2**20),b''):h.update(data);stream.write(data)
            code=child.wait();stream.flush();os.fsync(stream.fileno())
        if code:raise subprocess.CalledProcessError(code,[_common()[0],'cat',payload['remote']+'/pack-index.json'])
        if h.hexdigest()!=sha:raise ValueError('Cloud restore index SHA differs')
        index=json.loads(incoming.read_text())
        if index.get('format')!=PACK_FORMAT or index.get('all_member_hashes_exact') is not True:raise ValueError('Invalid archive index')
        incoming.replace(index_path)
    for value in payload['values']:
        r=value['receipt'];parts=r['parts']+r['aggregate_parts'];names=[]
        for item in parts:
            path=Path(item['path']);name=str(path.relative_to(base))
            if not path.resolve().is_relative_to(base.resolve()) or any(p.is_symlink() for p in [path,*path.parents] if p==base or p.is_relative_to(base)):raise ValueError('Invalid result restore location')
            names.append(name)
        if index is None:
            listing=ops/(r['job_id']+'.files.txt');listing.write_text('\n'.join(names)+'\n')
            subprocess.run(_common()+['copy',payload['remote'],str(base),'--files-from',str(listing),'--checksum','--immutable','--transfers','4'],check=True,capture_output=True,timeout=86400)
        else:
            for name,item in zip(names,parts):
                path=base/name;member=_safe_member(index,name,item['sha256'])
                if path.exists():
                    if path.stat().st_size!=member['bytes'] or checksum(path)!=item['sha256']:raise ValueError('Existing restored result conflicts')
                    continue
                path.parent.mkdir(parents=True,exist_ok=True);incoming=path.with_name(path.name+'.'+uuid.uuid4().hex+'.incoming')
                _readback(payload['remote']+'/'+member['pack'],member,offset=member['offset'],count=member['bytes'],output=incoming)
                try:os.link(incoming,path)
                except FileExistsError:
                    if checksum(path)!=item['sha256']:raise ValueError('Concurrent restored result conflicts')
                incoming.unlink()
                fd=os.open(path.parent,os.O_RDONLY)
                try:os.fsync(fd)
                finally:os.close(fd)
        for item in parts:
            if checksum(item['path'])!=item['sha256']:raise ValueError('Restored result SHA differs')
        restored.append(r['job_id'])
    return dict(restored_jobs=restored,all_exact=True,archive_format=payload.get('archive_format','raw'),created_at=time.time())
