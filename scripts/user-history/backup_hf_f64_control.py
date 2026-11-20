"""Verified immutable control snapshots on node; Data remains the authority."""
import argparse,hashlib,json,tempfile,time
from pathlib import Path
from distributed import Client
from shared.artifacts import checksum,write_json
from distributed_hf import backup_sqlite
from vector_coordinator import security
from vector_archive import registry_source
import materialize_accounts as m

def run(base):
    base=Path(base);p=m.load(base/'f64-plan.json');stamp=str(time.time_ns());prefix='recovery/'+stamp+'/'
    files={prefix+n:(base/n).read_bytes() for n in ['f64-plan.json','dataset.json','users.parquet']}
    files[prefix+'source-manifest.json']=Path(p['source_manifest']).read_bytes()
    for name in ['release-candidate.json','release-gates.json']:
        if (base/name).exists():files[prefix+name]=(base/name).read_bytes()
    direct=base/'direct-archive-config.json'
    if direct.exists():
        files[prefix+'direct-archive-config.json']=direct.read_bytes()
        for name,sha in m.load(direct)['code_sha256'].items():
            path=base/'direct-archive-code'/name
            path.resolve().relative_to((base/'direct-archive-code').resolve())
            if checksum(path)!=sha:raise ValueError('Direct archive source differs')
            files[prefix+'direct-archive-code/'+name]=path.read_bytes()
        for name in ['archive-controller-override.conf','dsc-aave-v3-hf-f64-direct-archive-worker.service']:
            path=base/'direct-archive-code'/name
            files[prefix+'direct-archive-code/'+name]=path.read_bytes()
    names=set(p['identity']['sources'])|set(p['archive_sources'])|{'audit_hf_f64.py','backup_hf_f64_control.py','require_hf_f64_release.py'}
    for name in sorted(names):files[prefix+'code/'+name]=(base/'code'/name).read_bytes()
    index=base/'deployment/deployment-index.json'
    if index.exists():
        files[prefix+'deployment/deployment-index.json']=index.read_bytes()
        for name,sha in m.load(index).items():
            path=base/'deployment'/name;path.resolve().relative_to((base/'deployment').resolve())
            if checksum(path)!=sha:raise ValueError('Deployment configuration differs')
            files[prefix+'deployment/'+name]=path.read_bytes()
    with tempfile.TemporaryDirectory(prefix='control-snapshot-',dir=base) as folder:
        snapshot=Path(folder)/'catalog.sqlite';backup_sqlite(base/'tracker.sqlite',snapshot);files[prefix+'catalog.sqlite']=snapshot.read_bytes()
    manifest=dict(generation=p['generation'],created_at=time.time(),source_base=str(base),credentials_included=False,files={n:dict(sha256=hashlib.sha256(v).hexdigest(),bytes=len(v)) for n,v in files.items()})
    files[prefix+'manifest.json']=json.dumps(manifest,sort_keys=True).encode()
    with Client(p['scheduler'],security=security(p['private']),set_as_default=False) as client:
        workers=[a for a,w in client.scheduler_info(n_workers=-1)['workers'].items() if str(w['name']).startswith('f64-accept')]
        if not workers:raise ValueError('Node metadata destination unavailable')
        scattered=client.scatter(list(files.values()),workers=workers,direct=True,broadcast=False,hash=False,timeout=120)
        try:stored=client.submit(registry_source,dict(zip(files,scattered)),p['storage_base'],pure=False,resources={'f64_accept':1}).result(timeout=180)
        finally:
            for future in scattered:future.release()
    for name,data in files.items():
        if stored['metadata/'+name]!=dict(sha256=hashlib.sha256(data).hexdigest(),bytes=len(data)):raise ValueError('Node control backup differs')
    report=dict(generation=p['generation'],destination=p['storage_base']+'/metadata/'+prefix,files=len(files),bytes=sum(len(v) for v in files.values()),all_hashes_exact=True,active_sqlite_copied=False,snapshot_api='sqlite3.Connection.backup',data_remains_authority=True,created_at=time.time())
    out=base/'evidence'/('control-backup-'+stamp+'.json');write_json(out,report);print(json.dumps(dict(report=str(out),sha256=checksum(out),**report)),flush=True);return report
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--base',required=True);a=p.parse_args();run(a.base)
