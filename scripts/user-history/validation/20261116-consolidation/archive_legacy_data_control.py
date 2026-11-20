"""Copy closed integer-generation controls via existing snapshot/TLS APIs."""
import json,sqlite3,tempfile,time,hashlib
from pathlib import Path
from distributed import Client
from distributed_hf import backup_sqlite
from vector_coordinator import security
from vector_archive import registry_source
from shared.artifacts import checksum,write_json
B=Path('/var/lib/dsc-aave-hf-f64/20261116-v2');old=Path('/var/lib/dsc-aave-hf-vector/20261116');current=json.loads((B/'f64-plan.json').read_text());descriptor=json.loads((old/'vector-plan.json').read_text());prefix='legacy-integer-control/'+str(time.time_ns())+'/';files={}
for name in ['vector-plan.json','dataset.json','users.parquet','retained-web-location-bootstrap.json','PAUSED_BY_USER']:
 p=old/name
 if p.exists():files[prefix+name]=p.read_bytes()
for name,sha in descriptor['identity']['sources'].items():
 p=old/'code'/name;p.resolve().relative_to((old/'code').resolve())
 if checksum(p)!=sha:raise ValueError('Legacy source differs')
 files[prefix+'code/'+name]=p.read_bytes()
with tempfile.TemporaryDirectory(dir=B) as folder:
 snap=Path(folder)/'legacy.sqlite';backup_sqlite(old/'tracker.sqlite',snap)
 with sqlite3.connect('file:'+str(snap)+'?mode=ro',uri=True) as db:rows=db.execute('select receipt,sha256 from accepted where generation=?',(descriptor['generation'],)).fetchall()
 for path,sha in rows:
  p=Path(path);relative=p.resolve().relative_to(old.resolve())
  if checksum(p)!=sha:raise ValueError('Legacy accepted receipt differs')
  files[prefix+str(relative)]=p.read_bytes()
 files[prefix+'tracker.snapshot.sqlite']=snap.read_bytes()
with Client(current['scheduler'],security=security(current['private']),set_as_default=False) as c:
 workers=[a for a,w in c.scheduler_info(n_workers=-1)['workers'].items() if str(w['name']).startswith('f64-accept')]
 if not workers:raise ValueError('Node archive destination unavailable')
 blobs=c.scatter(list(files.values()),workers=workers,direct=True,broadcast=False,hash=False,timeout=120)
 try:stored=c.submit(registry_source,dict(zip(files,blobs)),current['storage_base'],pure=False,resources={'f64_accept':1}).result(timeout=300)
 finally:
  for f in blobs:f.release()
for name,data in files.items():
 if stored['metadata/'+name]!=dict(bytes=len(data),sha256=hashlib.sha256(data).hexdigest()):raise ValueError('Node legacy control readback differs')
report=dict(created_at=time.time(),generation=descriptor['generation'],files=len(files),bytes=sum(len(v) for v in files.values()),accepted_legacy_receipts=len(rows),node_prefix=current['storage_base']+'/metadata/'+prefix,all_hashes_exact=True,no_private_keys_included=True,data_remains_authority=True,original_pause_preserved=(old/'PAUSED_BY_USER').exists())
write_json(B/'consolidation/legacy-data-control-node-copy.json',report);print(json.dumps(report),flush=True)
