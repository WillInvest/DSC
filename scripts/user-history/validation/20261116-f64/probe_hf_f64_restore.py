import hashlib,json,sqlite3,subprocess
from pathlib import Path
from distributed import Client
from shared.artifacts import checksum,write_json
from vector_coordinator import security
import materialize_accounts as m

def download_cloud(chunk):
 from vector_archive import RCLONE,CONFIG
 from hf_f64_archive import ROOT_ID
 result=[]
 for item in chunk['restore_probes']:
  raw=subprocess.run([RCLONE,'--config',CONFIG,'--drive-root-folder-id',ROOT_ID,'cat',chunk['remote']+'/'+item['path']],check=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE).stdout
  if len(raw)>32_000_000 or hashlib.sha256(raw).hexdigest()!=item['sha256']:raise ValueError('Cloud restore differs')
  result.append(dict(path=item['path'],sha256=item['sha256'],data=raw))
 return result

def verify_restored(files,base):
 import numpy as np
 import pyarrow as pa
 import pyarrow.parquet as pq
 from vector_compute import universe
 from hf_f64_vectors import decode_users,ORIGIN
 dictionary=next(v for v in files if v['path'].endswith('/users.parquet'));t=pq.read_table(pa.BufferReader(dictionary['data']));owners=universe();first=[b for _,b in owners]
 if t['wallet_id'].to_pylist()!=list(range(len(owners))) or t['address'].to_pylist()!=[a for a,_ in owners] or t['first_block_index'].to_pylist()!=[b-ORIGIN for _,b in owners]:raise ValueError('Restored dictionary identity differs')
 checked=[]
 for v in files:
  p=Path(base)/v['path'];p.resolve().relative_to(Path(base).resolve())
  if checksum(p)!=v['sha256'] or hashlib.sha256(v['data']).hexdigest()!=v['sha256']:raise ValueError('Restored bytes differ from accepted source')
  if '/parts-' in v['path']:
   a,b,ids,x=decode_users(v['data'],first);kind='user';rows=int(np.count_nonzero(~np.isnan(x)))
  else:
   table=pq.read_table(pa.BufferReader(v['data']));kind='dictionary' if v is dictionary else 'aggregate';rows=table.num_rows
  checked.append(dict(path=v['path'],sha256=v['sha256'],kind=kind,logical_or_physical_rows=rows,exact=True))
 return dict(restored_on='dsc-node',downloaded_on='dsc-compute',credentials_remained_on_compute=True,all_hashes_and_readers_passed=True,files=checked)

if __name__=='__main__':
 import probe_hf_f64_restore as canonical
 B=Path('/var/lib/dsc-aave-hf-f64/20261116-v2');p=m.load(B/'f64-plan.json')
 with sqlite3.connect('file:'+str(B/'f64-archive.sqlite')+'?mode=ro',uri=True) as db:report=json.loads(db.execute('select report from batches order by rowid desc limit 1').fetchone()[0])
 with Client(p['scheduler'],security=security(p['private']),set_as_default=False) as c:
  downloaded=c.submit(canonical.download_cloud,report['chunks'][0],pure=False,resources={'f64_archive':1});verified=c.submit(canonical.verify_restored,downloaded,p['storage_base'],pure=False,resources={'f64_accept':1});r=verified.result(timeout=240);write_json(B/'evidence/cross-machine-cloud-restore.json',r);print(json.dumps(r),flush=True);downloaded.release();verified.release()
