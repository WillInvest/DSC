"""Select explicit frozen source files absent from node's verified archive."""
import json,datetime,time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from shared.artifacts import checksum,write_json
B=Path('/home/node/hf-centralized/20261116');inventory=B/'frozen-source-inventory.json';source=json.loads(inventory.read_text())
VIRTUAL=Path('/home/dsc-data/data/aave-user-history-v3/26407881/accounting/hf-numeric');homes={'revert':Path('/home/dsc-revert'),'web':Path('/home/fao')}
for role,home in homes.items():
 root=Path('/home/node/hf-backups/lan-20261116')/role;prior=json.loads((root/'backup-verified.json').read_text());verified=datetime.datetime.fromisoformat(prior['verified_at']).timestamp();known={}
 for stamp in ['20261116T0030Z','20261116T042153Z','20261116T081725Z','20261116T122528Z']:
  p=root/'hf-failover-20261115/backup-checkpoints'/stamp/'manifest.json'
  if p.exists() and checksum(p)==prior['manifest_sha256'] and prior['all_exact']:known=json.loads(p.read_text())['files']
 if not known:raise ValueError('Prior backup receipt has no matching manifest')
 missing={};reused=hashed=0;checks=[]
 def check(item):
  name=str(Path('aave-hf-distributed/5197dae89b140ccb/outputs')/Path(item['path']).relative_to(VIRTUAL));p=root/name;p.resolve().relative_to(root.resolve())
  meta=dict(bytes=item['bytes'],sha256=item['sha256'])
  if not p.exists():return name,meta,'missing'
  s=p.stat()
  if p.is_symlink() or not p.is_file() or s.st_size!=meta['bytes']:raise ValueError('Existing archive conflicts')
  old=known.get(name)
  if old==meta and s.st_ctime<=verified:kind='prior_verified_content_and_unchanged_ctime'
  else:
   if checksum(p)!=meta['sha256']:raise ValueError('Existing archive hash conflicts')
   kind='fresh_sha256'
  return name,meta,kind
 selected=[x for x in source['files'] if x['role']==role]
 with ThreadPoolExecutor(max_workers=4) as pool:
  for index,(name,meta,kind) in enumerate(pool.map(check,selected),1):
   if kind=='missing':missing[name]=meta
   else:
    reused+=kind=='prior_verified_content_and_unchanged_ctime';hashed+=kind=='fresh_sha256';checks.append(dict(path=name,sha256=meta['sha256'],kind=kind))
   if index%100==0:write_json(B/'selection-progress.json',dict(role=role,checked=index,total=len(selected),missing=len(missing),updated_at=time.time()))
 result=dict(role=role,inventory_sha256=checksum(inventory),numeric_generation=source['generation'],files=missing,verified_existing=checks,prior_verified_files=reused,freshly_hashed_existing=hashed,prior_backup_receipt=prior,created_at=time.time(),scope='Explicit frozen accepted HF source files; no numerical coverage advancement')
 out=B/('missing-'+role+'.json')
 if out.exists():raise ValueError('Preserve selection evidence')
 write_json(out,result);print(json.dumps(dict(path=str(out),sha256=checksum(out),files=len(missing),bytes=sum(x['bytes'] for x in missing.values()),prior_verified_files=reused,freshly_hashed_existing=hashed)),flush=True)
