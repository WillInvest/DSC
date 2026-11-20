"""Project explicit accepted archive paths; Data's numeric plan is unchanged."""
import json
from pathlib import Path
from shared.artifacts import checksum,write_json
B=Path('/var/lib/dsc-aave-hf-f64/20261116-v2');p=json.loads((B/'f64-plan.json').read_text());V=Path('/home/dsc-data/data/aave-user-history-v3/26407881/accounting/hf-numeric');node=Path('/home/node');work=Path('aave-hf-distributed/5197dae89b140ccb/outputs');requests={'revert':{},'web':{}};files={}
for key,src in p['sources'].items():
 role=p['source_roles'][key]
 for f in src['files']:
  q=Path(f['path'])
  if q.is_relative_to(V):target=node/work/q.relative_to(V) if role=='node' else node/'hf-backups/lan-20261116'/role/work/q.relative_to(V)
  elif q.is_relative_to(node):target=q
  else:raise ValueError('Unreviewed archive source location')
  item=files.setdefault(str(target),dict(sha256=f['sha256'],source_kind=src['kind'],source_role=role,source_jobs=[]));item['source_jobs'].append(key)
 if role in requests and src['kind']=='integer_table':
  r=src['receipt'];v=Path(r['validation']);v.relative_to(V);requests[role][str(v)]=dict(sha256=r['validation_sha256'],kind='original_validation')
  sidecar=Path(src['files'][0]['path']).with_name('output.json');sidecar.relative_to(V);requests[role][str(sidecar)]=dict(sha256=None,kind='original_output_sidecar')
root=B/'consolidation'
for role,value in requests.items():write_json(root/('metadata-requests-'+role+'.json'),dict(generation=p['generation'],numeric_plan_sha256=checksum(B/'f64-plan.json'),role=role,files=value))
write_json(root/'node-source-projection.json',dict(generation=p['generation'],numeric_plan_sha256=checksum(B/'f64-plan.json'),files=files,scope='Expected preserved source archive; transfer receipts establish verified presence, not numerical coverage'))
print(json.dumps(dict(projected_files=len(files),metadata_requests={r:len(v) for r,v in requests.items()})))
