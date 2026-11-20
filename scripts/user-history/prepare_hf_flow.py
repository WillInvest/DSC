"""Freeze disjoint host assignments from the existing accepted SQLite catalog."""
import json,time
from pathlib import Path
from shared.artifacts import checksum,digest,write_json
from hf_flow_queue import FlowLedger,assignments,snapshot
from hf_f64_queue import validate

BASE=Path('/var/lib/dsc-aave-hf-f64/20261116-v2')
FLOW=Path('/var/lib/dsc-aave-hf-flow/20261116')
HOSTS={'data':'/var/lib/dsc-aave-hf-flow/20261116','node':'/home/node/hf-flow-20261116','revert':'/home/dsc-revert/hf-flow-20261116','web':'/home/fao/dsc/data/incoming/aave-hf-flow-20261116'}
CODE=['hf_flow_queue.py','hf_flow_worker.py','hf_flow_artifacts.py','hf_flow_io.py','hf_flow_json.py','consolidate_hf.py']


def prepare():
 FLOW.mkdir(parents=True,exist_ok=True);p=json.loads((BASE/'f64-plan.json').read_text());validate(p)
 gate=json.loads((BASE/'release-gates.json').read_text())
 if not gate.get('approved') or gate['generation']!=p['generation'] or gate['manifest_sha256']!=checksum(BASE/'f64-plan.json'):raise ValueError('Wrong original release approval')
 with snapshot(BASE/'tracker.sqlite') as db:rows=db.execute('select job_id,receipt,sha256 from accepted where generation=?',(p['generation'],)).fetchall()
 imported={};jobmap={j['id']:j for j in p['jobs']}
 with FlowLedger(FLOW/'flow.sqlite',p['generation']) as db:
  for key,path,sha in rows:
   if checksum(path)!=sha:raise ValueError('Existing accepted receipt changed')
   r=json.loads(Path(path).read_text())
   if r['generation']!=p['generation'] or r['job']!=jobmap[key] or not r['structural_exact'] or not r['within_error_budget']:raise ValueError('Existing acceptance mismatch')
   nodepath=str(Path(p['storage_base'])/'outputs'/p['generation']/'jobs'/key/'receipt.json')
   value=dict(receipt=r,path=nodepath,sha256=sha,certificate=None,imported_prior_acceptance=True)
   db.node_commit(key,value,role=r['role']);imported[key]=r
  with snapshot(BASE/'f64-archive.sqlite') as cloud:
   reports=dict(cloud.execute('select batch_id,report from batches'))
   groups={}
   for key,batch in cloud.execute('select job_id,batch_id from archived where generation=?',(p['generation'],)):
    if key in imported:groups.setdefault(batch,[]).append(key)
   for batch,keys in groups.items():db.cloud_commit(keys,json.loads(reports[batch]))
 distribution=assignments(p,set(imported));numeric=FLOW/'numeric-plan.json'
 if numeric.exists() and checksum(numeric)!=checksum(BASE/'f64-plan.json'):raise ValueError('Preserve prior numerical plan')
 if not numeric.exists():numeric.write_bytes((BASE/'f64-plan.json').read_bytes())
 configs={}
 for role,home in HOSTS.items():
  root=Path(home);state=root/'local';output=root/'bulk' if role=='node' else root/'local/results'
  configs[role]=dict(role=role,generation=p['generation'],numeric_plan=str(root/'numeric-plan.json'),numeric_plan_sha256=checksum(numeric),
   jobs=distribution[role],assignment_sha256=digest(distribution[role]),expected_role_jobs=len(distribution[role])+sum(r['role']==role for r in imported.values()),slots=1,production_slots=20 if role=='node' else 12,canary_limit=1,
   local_root=str(state),output_root=str(output),node_bulk=HOSTS['node']+'/bulk',node_flow_root=HOSTS['node'],scheduler=p['scheduler'],tls_private='/home/dsc-data/worker-state/private',
   local_high_water_bytes=150_000_000_000,transfer_target_bytes=125_000_000_000,cloud_target_bytes=500_000_000_000,
   reserve_bytes=50*2**30,inflight_bytes_per_slot=1_000_000_000,
   operational_code={n:checksum(Path(__file__).with_name(n)) for n in CODE})
  write_json(FLOW/('role-'+role+'.json'),configs[role])
 config=dict(generation=p['generation'],numeric_plan=str(numeric),numeric_plan_sha256=checksum(numeric),old_base=str(BASE),control_root=str(FLOW),
  node_home='/home/node',node_bulk=HOSTS['node']+'/bulk',node_flow_root=HOSTS['node'],scheduler=p['scheduler'],private=p['private'],roles=configs,
  transfer_target_bytes=125_000_000_000,cloud_target_bytes=500_000_000_000,cloud_remote='dsc_hf:26407881/hf-flow-20261116',
  created_at=time.time(),startup='Trusted accepted SQLite receipts; no historical decode or recalculation')
 write_json(FLOW/'flow-config.json',config)
 print(json.dumps(dict(imported_accepted=len(imported),imported_values=sum(r['rows'] for r in imported.values()),remaining={r:len(v) for r,v in distribution.items()},config=str(FLOW/'flow-config.json'))),flush=True)

if __name__=='__main__':prepare()
