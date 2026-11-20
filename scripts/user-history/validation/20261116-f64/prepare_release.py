import json,sqlite3,time
from pathlib import Path
from shared.artifacts import checksum,write_json
import materialize_accounts as m
B=Path('/var/lib/dsc-aave-hf-f64/20261116-v2');p=m.load(B/'f64-plan.json');gen=p['generation'];paths={k:B/'evidence'/n for k,n in [('profiles','all-profiles.json'),('population','manifest-population-and-memory.json'),('compute_benchmark','whole-job-comparison.json'),('pipeline_benchmark','complete-transport-comparison.json'),('cloud_restore','cross-machine-cloud-restore.json')]};values={k:m.load(v) for k,v in paths.items()}
if not values['profiles']['all_passed'] or values['profiles']['profiles']!=12:raise ValueError('Historical version gate failed')
if not values['population']['every_job_population_exact'] or values['population']['all_logical_values']!=910310934885:raise ValueError('Population gate failed')
if values['pipeline_benchmark']['speedup']<1.5 or not values['cloud_restore']['all_hashes_and_readers_passed']:raise ValueError('Endpoint/restore gate failed')
for name,sha in p['identity']['sources'].items():
 if checksum(B/'code'/name)!=sha:raise ValueError('Current code differs: '+name)
 if name!='hf_f64_queue.py' and checksum(B/'code-b7446373'/name)!=sha:raise ValueError('Numerical evidence source differs: '+name)
audit=B/'audits/audit-1794861172195087242.json';a=m.load(audit)
if not a['within_error_budget'] or not a['structural_exact'] or a['extra_node_rpc']+115>200:raise ValueError('Fresh node audit gate failed')
errors=[]
for c in a['checks']:
 if c.get('stored_hf')!='+inf' and 'stored_hf' in c:
  actual=float(c['stored_hf']);ref=int(c['node_account'][5])/1e18;errors.append(abs(actual-ref))
error_summary=dict(source_audit=str(audit),source_sha256=checksum(audit),fresh_rpc_already_counted=a['extra_node_rpc'],additional_rpc=0,max_absolute_error=max(errors,default=0),finite_samples=len(errors));write_json(B/'evidence/first-audit-error-summary.json',error_summary)
with sqlite3.connect('file:'+str(B/'tracker.sqlite')+'?mode=ro',uri=True) as db:rows=db.execute('select job_id,receipt,sha256 from accepted where generation=?',(gen,)).fetchall()
receipts=[]
for key,path,sha in rows:
 if checksum(path)!=sha:raise ValueError('Accepted Data receipt changed')
 r=m.load(path)
 if not r['structural_exact'] or not r['within_error_budget'] or r['generation']!=gen:raise ValueError('Invalid accepted canary')
 receipts.append(r)
if len(receipts)!=6 or {r['role'] for r in receipts}!={'data','node','revert','web'} or {r['source_kind'] for r in receipts}!={'integer_cast','float64_batch'}:raise ValueError('Incomplete four-host/source canary')
with sqlite3.connect('file:'+str(B/'f64-archive.sqlite')+'?mode=ro',uri=True) as db:cloud=json.loads(db.execute('select report from batches order by rowid desc limit 1').fetchone()[0])
if not all(c['all_remote_size_md5_exact'] and len(c['restore_probes'])==3 for c in cloud['chunks']):raise ValueError('Cloud copy gate failed')
r=dict(generation=gen,manifest_sha256=checksum(B/'f64-plan.json'),approved=False,waiting_for='Verified immutable control backup',all_data_gates_passed=True,dust_threshold_usd='0.01',current_canary_jobs=len(receipts),current_canary_user_values=sum(x['rows'] for x in receipts),current_canary_bytes=sum(x['bytes'] for x in receipts),current_canary_node_getter_samples=sum(x['validated_samples'] for x in receipts),scope_user_values=910310934885,not_all_history_complete=True,pipeline_speedup=values['pipeline_benchmark']['speedup'],evidence={k:dict(path=str(v),sha256=checksum(v)) for k,v in paths.items()},fresh_audit=dict(path=str(audit),sha256=checksum(audit),extra_rpc=51,earlier_extra_rpc=115,total_extra_rpc_this_cycle=166),cloud_pilot_verified_bytes=cloud['verified_bytes'],tests=dict(count=32,passed=True,observed_journal_unit='dsc-hf-f64-v2-final-tests.service'),code_review=dict(status='complete',run_id='20261116-145340-8f30644e',pre_fix_verdict='Not ready',all_three_actionable_findings_fixed_and_tested=True),created_at=time.time())
write_json(B/'release-candidate.json',r);print(json.dumps(r),flush=True)
