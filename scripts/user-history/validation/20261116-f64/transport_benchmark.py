import json,time,sqlite3
from pathlib import Path
from distributed import Client
from shared.artifacts import checksum,write_json
from vector_coordinator import security
from vector_compute import task as exact_task
from vector_tasks import accept_remote as exact_accept
from hf_f64_compute import task as float_task
from hf_f64_tasks import accept_remote as float_accept
import materialize_accounts as m
B=Path('/var/lib/dsc-aave-hf-f64/20261116-v2');JOB=dict(id='25200029-25200073',start=25200029,stop=25200074,expected_rows=9794116)

def main():
 p=m.load(B/'f64-plan.json');old=m.load(B/'code/exact-descriptor.json');c=Client(p['scheduler'],security=security(p['private']),set_as_default=False);db=sqlite3.connect(B/'evidence/transport-benchmark.sqlite');db.execute('create table if not exists accepted(kind text primary key,receipt text,sha256 text,seconds real)');db.commit();reports=[]
 try:
  info=c.scheduler_info(n_workers=-1)['workers'];data=sorted(a for a,w in info.items() if str(w['name']).startswith('f64-data'))
  if not data:raise ValueError('No Data compute worker')
  for kind,fn,accept,identity,generation in [('exact',exact_task,exact_accept,old['identity'],old['generation']),('float64',float_task,float_accept,p['identity'],p['generation'])]:
   if db.execute('select 1 from accepted where kind=?',(kind,)).fetchone():raise ValueError('Refuse repeated benchmark overwriting')
   payload=dict(job=JOB,identity=identity,generation=generation)
   if kind=='float64':payload['checkpoints']=p['checkpoints']
   start=time.monotonic();computed=c.submit(fn,payload,pure=False,workers=[data[0]],resources={'f64_compute':1});target=p['storage_base']+'/benchmarks/complete-transport/'+kind+'/'+JOB['id'];future=c.submit(accept,computed,target,pure=False,resources={'f64_accept':1});value=future.result(timeout=600);r=value['receipt'];path=B/'evidence'/('transport-'+kind+'-receipt.json');write_json(path,r)
   if checksum(path)!=value['sha256']:raise ValueError('Data/node benchmark receipt differs')
   elapsed=time.monotonic()-start;db.execute('insert into accepted values(?,?,?,?)',(kind,str(path),value['sha256'],elapsed));db.commit();report=dict(kind=kind,seconds_with_transfer_write_node_validation_and_data_commit=time.monotonic()-start,rows=r['rows'],bytes=r['bytes'],node_getter_samples=r['validated_samples']);reports.append(report);print(json.dumps(report),flush=True);computed.release();future.release()
  result=dict(same_host_same_cpu_budget=True,job=JOB,variants=reports,speedup=reports[0]['seconds_with_transfer_write_node_validation_and_data_commit']/reports[1]['seconds_with_transfer_write_node_validation_and_data_commit'],production_coverage=False,normal_acceptance_rpc_separate_from_extra_audit=True)
  if result['speedup']<1.5:raise ValueError('Full pipeline speedup below deployment gate')
  write_json(B/'evidence/complete-transport-comparison.json',result);print(json.dumps(result),flush=True)
 finally:c.close();db.close()
if __name__=='__main__':main()
