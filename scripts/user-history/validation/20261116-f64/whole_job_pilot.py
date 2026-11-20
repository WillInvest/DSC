import json,time,multiprocessing,hashlib,os
from pathlib import Path
import numpy as np
import pyarrow as pa
from shared.artifacts import write_json,checksum
import materialize_accounts as m
ROOT=Path('/home/dsc-data/runs/aave-user-history-v3/26407881');BASE=Path('/var/lib/dsc-aave-hf-f64/20261116-v2');JOB=dict(id='25200029-25200073',start=25200029,stop=25200074,expected_rows=9794116)

def run_variant(kind):
    out=BASE/'whole-job'/kind;out.mkdir(parents=True,exist_ok=True);started=time.monotonic()
    if kind=='exact':
        from vector_compute import compute
        d=json.loads((BASE/'code/exact-descriptor.json').read_text());r=compute(dict(job=JOB,generation=d['generation'],identity=d['identity']))
        groups={'user':r['parts']};info=dict(kind=kind,rows=r['rows'],compute_seconds=r['elapsed_seconds'],peak_rss_kib=r['peak_rss_kib'])
    else:
        from hf_f64_compute import compute
        cp=m.load('/var/lib/dsc-aave-hf-f64/20261116/checkpoints/before-25199900.json.receipt.json');r=compute(dict(job=JOB,checkpoint=cp));groups={'user':r['parts'],'aggregates':r['aggregate_parts']};info={k:v for k,v in r.items() if k not in ['parts','aggregate_parts']};info['kind']=kind
    files={}
    for group,parts in groups.items():
        files[group]=[]
        for i,part in enumerate(parts):
            path=out/(group+'-'+str(i)+'.parquet')
            if path.exists():raise ValueError('Refuse overwrite trial output')
            with path.open('xb') as f:f.write(part['data']);f.flush();os.fsync(f.fileno())
            files[group].append(dict(path=str(path),sha256=checksum(path),bytes=path.stat().st_size,**{k:v for k,v in part.items() if k not in ['data','sha256']}))
    info.update(files=files,wall_with_write_seconds=time.monotonic()-started,bytes=sum(x['bytes'] for rows in files.values() for x in rows));write_json(out/'report.json',info);return dict(kind=kind,rows=info['rows'],seconds=info['wall_with_write_seconds'],bytes=info['bytes'])

def main():
    for kind in ['exact','float64']:
        with multiprocessing.get_context('spawn').Pool(1,maxtasksperchild=1) as pool:r=pool.apply(run_variant,(kind,))
        print(json.dumps(r),flush=True)
    import hf_vectors as old
    from hf_f64_vectors import decode_users
    owners=m.owner_universe(ROOT,26407882);firsts=np.array([b for _,b in owners]);n=int(np.searchsorted(firsts,JOB['stop'],side='left'));fast=np.full((JOB['stop']-JOB['start'],n),np.nan);ref=fast.copy();f=m.load(BASE/'whole-job/float64/report.json');e=m.load(BASE/'whole-job/exact/report.json')
    for part in f['files']['user']:
        a,b,ids,x=decode_users(Path(part['path']).read_bytes(),firsts);fast[a-JOB['start']:b-JOB['start'],ids]=x
    for part in e['files']['user']:
        x=old.unpack(old.loads(Path(part['path']).read_bytes()));u=x['entity_id'].to_numpy();b=x['block_number'].to_numpy()-JOB['start'];values=np.fromiter((np.inf if int.from_bytes(v,'big')==2**256-1 else int.from_bytes(v,'big')/1e18 for v in x['hf_values'].to_pylist()),dtype=np.float64,count=x.num_rows);ref[b,u]=values
    existing=np.arange(JOB['start'],JOB['stop'])[:,None]>=firsts[:n][None,:]
    if np.any(np.isnan(fast[existing])) or np.any(np.isnan(ref[existing])):raise ValueError('Whole-job coverage missing')
    from native_batch import BatchEvaluator
    from hf_f64_contract import CONTRACT
    db=m.readonly(ROOT/'accounting.sqlite');ev=BatchEvaluator(ROOT,db,m.load(ROOT/'plan.json'),JOB['start'],JOB['stop']);excluded=0
    try:
        for b in range(JOB['start'],JOB['stop']):
            ev.advance(b);t=b-JOB['start'];changed=np.flatnonzero(np.isposinf(fast[t])&np.isfinite(ref[t]))
            for uid in changed:
                if int(ev.account(owners[int(uid)][0])['account'][1])>=CONTRACT['dust_debt_base_exclusive']:raise ValueError('Non-dust borrower omitted')
                ref[t,uid]=np.inf;excluded+=1
    finally:ev.close();db.close()
    if not np.array_equal(np.isposinf(fast[existing]),np.isposinf(ref[existing])):raise ValueError('Policy-adjusted no-debt classifications differ')
    from hf_f64_accept import audit
    structural=dict(f,parts=f['files']['user'],aggregate_parts=f['files']['aggregates']);audit(structural,owners)
    valid=existing&np.isfinite(ref)&np.isfinite(fast);delta=np.abs(fast[valid]-ref[valid]);relative=delta/np.maximum(np.abs(ref[valid]),1e-300)
    report=dict(job=JOB,rows=int(np.count_nonzero(existing)),all_user_block_coverage_exact=True,all_no_debt_classifications_exact=True,hf_max_absolute_error=float(np.max(delta,initial=0)),hf_max_relative_error=float(np.max(relative,initial=0)),hf_p99_absolute_error=float(np.quantile(delta,.99)),same_host_same_cpu_limit=True,exact_seconds=e['wall_with_write_seconds'],float64_seconds=f['wall_with_write_seconds'],speedup=e['wall_with_write_seconds']/f['wall_with_write_seconds'],exact_bytes=e['bytes'],float64_user_and_aggregate_bytes=f['bytes'],new_node_rpc=0,production_coverage=False,dust_threshold_usd=CONTRACT['dust_threshold_usd'],excluded_user_blocks_independently_checked=excluded)
    if not np.all(delta<=1e-8+1e-6*np.abs(ref[valid])):raise ValueError('User error budget exceeded')
    if report['speedup']<1.5:raise ValueError('Complete-job speedup below deployment gate')
    write_json(BASE/'evidence/whole-job-comparison.json',report);print(json.dumps(report),flush=True)
if __name__=='__main__':main()
