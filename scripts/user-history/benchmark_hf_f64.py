"""Bounded real-history characterization; never publishes production coverage."""
import argparse,json,time,resource
from pathlib import Path
import numpy as np
from shared.artifacts import write_json
import materialize_accounts as m
from native_batch import BatchEvaluator
from native_hf import MAX
from hf_batch_f64 import FloatBatch,exact_debts
from hf_aggregates import aggregate
from hf_f64_contract import CONTRACT
ROOT=Path('/home/dsc-data/runs/aave-user-history-v3/26407881')

def probe(block):
    started=time.monotonic();db=m.readonly(ROOT/'accounting.sqlite');plan=m.load(ROOT/'plan.json');owners=m.owner_universe(ROOT,26407882)
    model=FloatBatch(owners,plan['jobs']);ev=BatchEvaluator(ROOT,db,plan,block,block+1);ev.advance(block);loaded=time.monotonic()
    fast=model.evaluate(ev,dust_cutoff=CONTRACT['dust_debt_base_exclusive']);fast_a=aggregate(fast['hf'],fast['debt_user_ids'],fast['debt_reserve_ids'],fast['debt_values'],len(model.reserves));computed=time.monotonic()
    reference=[];uids=[];rids=[];weights=[];samples=[]
    for uid,(owner,first) in enumerate(owners):
        if first>block:break
        value=ev.account(owner);a=value['account'];h=int(a[5]);included=int(a[1])>=CONTRACT['dust_debt_base_exclusive'];reference.append(np.inf if not included or h==MAX else float(h)/1e18)
        if included:
            edges=exact_debts(ev,owner,model.reserve_ids)
            for reserve,value_d in edges:uids.append(uid);rids.append(reserve);weights.append(value_d)
            if len(samples)<3:samples.append(dict(user=owner,block=block,profile=value['profile'],block_hash=value['block_hash'],account=a,fast_hf=float(fast['hf'][uid])))
    exact=np.asarray(reference);ref_a=aggregate(exact,np.asarray(uids),np.asarray(rids),np.asarray(weights),len(model.reserves));ended=time.monotonic()
    finite=np.isfinite(exact)&np.isfinite(fast['hf']);diff=np.abs(exact[finite]-fast['hf'][finite]);relative=diff/np.maximum(np.abs(exact[finite]),1e-300)
    numeric=np.array([fast_a['protocol_hf'] or 0,*fast_a['hf']]);ref_numeric=np.array([ref_a['protocol_hf'] or 0,*ref_a['hf']]);agg_exact=np.allclose(numeric,ref_numeric,atol=1e-8,rtol=1e-6)
    denominators=np.allclose(fast_a['denominator'],ref_a['denominator'],atol=1e-10,rtol=1e-10);sentinels=np.array_equal(np.isposinf(exact),np.isposinf(fast['hf']))
    result=dict(block=block,profile=ev.profile,users=len(exact),init_seconds=loaded-started,fast_seconds=computed-loaded,reference_seconds=ended-computed,wall_seconds=ended-started,peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,exact_fallback_users=fast['exact_fallback_users'],hf_max_absolute_error=float(np.max(diff,initial=0)),hf_max_relative_error=float(np.max(relative,initial=0)),hf_p99_absolute_error=float(np.quantile(diff,.99)) if len(diff) else 0,all_aggregate_within_budget=bool(agg_exact),all_denominators_within_budget=bool(denominators),all_no_debt_classifications_exact=bool(sentinels),protocol_fast=fast_a['protocol_hf'],protocol_reference=ref_a['protocol_hf'],reserve_fast=fast_a['hf'].tolist(),reserve_reference=ref_a['hf'].tolist(),reserve_debt_fast=fast_a['denominator'].tolist(),reserve_debt_reference=ref_a['denominator'].tolist(),samples=samples,new_node_rpc=0,production_coverage=False)
    result.update(dust_threshold_usd=CONTRACT['dust_threshold_usd'],excluded_users=fast['excluded_users'],excluded_debt_base=fast['excluded_debt_base'],all_user_hf_within_budget=bool(np.all(diff<=1e-8+1e-6*np.abs(exact[finite]))))
    ev.close();db.close();return result

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--block',type=int,required=True);p.add_argument('--output',required=True);a=p.parse_args();r=probe(a.block);write_json(a.output,r);print(json.dumps({k:v for k,v in r.items() if not k.startswith('reserve_') and k!='samples'}),flush=True)
    if not all(r[k] for k in ['all_aggregate_within_budget','all_denominators_within_budget','all_no_debt_classifications_exact']):raise SystemExit('Historical trial failed; do not deploy')
