"""Bounded float64 user vectors and synchronized debt-weighted aggregate proofs."""
import bisect,hashlib,resource,time,os,multiprocessing
from pathlib import Path
import numpy as np
from shared.artifacts import checksum,digest
import materialize_accounts as m
from native_batch import BatchEvaluator
from hf_batch_f64 import FloatBatch
from hf_aggregates import aggregate
from hf_f64_vectors import encode_users,decode_users,encode_aggregates
from hf_f64_contract import REGISTRY,FACTOR,IMAGE,CONTRACT
from hf_replay_checkpoint import restore
ROOT=Path('/home/dsc-data/runs/aave-user-history-v3/26407881')


def compute(payload):
    began=time.monotonic();job=payload['job'];plan=m.load(ROOT/'plan.json');factor,waiting=m.factor_input(ROOT,plan,FACTOR)
    if 'identity' in payload:
        if digest(payload['identity'])!=payload['generation'] or payload['identity']['contract']!=CONTRACT:raise ValueError('Task generation/contract differs')
        for name,sha in payload['identity']['sources'].items():
            if checksum(Path(__file__).with_name(name))!=sha:raise ValueError('Task source identity differs: '+name)
    if waiting:raise ValueError(waiting)
    if checksum('/home/dsc-data/snapshot-manifest.json')!=IMAGE:raise ValueError('Wrong frozen input')
    owners=m.owner_universe(ROOT,26407882)
    if digest(owners)!=REGISTRY:raise ValueError('User registry changed')
    model=FloatBatch(owners,plan['jobs']);db=m.readonly(ROOT/'accounting.sqlite');checkpoint=payload.get('checkpoint')
    ev=restore(ROOT,db,plan,job['start'],job['stop'],checkpoint['path'],checkpoint['sha256']) if checkpoint else BatchEvaluator(ROOT,db,plan,job['start'],job['stop'])
    initialized=time.monotonic();parts=[];proofs=[];rows=0;samples=[];fallback=0;advance_seconds=arithmetic_seconds=encoding_seconds=0.0
    checkpoints={job['start'],(job['start']+job['stop']-1)//2,job['stop']-1};nmax=bisect.bisect_left([b for _,b in owners],job['stop']);span=min(4096,max(1,32_000_000//max(nmax,1)))
    try:
        reused=None
        if payload.get('hf_source'):
            if (job['stop']-job['start'])*nmax>32_000_000:raise ValueError('Retained-source matrix exceeds bounded job budget')
            from hf_f64_sources import read_source
            reused=read_source(payload['hf_source'],owners,job['start'],job['stop'])
        for start in range(job['start'],job['stop'],span):
            stop=min(job['stop'],start+span);n=int(np.searchsorted(model.firsts,stop,side='left'));matrix=np.full((stop-start,n),np.nan);ns=[];ds=[];hs=[];excluded_users=[];excluded_debt=[]
            for block in range(start,stop):
                at=time.monotonic();ev.advance(block);advance_seconds+=time.monotonic()-at;at=time.monotonic()
                nblock=int(np.searchsorted(model.firsts,block,side='right'))
                value=model.evaluate(ev,None if reused is None else reused[block-job['start'],:nblock],dust_cutoff=CONTRACT['dust_debt_base_exclusive']);a=aggregate(value['hf'],value['debt_user_ids'],value['debt_reserve_ids'],value['debt_values'],len(model.reserves));fallback+=value['exact_fallback_users']
                excluded_users.append(value['excluded_users']);excluded_debt.append(value['excluded_debt_base'])
                matrix[block-start,:nblock]=value['hf'];rows+=nblock
                ns.append(np.append(a['numerator'],a['protocol_numerator']));ds.append(np.append(a['denominator'],a['protocol_denominator']));hs.append(np.append(a['hf'],a['protocol_hf'] or 0.0))
                arithmetic_seconds+=time.monotonic()-at
                if block in checkpoints:
                    for debt_flag in [True,False]:
                        matches=np.flatnonzero((value['debt']>0)==debt_flag)
                        if len(matches):
                            uid=int(matches[0]);reference=ev.account(owners[uid][0]);h=float(value['hf'][uid]);samples.append(dict(user=owners[uid][0],entity_id=uid,block=block,profile=ev.profile,block_hash=ev.block_hash,exact_reference=reference['account'],hf=h if np.isfinite(h) else '+inf',has_debt=debt_flag))
            at=time.monotonic();width=max(1,1_000_000//(stop-start))
            for uid in range(0,n,width):
                end=min(n,uid+width);raw=encode_users(matrix[:,uid:end],start,model.firsts[uid:end],wallet_start=uid)
                lo,hi,ids,decoded=decode_users(raw,model.firsts)
                expected=matrix[:,ids];valid=~np.isnan(expected)
                if not np.array_equal(decoded[valid].view(np.uint64),expected[valid].view(np.uint64)):raise ValueError('Float64 disk roundtrip changed bits')
                parts.append(dict(data=raw,sha256=hashlib.sha256(raw).hexdigest(),start=start,stop=stop,wallet_start=uid,wallet_stop=end,values=int(np.count_nonzero(valid))))
            proof=encode_aggregates(np.arange(start,stop),np.asarray(hs),np.asarray(ns),np.asarray(ds),np.arange(len(model.reserves)+1),excluded_users,excluded_debt);proofs.append(dict(data=proof,sha256=hashlib.sha256(proof).hexdigest(),start=start,stop=stop))
            encoding_seconds+=time.monotonic()-at
        if rows!=m.row_count(owners,job['start'],job['stop']):raise ValueError('Logical user-block coverage differs')
        return dict(job=job,format='hf-vector-f64-v2',dust_cutoff=CONTRACT['dust_debt_base_exclusive'],rows=rows,parts=parts,aggregate_parts=proofs,samples=samples,reserves=model.reserves,registry_digest=REGISTRY,factor_input=factor,exact_fallback_user_blocks=fallback,source_kind='integer_cast' if payload.get('hf_source') else 'float64_batch',retained_source_sha256=digest(payload['hf_source']) if payload.get('hf_source') else None,timing=dict(init_seconds=initialized-began,advance_seconds=advance_seconds,arithmetic_seconds=arithmetic_seconds,encoding_roundtrip_seconds=encoding_seconds,total_seconds=time.monotonic()-began),peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,bytes=sum(len(p['data']) for p in parts+proofs),production_coverage=False)
    finally:ev.close();db.close()


def task(payload):
    from distributed import get_worker
    payload=dict(payload);role=os.environ['DSC_HF_ROLE']
    candidates=[p for p in payload.pop('checkpoints',[]) if p['role']==role and p['block']<=payload['job']['start']]
    if candidates:payload['checkpoint']=max(candidates,key=lambda p:p['block'])
    with multiprocessing.get_context('spawn').Pool(1,maxtasksperchild=1) as pool:result=pool.apply(compute,(payload,))
    result.update(identity=payload['identity'],generation=payload['generation'],role=role,worker=get_worker().address,checkpoint_used=payload.get('checkpoint'))
    return result
