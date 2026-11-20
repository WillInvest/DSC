#!/usr/bin/env python3
"""View-only fixed SSH report. Collect only small progress JSON files."""
from pathlib import Path
import json
import time
from datetime import timedelta

ROOT = Path('/home/dsc-data/runs/sequential-events')
AUTO = Path('/home/dsc-data/runs/event-pipeline')


def optional(path, default=None):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return default


def tracing_estimate(receipt, now):
    trace=receipt.get('traces',{})
    if not trace.get('total_blocks'): return
    remaining=max(0,trace['total_blocks']-trace.get('completed_blocks',0))
    rate=trace.get('blocks_per_second',0)
    fresh=bool(trace.get('updated_at')) and now-trace['updated_at']<=60 and not trace.get('connection_error')
    usable=fresh and rate>0 and trace.get('state') in ('tracing','waiting_receipts','complete')
    seconds=0 if remaining==0 else remaining/rate if usable else None
    trace['estimate']=dict(remaining_blocks=remaining,full_seconds=seconds,
        full_duration=str(timedelta(seconds=round(seconds))) if seconds is not None else 'unavailable',
        estimated_finish_at=now+seconds if seconds is not None else None)


def report():
    current=optional(Path('/home/dsc-data/runs/pipeline/current.json'),{})
    coordinator=optional(Path(current['root']).parents[1]/'progress.json',{}) if current else optional(Path('/home/dsc-data/runs/pipeline/progress.json'),{})
    if current and Path(current['root']).parents[1].name=='verification':coordinator['purpose']='verification'
    if current and coordinator.get('purpose')=='verification':
        verified=optional(Path(current['root']).parents[1]/'verification.json',{})
        if verified:coordinator['state']=verified['state']
        elif coordinator.get('state')=='complete':coordinator['state']='comparing'
    root=Path(current['root'])/'events' if current else ROOT
    auto=Path(current['root'])/'offline' if current else AUTO
    if not (root/'plan.json').exists():root=ROOT;auto=AUTO
    data = optional(root/'progress.json',optional(ROOT/'progress.json',{}))
    data['coordinator']=coordinator
    data['compaction']=optional(Path('/home/dsc-data/runs/pipeline/compaction.json'),{})
    data['decoding'],data['reconstruction']=[],[]
    for job in json.loads((root/'plan.json').read_text())['jobs']:
        encoded=Path(job['destination']);base=encoded.parent.parent
        for key,folder,file in [('decoding','decoded','decode-progress.json'),
                                 ('reconstruction','reconstructed','reconstruction-progress.json')]:
            progress=optional(base/folder/encoded.name/file)
            if progress:
                data[key].append({**progress,'name':job['name'],'address':job['address']})
    data['preparation']=optional(AUTO/'preparation.json',{'jobs':[]})
    data['receipt_scan']=optional(AUTO/'receipt-scan.json',{})
    tracing_estimate(data['receipt_scan'],time.time())
    delivery=Path('/home/dsc-data/data/incremental')/current['cycle']['id']/'reverted/progress.json' if current else Path('/home/dsc-data/data/reverted/drpc-v1/progress.json')
    data['trace_delivery']=optional(delivery,{})
    data['storage']=optional(AUTO/'storage.json',[])
    data['pipeline']=optional(auto/'progress.json',{'state':'waiting'})
    for job in data['decoding']:
        pipeline=data['pipeline']
        if pipeline.get('state')=='decode' and pipeline.get('current',{}).get('address')==job['address']:
            fresh=max(0,job.get('decoded_rows',0)-pipeline.get('reused_rows',0))
            job['rows_per_second']=fresh/max(1,job.get('elapsed_seconds',1))
    data['pipeline_jobs']=optional(auto/'jobs.json',{})
    oracle=optional(Path('/home/dsc-data/runs/aave-oracle-v3/current.json'),{})
    data['aave_oracle']=optional(Path(oracle['root'])/'progress.json',{}) if oracle else {}
    users=optional(Path('/home/dsc-data/runs/aave-user-history-v3/current.json'),{})
    data['aave_nominal'],data['aave_hf'],data['aave_hf_inputs']={},{},{}
    if users:
        user_root=Path(users['root'])
        for key,name in [('aave_nominal','nominal-progress.json'),('aave_hf','hf-progress.json')]:
            data[key]=optional(user_root/name,dict(state='not_started',waiting_for=['No progress report published yet']))
        data['aave_hf_inputs']=optional(user_root/'hf-input-progress.json',{})
        raw=optional(user_root/'progress.json',{})
        positions=optional(user_root/'position-jobs.json',{})
        decoded=optional(user_root/'decode-jobs.json',{})
        data['aave_users']=dict(raw_completed=raw.get('completed_addresses',0),total=raw.get('total_addresses',0),
            decoded_completed=sum(s.get('state')=='token_history_decoded' for s in decoded.values()),
            scaled_completed=sum(s.get('state')=='scaled_replay_complete' for s in positions.values()),
            failed=[dict(token=t,error=s.get('error','')) for t,s in positions.items() if s.get('state')=='failed'],
            scope=optional(user_root/'priority.json',{}).get('scope','four_asset_priority'))
    return data


if __name__=='__main__':
    print(json.dumps(report()))
