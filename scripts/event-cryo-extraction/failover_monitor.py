"""Combine fixed, read-only HF progress endpoints from an immutable assignment."""
import copy,json,time,urllib.request
from concurrent.futures import ThreadPoolExecutor

TERMINAL_STATES=('paused_by_user','complete','canary_complete')


def with_recovery_notice(report,notice):
    """Attach dated milestones without changing authority or activity counters."""
    if (not isinstance(notice,dict) or notice.get('generation')!=report.get('generation')
            or not isinstance(notice.get('recorded_at'),(int,float))
            or not isinstance(notice.get('milestones'),list)
            or not all(isinstance(item,str) for item in notice['milestones'])):
        return report
    result=dict(report)
    result['recovery_notice']=dict(recorded_at=notice['recorded_at'],milestones=notice['milestones'][:20])
    return result


def age_cached_report(value,now=None):
    """Preserve last accepted totals, but never present expired activity as live."""
    current=time.time() if now is None else now
    if not isinstance(value,dict) or value.get('state') in TERMINAL_STATES or current-(value.get('updated_at') or 0)<=90:
        return value
    result=copy.deepcopy(value)
    result.update(state='waiting_for_inputs',activity_unavailable=True,
                  reported_active_compute_processes=value.get('active_compute_processes'),active_compute_processes=None,
                  error='; '.join(x for x in [value.get('error'),'HF reports are stale; current activity is unknown'] if x))
    stale=set()
    for role,flow in result.get('host_flow',{}).items():
        if flow.get('local_producer_state',flow.get('state')) not in TERMINAL_STATES and current-(flow.get('local_producer_updated_at') or 0)>90:
            stale.add(role);flow['producer_stale']=True
            flow['reported_active_compute_processes']=flow.get('active_compute_processes');flow['active_compute_processes']=None
    for role in stale:
        result.get('distributed',{}).get('online_hosts',{}).update({role:0})
        result.get('distributed',{}).get('active_by_host',{}).update({role:None})
    return result


def aggregate(reports,config):
    valid={};errors=[]
    for role,value in reports.items():
        if isinstance(value,str):errors.append(role+': '+value);continue
        if value.get('role')!=role or value.get('generation')!=config['generation'] or value.get('manifest_sha256')!=config['manifest_sha256']:
            errors.append(role+': wrong execution identity');continue
        valid[role]=value
    if not valid:return None,'; '.join(errors)
    current=time.time();states=[v['state'] for v in valid.values()]
    independent=config.get('storage_role')=='local_then_node_then_drive' or any(v.get('storage_role')=='local_then_node_then_drive' for v in valid.values())
    terminal=TERMINAL_STATES
    producer_stale={}
    state='complete' if len(valid)==len(config['sources']) and all(s=='complete' for s in states) else 'canary_complete' if all(s=='canary_complete' for s in states) else 'paused_by_user' if any(s=='paused_by_user' for s in states) else 'awaiting_user_decision' if any(s=='awaiting_user_decision' for s in states) else 'materializing'
    if len(valid)<len(config['sources']):state='waiting_for_inputs'
    for role,v in valid.items():
        if current-v['updated_at']>90 and v['state'] not in terminal:errors.append(role+': stale HF heartbeat')
        if independent:
            stamp=v.get('local_producer_updated_at')
            producer_stale[role]=v.get('local_producer_state',v['state']) not in terminal and (stamp is None or current-stamp>90)
            if producer_stale[role]:errors.append(role+': '+('missing' if stamp is None else 'stale')+' local producer heartbeat')
        if v.get('error'):errors.append(role+': '+v['error'])
    fields=['processed_rows','completed_blocks','completed_jobs','validated_samples','inflight_rows','mismatches','recovered_rows','new_rows']
    result={field:sum(v.get(field,0) for v in valid.values()) for field in fields}
    result.update(state=state,generation=config['generation'],run_id='26407881',start_block=16291127,end_block=26407881,total_blocks=10116755,total_jobs=config['total_jobs'],updated_at=min(v['updated_at'] for v in valid.values()),error='; '.join(errors) or None,waiting_for=[],failover=True,representation=config.get('representation','Independent surviving-host execution; recovered prior-generation outputs retain their receipts; unavailable data tracker unchanged.'),distributed=dict(engine=config.get('engine','Independent host processes'),hosts=config['workers'],online_hosts={r:config['workers'][r] if current-v['updated_at']<=90 and v['state']=='materializing' else 0 for r,v in valid.items()}),active_compute_processes=sum(v.get('active_compute_processes',0) for v in valid.values()))
    for field in ['coordinator_role','storage_role','format','dust_threshold_usd']:
        if field in config:result[field]=config[field]
    if independent:
        result['storage_role']='local_then_node_then_drive'
        fields=['local_buffered_bytes','node_transferred_bytes','node_accepted_jobs','local_transfer_target_bytes','local_producer_updated_at','local_producer_state','free_bytes','active_compute_processes','workers_configured']
        result['host_flow']={r:dict({k:v[k] for k in fields if k in v},state=v['state'],producer_stale=producer_stale[r]) for r,v in valid.items()}
        result['distributed']['hosts']={r:v.get('workers_configured',config['workers'][r]) for r,v in valid.items()}
        result['distributed']['online_hosts']={r:result['distributed']['hosts'][r] if current-v['updated_at']<=90 and not producer_stale[r] and v['state']=='materializing' else 0 for r,v in valid.items()}
    if config.get('format')=='hf-vector-f64-v2':
        result['total_user_values']=config.get('total_user_values',910310934885)
        for field in ['user_values_accepted','reserve_blocks_accepted','protocol_blocks_accepted','bytes']:
            result[field]=sum(v.get(field,0) for v in valid.values())
        # These are global Data-owned watermarks, repeated by each role endpoint.
        result['continuous_end_block']=min(v['continuous_end_block'] for v in valid.values())
        result['highest_accepted_block']=max(v['highest_accepted_block'] for v in valid.values())
        result['gap_jobs']=max(v['gap_jobs'] for v in valid.values())
        result['conditional_eta_seconds']=valid.get('data',{}).get('conditional_eta_seconds') if state=='materializing' else None
        result['validation_kind']='float64_model'
        result['distributed']['active_by_host']={r:v.get('active_compute_processes',0) for r,v in valid.items()}
    return result,'; '.join(errors) or None

def collect(config):
    def get(item):
        role,url=item
        try:
            with urllib.request.urlopen(url,timeout=3) as response:return role,json.load(response)
        except (OSError,ValueError) as e:return role,type(e).__name__
    with ThreadPoolExecutor(max_workers=len(config['sources'])) as pool:reports=dict(pool.map(get,config['sources'].items()))
    result,error=aggregate(reports,config)
    if result is not None and config.get('archive_source'):
        _,archive=get(('archive',config['archive_source']))
        result['archive']=archive if isinstance(archive,dict) else dict(state='unavailable',error=archive)
    return result,error
