"""Data assigns and observes independent producers; transfer/cloud never gate compute."""
import argparse,fcntl,json,time
from pathlib import Path
from shared.artifacts import checksum,digest,write_json
from hf_flow_queue import FlowLedger
from hf_flow_io import read_catalog,prepare_transfer,copy_batch,transfer_report,retire_local,hold
from hf_flow_cloud import upload_and_retire


def submit_bulk(client, function, *args, payload_index=0, **kwargs):
    """Keep large receipt/certificate graphs off the scheduler's heap."""
    target = kwargs['workers'][:1]
    reference = client.scatter([args[payload_index]], workers=target, direct=True, hash=False)[0]
    arguments = list(args); arguments[payload_index] = reference
    try:
        future = client.submit(function, *arguments, **dict(kwargs, workers=target))
    except BaseException:
        reference.release()
        raise
    return future, reference


def validate_value(value,job,generation,source):
    try:
        r=value['receipt']
        valid=(r['generation']==generation and r['job']==job and digest(r['identity'])==generation
               and r.get('within_error_budget') and r.get('structural_exact') and r['rows']==job['expected_rows']
               and digest(r)==value['sha256'])
    except (KeyError,TypeError) as exc:
        raise ValueError('Malformed accepted producer receipt') from exc
    if not valid:raise ValueError('Invalid accepted producer receipt')
    if (r.get('retained_source_sha256')!=(digest(source) if source else None)
        or r.get('source_kind')!=('integer_cast' if source else 'float64_batch')):
        raise ValueError('Accepted HF provenance differs from assigned source')


def transfer_target(db, totals, pending, cfg, available, *, available_roles=None):
    """Normally125GB; close a nearby500GB archive using already accepted tails."""
    normal=cfg['transfer_target_bytes']
    if available>=normal:return normal
    if not cfg.get('archive_fill_transfers') or any(o['kind']=='cloud' for o in pending):return None
    projected=sum(t['cloud_pending_bytes'] for t in totals)
    for op in pending:
        if op['kind']!='transfer':continue
        if available_roles is not None and op['role'] not in available_roles:continue
        keys=op['payload']['keys']
        for offset in range(0,len(keys),500):
            part=keys[offset:offset+500]
            if not part:continue
            sql='select coalesce(sum(m.bytes),0) from jobs j join metrics m on j.job_id=m.job_id where j.node_value is null and j.job_id in ('+','.join('?' for _ in part)+')'
            projected+=db.db.execute(sql,part).fetchone()[0]
    gap=cfg['cloud_target_bytes']-projected
    if 0<gap<=normal and available>=min(gap,max(1,normal//4)):
        return min(available,gap)
    return None


def completed_scope(db,jobmap):
    rows=db.db.execute('select j.job_id,j.cloud_batch,m.rows from jobs j join metrics m on j.job_id=m.job_id').fetchall()
    if {r[0] for r in rows}!=set(jobmap) or any(r[1] is None for r in rows) or db.pending_operations():
        return False
    if sum(r[2] for r in rows)!=910310934885 or sum(j['expected_rows'] for j in jobmap.values())!=910310934885:
        raise ValueError('Completed HF population differs from frozen production scope')
    return True


def run(config_path,pilot=False):
    from distributed import Client
    from vector_coordinator import security
    from vector_archive import registry_source
    cfg=json.loads(Path(config_path).read_text());root=Path(cfg['control_root']);gen=cfg['generation']
    if not pilot and cfg.get('production_ready') is not True:raise ValueError('Production flow checks are incomplete')
    if checksum(cfg['numeric_plan'])!=cfg['numeric_plan_sha256']:raise ValueError('Frozen numeric plan changed')
    plan=json.loads(Path(cfg['numeric_plan']).read_text());jobmap={j['id']:j for j in plan['jobs']}
    roles=cfg['roles'];pilot_keys={v['jobs'][0] for v in roles.values()}
    started=time.monotonic();active={};progress={};errors={};catalog_errors={};initial=None;metadata=None;metadata_future=None;metadata_reference=None;metadata_retry=0
    old=Path(cfg['old_base']);public=root/'public';public.mkdir(exist_ok=True)
    with (root/'controller.lock').open('a') as lock,FlowLedger(root/'flow.sqlite',gen) as db:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        client=Client(cfg['scheduler'],security=security(cfg['private']),set_as_default=False)
        def store_node(report):
            if report.get('all_exact') is not True:raise ValueError('Unverified node transfer')
            expected=set(next(o['payload']['keys'] for o in db.pending_operations() if o['id']==report['id']))
            if {v['job_id'] for v in report['jobs']}!=expected:raise ValueError('Transfer scope changed')
            for value in report['jobs']:
                if value['job_id'] not in jobmap:raise ValueError('Unknown transferred job')
                validate_value(value,jobmap[value['job_id']],gen,plan['sources'].get(value['job_id']))
                db.node_commit(value['job_id'],value)
            write_json(root/'transfers'/(report['id']+'.json'),report)
        def integrity_failure(role,exc):
            errors[role]=type(exc).__name__+': '+str(exc)
            affected=list(roles) if role=='cloud' else [role]
            if role in roles and role!='node':affected.append('node')
            publish('failed')
            for affected_role in set(affected):
                if workers.get(affected_role):
                    client.run(hold,roles[affected_role],'Integrity failure: '+errors[role],workers=workers[affected_role][:1])
            raise ValueError('Flow integrity failure; affected new producers held') from exc
        def publish(overall_state=None):
            nonlocal initial
            totals=db.stage_totals();total_values=sum(x['rows'] for x in totals)
            if initial is None:initial=total_values
            elapsed=time.monotonic()-started;rate=(total_values-initial)/elapsed if elapsed>30 else None
            cloud=[json.loads(x[0]) for x in db.db.execute('select report from cloud')]
            byrole={v['role']:v for v in totals};node_count=sum(t['node_jobs'] for t in totals)
            known={x[0] for x in db.db.execute('select job_id from jobs')};continuous=16291127
            for job in sorted(plan['jobs'],key=lambda j:j['start']):
                if job['start']!=continuous or job['id'] not in known:break
                continuous=job['stop']
            highest=max((jobmap[k]['stop']-1 for k in known),default=16291126)
            for role,rcfg in roles.items():
                s=progress.get(role,{});t=byrole.get(role,{})
                state=overall_state or s.get('state','connecting')
                if state=='running':state='materializing'
                if state=='complete' and pilot:state='canary_complete'
                elif state=='complete' and t.get('jobs',0)<rcfg.get('expected_role_jobs',len(rcfg['jobs'])):state='finalizing_catalog'
                value=dict(role=role,display_role='dsc-compute' if role=='web' else 'dsc-'+role,generation=gen,manifest_sha256=cfg['numeric_plan_sha256'],epoch='20261116-independent-flow',run_id='26407881',updated_at=time.time(),
                    state=state,error=s.get('error') or errors.get(role) or catalog_errors.get(role),start_block=16291127,end_block=26407881,total_blocks=10116755,total_jobs=len(plan['jobs']),
                    completed_jobs=t.get('jobs',0),completed_blocks=t.get('blocks',0),processed_rows=t.get('rows',0),new_rows=t.get('rows',0),recovered_rows=0,
                    user_values_accepted=t.get('rows',0),reserve_blocks_accepted=t.get('blocks',0),protocol_blocks_accepted=t.get('blocks',0),gap_jobs=len(plan['jobs'])-sum(x['jobs'] for x in totals),
                    continuous_end_block=continuous-1,highest_accepted_block=highest,
                    bytes=t.get('bytes',0),free_bytes=s.get('free_bytes'),reserved_free_bytes=rcfg['reserve_bytes'],workers_configured=s.get('slots',rcfg['slots']),active_compute_processes=len(s.get('active_jobs',[])),inflight_rows=sum(jobmap[k]['expected_rows'] for k in s.get('active_jobs',[])),
                    current=dict(jobs=[dict(id=k,stage='local_compute_and_accept') for k in s.get('active_jobs',[])]),rate_user_values_per_second=rate,
                    conditional_eta_seconds=(910310934885-total_values)/rate if rate and rate>0 else None,mismatches=0,validated_samples=t.get('validated_samples',0),
                    normal_provider_rpc_recorded=t.get('normal_provider_rpc_recorded',0),normal_rpc_jobs_recorded=t.get('normal_rpc_jobs_recorded',0),
                    local_buffered_bytes=s.get('buffered_bytes',0),node_transferred_bytes=t.get('node_bytes',0),node_accepted_jobs=t.get('node_jobs',0),
                    local_transfer_target_bytes=cfg['transfer_target_bytes'],local_producer_updated_at=s.get('updated_at'),local_producer_state=s.get('state'),coordinator_role=cfg.get('coordinator_role','data'),storage_role='local_then_node_then_drive',
                    representation='Independent host-local NumPy production; accepted local and node stages shown separately; original numeric generation retained')
                write_json(public/(role+'.json'),value)
            pending=sum(t['cloud_pending_bytes'] for t in totals)
            cloudstate=overall_state or ('uploading' if any(v['kind']=='cloud' for v in active.values()) else 'collecting')
            write_json(public/'archive.json',dict(state=cloudstate,generation=gen,updated_at=time.time(),transport='node_direct_rclone',batch_target_bytes=cfg['cloud_target_bytes'],
                pending_bytes=pending,pending_jobs=sum(1 for _ in db.db.execute('select job_id from jobs where node_value is not null and cloud_batch is null')),
                verified_bytes=sum(r['verified_bytes'] for r in cloud),staged_bytes=0,staging_chunk_limit_bytes=0,error=errors.get('cloud'),
                completed_batches=len(cloud),node_parquet_cleanup_enabled=True))
            write_json(root/'progress.json',dict(generation=gen,state=overall_state or ('pilot' if pilot else 'running'),updated_at=time.time(),totals=totals,node_jobs=node_count,
                operations=[dict(id=k,kind=v['kind'],stage=v['stage']) for k,v in active.items()],errors=dict(catalog_errors,**errors),startup_seconds=None,all_hosts_independent=True))
        try:
            while True:
                info=client.scheduler_info(n_workers=-1)['workers'];workers={r:[a for a,w in info.items() if str(w['name']).startswith('flow-io-'+r)] for r in roles}
                clouds=[a for a,w in info.items() if str(w['name']).startswith('flow-cloud')]
                for role,rcfg in roles.items():
                    if not workers[role]:continue
                    cursor=db.db.execute('select sequence from cursors where role=?',(role,)).fetchone();cursor=cursor[0] if cursor else 0
                    try:
                        values=client.run(read_catalog,rcfg,cursor,workers=workers[role][:1])
                        page=next(iter(values.values()));progress[role]=page['progress']
                        for item in page['entries']:
                            key=item['job_id'];value=item['value']
                            if key not in jobmap:raise ValueError('Unknown producer job')
                            validate_value(value,jobmap[key],gen,plan['sources'].get(key))
                            if key not in rcfg['jobs']:raise ValueError('Result on wrong host assignment')
                            db.observe(key,role,value)
                            if role=='node':db.node_commit(key,value)
                        with db.transaction():db.db.execute('insert into cursors values(?,?) on conflict(role) do update set sequence=excluded.sequence',(role,page['sequence']))
                        catalog_errors.pop(role,None)
                    except ValueError as exc:
                        integrity_failure(role,exc)
                    except Exception as exc:
                        catalog_errors[role]=type(exc).__name__+': '+str(exc)
                failed=[r for r,p in progress.items() if p.get('error') or p.get('state')=='failed']
                if failed:
                    for role in failed:
                        if workers[role]:client.run(hold,roles[role],'Validation failure on '+','.join(failed),workers=workers[role][:1])
                    publish('failed');raise ValueError('Producer validation failure; new producers held')
                for key in list(active):
                    item=active[key];f=item['future']
                    if f is None:continue
                    if not f.done():continue
                    reference=item.pop('payload_reference',None)
                    try:
                        value=client.gather(f,direct=True);op=item['op'];role=op['role']
                        if item['stage']=='probe':
                            if value is None:
                                item['stage']='prepare';item['future']=client.submit(prepare_transfer,roles[role],op['payload']['keys'],key,pure=False,workers=workers[role],allow_other_workers=False,resources={'flow_io':1})
                            else:
                                store_node(value);item['stage']='cleanup';item['future'],item['payload_reference']=submit_bulk(client,retire_local,roles[role],value,payload_index=1,pure=False,workers=workers[role],allow_other_workers=False,resources={'flow_io':1})
                        elif item['stage']=='prepare':
                            value['source_workers']=workers[role];item['stage']='copy';item['future'],item['payload_reference']=submit_bulk(client,copy_batch,value,dict(roles['node'],node_flow_root=cfg['node_flow_root']),pure=False,workers=workers['node'],allow_other_workers=False,resources={'flow_io':1})
                        elif item['stage']=='copy':
                            store_node(value);item['stage']='cleanup';item['future'],item['payload_reference']=submit_bulk(client,retire_local,roles[role],value,payload_index=1,pure=False,workers=workers[role],allow_other_workers=False,resources={'flow_io':1})
                        else:
                            if item['kind']=='cloud':
                                if value.get('all_remote_size_md5_exact') is not True or value.get('node_result_parquets_retired') is not True:raise ValueError('Incomplete cloud acceptance/cleanup')
                                db.cloud_commit(op['payload']['keys'],value)
                                write_json(root/'cloud'/(key+'.json'),value)
                            db.finish_operation(key,value);del active[key];errors.pop(role,None)
                    except ValueError as exc:
                        integrity_failure(item['op']['role'],exc)
                    except Exception as exc:
                        errors[item['op']['role']]=type(exc).__name__+': '+str(exc)
                        write_json(root/'errors'/(key+'-'+str(time.time_ns())+'.json'),dict(id=key,error=errors[item['op']['role']],created_at=time.time()))
                        item['retry_after']=time.monotonic()+60;item['future']=None
                    finally:
                        f.release()
                        if reference is not None:reference.release()
                for key in list(active):
                    if active[key]['future'] is None and time.monotonic()>=active[key]['retry_after']:del active[key]
                pending=db.pending_operations();busy={v['role'] for v in pending if v['kind']=='transfer'}
                totals=db.stage_totals();stats={x['role']:x for x in totals}
                for role in roles:
                    if role=='node' or role in busy:continue
                    t=stats.get(role,{})
                    terminal=progress.get(role,{}).get('state')=='complete'
                    target=transfer_target(db,totals,db.pending_operations(),cfg,t.get('transfer_bytes',0),available_roles={r for r in roles if workers.get(r)})
                    if pilot or target is not None or terminal:
                        ready=db.pending_transfer(role,target or cfg['transfer_target_bytes'])
                        if pilot:ready=[x for x in ready if x['job_id'] in pilot_keys]
                        if ready:
                            keys=[x['job_id'] for x in ready];batch=digest(dict(role=role,receipts=[(x['job_id'],x['value']['sha256']) for x in ready]))[:24]
                            db.prepare_operation(batch,'transfer',role,dict(keys=keys))
                cloud_pending=any(x['kind']=='cloud' for x in db.pending_operations())
                all_node=sum(x['node_jobs'] for x in totals)==len(plan['jobs'])
                if not cloud_pending:
                    ready=[];pending_bytes=sum(x['cloud_pending_bytes'] for x in totals)
                    if pilot:
                        ready=[x for x in db.cloud_candidates() if x['job_id'] in pilot_keys]
                        if {x['job_id'] for x in ready}!=pilot_keys:ready=[]
                    elif pending_bytes>=cfg['cloud_target_bytes'] or all_node:
                        ready=db.cloud_candidates(cfg['cloud_target_bytes'])
                    if ready:
                        keys=[x['job_id'] for x in ready];batch=digest([(x['job_id'],x['value']['sha256']) for x in ready])[:24]
                        db.prepare_operation(batch,'cloud','cloud',dict(keys=keys))
                for op in db.pending_operations():
                    key=op['id'];role=op['role']
                    if key in active:continue
                    if op['kind']=='transfer' and workers.get(role) and workers['node']:
                        f=client.submit(transfer_report,dict(roles['node'],node_flow_root=cfg['node_flow_root']),key,pure=False,workers=workers['node'],allow_other_workers=False,resources={'flow_io':1})
                        active[key]=dict(kind='transfer',stage='probe',future=f,op=op)
                    elif op['kind']=='cloud' and clouds and workers['node']:
                        if metadata is None:
                            if metadata_future is None and time.monotonic()>=metadata_retry:
                                values={n:(old/n).read_bytes() for n in ['users.parquet','dataset.json']}
                                values['numeric-plan-'+cfg['numeric_plan_sha256']+'.json']=Path(cfg['numeric_plan']).read_bytes()
                                metadata_future,metadata_reference=submit_bulk(client,registry_source,values,cfg['node_bulk'],pure=False,workers=workers['node'],allow_other_workers=False,resources={'flow_io':1},priority=100)
                            if metadata_future is None or not metadata_future.done():continue
                            try:
                                result=client.gather(metadata_future,direct=True)
                                metadata={str((Path(cfg['node_bulk'])/k).relative_to(cfg['node_home'])):v for k,v in result.items()}
                            except ValueError as exc:
                                integrity_failure('cloud',exc)
                            except Exception as exc:
                                errors['cloud']=type(exc).__name__+': '+str(exc)
                                metadata_retry=time.monotonic()+60
                            finally:
                                metadata_future.release();metadata_future=None
                                if metadata_reference is not None:metadata_reference.release();metadata_reference=None
                            if metadata is None:continue
                        values=[]
                        for job in op['payload']['keys']:
                            row=db.db.execute('select node_value from jobs where job_id=?',(job,)).fetchone();values.append(json.loads(row[0]))
                        payload=dict(id=key,values=values,job_receipts=[[v['receipt']['job_id'],v['sha256']] for v in values],metadata=metadata)
                        f,ref=submit_bulk(client,upload_and_retire,payload,cfg,pure=False,workers=clouds,allow_other_workers=False,resources={'flow_cloud':1})
                        active[key]=dict(kind='cloud',stage='cloud',future=f,payload_reference=ref,op=op)
                publish()
                if pilot:
                    count=db.db.execute('select count(*) from jobs where cloud_batch is not null and job_id in ('+','.join('?' for _ in pilot_keys)+')',tuple(pilot_keys)).fetchone()[0]
                    if count==len(pilot_keys) and not db.pending_operations('transfer'):
                        write_json(root/'pilot-completed.json',dict(all_four_hosts=True,user_reserve_protocol=True,transfers_verified=True,source_copies_retired=True,cloud_verified=True,node_parquets_retired=True,pilot_keys=sorted(pilot_keys),created_at=time.time()))
                        return
                elif completed_scope(db,jobmap):
                    publish('complete')
                    write_json(root/'complete.json',dict(generation=gen,logical_values=910310934885,jobs=len(jobmap),all_cloud_verified=True,completed_at=time.time()))
                    return
                time.sleep(5)
        finally:
            if metadata_future is not None:metadata_future.release()
            if metadata_reference is not None:metadata_reference.release()
            for item in active.values():
                if item.get('payload_reference') is not None:item['payload_reference'].release()
            client.close()


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--config',required=True);p.add_argument('--pilot',action='store_true');a=p.parse_args();run(a.config,a.pilot)
