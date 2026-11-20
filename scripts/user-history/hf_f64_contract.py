"""Pinned scope and research metric contract for the float64 result generation."""
ORIGIN=16291127
STOP=26407882
LOGICAL_VALUES=910310934885
CUTOFF_HASH='0x2c88bc81d57ed2af2a17622a4450a182b25663b3c8d7c48cbcf8bdd8a5a46b91'
FACTOR='5197dae89b140ccbed5090f2f3e6ff39dde3eba5c737bb97b0ec320663532004'
IMAGE='1d1a9be72b345989a4a8667573e929c66320535824f480517e6e440633d0e32d'
REGISTRY='88289451664b81f6e08c52110f2fcd0e74f7dbe792c5fd9ee2d5c170df55c70d'
CONTRACT=dict(format='hf-vector-f64-v2',block_origin=ORIGIN,end_block=STOP-1,end_hash=CUTOFF_HASH,all_users=True,block_step=1,logical_values=LOGICAL_VALUES,factor_generation=FACTOR,snapshot_manifest_sha256=IMAGE,registry_digest=REGISTRY,hf_scale=1,no_debt='positive_infinity',missing_user_values='forbidden',zero_debt_aggregate='null',metric='debt_value_weighted_overall_user_health_factor',reserve_attribution='borrowed_asset_using_same_historical_user_valuation',aggregate_reference_rtol=1e-6,aggregate_reference_atol=1e-8,within_model_rtol=1e-10,within_model_atol=1e-10,raw_and_exact_sources_retained=True)
CONTRACT.update(dust_debt_base_exclusive=1_000_000,base_currency_unit=100_000_000,dust_threshold_usd='0.01',dust_policy='Account total debt below threshold is treated as zero; HF positive infinity; exclude from all debt-weighted indicators; retain raw inputs and per-block exclusion counts/value',source_projection='integer_cast_then_same_dust_policy_or_float64_batch')

def source_assignments(manifest):
    """Derive physical source ownership only from frozen reconciliation evidence."""
    relocated={x['job_id']:x['backup'] for x in manifest.get('retained_location_mapping',{}).get('data',{}).get('files',[])}
    jobs=[];sources={};roles={}
    for value in manifest['jobs']:
        job={k:value[k] for k in ['id','start','stop','expected_rows']};jobs.append(job);kind=value['user_source']
        if kind=='new_float64':continue
        receipt=value['source']['receipt'];role=value['source'].get('owner','node') if kind=='integer_table' else 'node'
        if kind=='integer_vector':files=[dict(path=p['path'],sha256=p['sha256'],values=p['hf_values']) for p in receipt['parts']]
        elif kind=='integer_table':files=[dict(path=receipt['output'],sha256=receipt['sha256'],values=receipt['rows'])]
        else:raise ValueError('Unknown reconciled source format')
        if job['id'] in relocated:files[0]['path']=relocated[job['id']];role='node'
        sources[job['id']]=dict(kind=kind,receipt=receipt,files=files);roles[job['id']]=role
    return jobs,sources,roles

def reconcile(predecessor,accepted_vectors):
    """One target source per preserved interval, without crediting f64 completion."""
    if predecessor['planned_rows']!=LOGICAL_VALUES or predecessor['identity']['factor_generation']!=FACTOR:raise ValueError('Input population identity differs')
    scope=sorted(predecessor['coverage'],key=lambda j:j['start'])
    if scope[0]['start']!=ORIGIN or scope[-1]['stop']!=STOP or any(a['stop']!=b['start'] for a,b in zip(scope,scope[1:])):raise ValueError('Scope gap/overlap')
    jobs={j['id']:j for j in predecessor['jobs']};vectors={}
    for value in accepted_vectors:
        r=value['receipt'];key=r['job_id']
        if key in vectors or key not in jobs or r['job']!=jobs[key] or not r['all_exact'] or r['mismatches']:raise ValueError('Duplicate/invalid retained vector')
        vectors[key]=value
    result=[]
    for j in scope:
        key=j['id']
        if key in predecessor['recovered']:
            source=predecessor['recovered'][key];r=source['receipt'];kind='integer_table';locator=source
        elif key in vectors:
            locator=vectors[key];r=locator['receipt'];kind='integer_vector'
        else:r=None;kind='new_float64';locator=None
        expected=j.get('expected_rows',r['rows'] if r else None)
        if expected is None or r and (r['start']!=j['start'] or r['stop']!=j['stop'] or r['rows']!=expected):raise ValueError('Retained interval population differs')
        result.append(dict(id=key,start=j['start'],stop=j['stop'],expected_rows=expected,user_source=kind,source=locator,needs_weights=True,target_complete=False))
    if sum(j['expected_rows'] for j in result)!=LOGICAL_VALUES:raise ValueError('Logical user population differs')
    return result
