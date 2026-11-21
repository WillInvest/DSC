"""Float-aware immutable acceptance; old exact acceptance is unchanged."""
import bisect,fcntl,hashlib,os,time
from pathlib import Path
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from shared.artifacts import checksum,digest,write_json
from hf_f64_contract import CONTRACT,REGISTRY,FACTOR
from hf_f64_vectors import decode_users
import materialize_accounts as m


def audit(result,owners):
    job=result['job'];first=np.array([b for _,b in owners]);n=bisect.bisect_left(first,job['stop'])
    if result['registry_digest']!=REGISTRY or digest(owners)!=REGISTRY:raise ValueError('Wrong user registry')
    cursor=np.maximum(job['start'],first[:n]).copy();rows=0;found=set()
    samples={(s['entity_id'],s['block']):s for s in result['samples']}
    def read(part):
        raw=part.get('data')
        if raw is None:raw=Path(part['path']).read_bytes()
        if hashlib.sha256(raw).hexdigest()!=part['sha256']:raise ValueError('Transferred part hash differs')
        return raw
    for part in result['parts']:
        start,stop,ids,values=decode_users(read(part),first)
        if start<job['start'] or stop>job['stop'] or not np.array_equal(cursor[ids],np.maximum(start,first[ids])):raise ValueError('Duplicate/gap in user coverage')
        count=int(np.count_nonzero(~np.isnan(values)))
        if count!=part['values'] or (start,stop)!=(part['start'],part['stop']):raise ValueError('User part count/scope differs')
        cursor[ids]=stop;rows+=count
        for (uid,block),sample in samples.items():
            index=int(np.searchsorted(ids,uid))
            if start<=block<stop and index<len(ids) and ids[index]==uid:
                value=values[block-start,index];expected=np.inf if sample['hf']=='+inf' else float(sample['hf'])
                if (uid,block) in found or value.view(np.uint64)!=np.float64(expected).view(np.uint64):raise ValueError('Stored sample differs')
                found.add((uid,block))
    expected=m.row_count(owners,job['start'],job['stop'])
    if rows!=expected or result['rows']!=rows or np.any(cursor!=job['stop']) or found!=set(samples):raise ValueError('Incomplete user coverage/samples')
    next_block=job['start'];scopes=len(result['reserves'])+1
    if result['reserves']!=sorted(set(result['reserves'])):raise ValueError('Reserve dictionary differs')
    for part in result['aggregate_parts']:
        t=pq.read_table(pa.BufferReader(read(part)));meta=t.schema.metadata or {}
        start=int(meta[b'chunk_start']);stop=int(meta[b'chunk_stop'])
        expected_columns=['scope_id','first_block_index','hf_vector','numerator_vector','denominator_vector']
        if result.get('dust_cutoff'):expected_columns+=['excluded_users_vector','excluded_debt_base_vector']
        if meta.get(b'format')!=b'hf-aggregate-f64-v1' or t.column_names!=expected_columns:raise ValueError('Aggregate schema differs')
        if start!=next_block or stop<=start or stop>job['stop'] or (start,stop)!=(part['start'],part['stop']):raise ValueError('Aggregate gap/overlap')
        if t['scope_id'].to_pylist()!=list(range(scopes)) or t['first_block_index'].to_pylist()!=[0]*scopes:raise ValueError('Aggregate dictionary differs')
        arrays={}
        for name in ['hf_vector','numerator_vector','denominator_vector']:
            col=t[name].combine_chunks()
            if col.type!=pa.list_(pa.float64()) or col.null_count or not np.all(np.diff(col.offsets.to_numpy())==stop-start):raise ValueError('Incomplete aggregate vector')
            arrays[name]=col.values.to_numpy(zero_copy_only=False).reshape(scopes,stop-start)
        h=arrays['hf_vector'];num=arrays['numerator_vector'];den=arrays['denominator_vector'];valid=den>0
        if not np.all(np.isfinite(num)) or not np.all(np.isfinite(den)) or np.any(num<0) or np.any(den<0) or np.any(num[~valid]!=0):raise ValueError('Invalid N/W proof')
        if not np.array_equal(np.isnan(h),~valid) or not np.allclose(h[valid],num[valid]/den[valid],atol=1e-10,rtol=1e-10):raise ValueError('HF/null formula differs')
        if not np.allclose(num[:-1].sum(axis=0),num[-1],atol=1e-10,rtol=1e-10) or not np.allclose(den[:-1].sum(axis=0),den[-1],atol=1e-10,rtol=1e-10):raise ValueError('Reserve/protocol reduction differs')
        if result.get('dust_cutoff'):
            evidence=[]
            for col,typ in [('excluded_users_vector',pa.uint32()),('excluded_debt_base_vector',pa.float64())]:
                if t.schema.field(col).type!=pa.list_(typ):raise ValueError('Exclusion evidence type differs')
                vectors=t[col].to_pylist()
                if any(v is not None for v in vectors[:-1]) or vectors[-1] is None or len(vectors[-1])!=stop-start:raise ValueError('Exclusion evidence coverage differs')
                evidence.append(np.asarray(vectors[-1]))
            count,value=evidence
            if not np.all(np.isfinite(value)) or np.any(value<0) or np.any(value>count*result['dust_cutoff']):raise ValueError('Invalid excluded debt amount')
        next_block=stop
    if next_block!=job['stop']:raise ValueError('Missing aggregate coverage')
    return dict(structural_exact=True,user_values=rows,aggregate_blocks=job['stop']-job['start'],reserve_count=scopes-1)


def node_check(samples,job,*,expected_headers=None):
    from vector_accept import node_check as exact_node_check
    adapted=[dict(s,account=s['exact_reference']) for s in samples]
    header_options={} if expected_headers is None else {'expected_headers':expected_headers}
    report=exact_node_check(adapted,job,**header_options)
    errors=[];excluded=0
    for s in samples:
        reference=int(s['exact_reference'][5]);actual=np.inf if s['hf']=='+inf' else float(s['hf'])
        if int(s['exact_reference'][1])<CONTRACT['dust_debt_base_exclusive']:
            if not np.isposinf(actual):raise ValueError('Dust/no-debt research sentinel differs')
            excluded+=int(int(s['exact_reference'][1])>0)
        elif reference==2**256-1:
            if not np.isposinf(actual):raise ValueError('No-debt sentinel differs from node')
        else:
            expected=reference/1e18;absolute=abs(actual-expected);relative=absolute/max(abs(expected),np.finfo(float).tiny)
            if not np.isfinite(actual) or absolute>1e-8+1e-6*abs(expected):raise ValueError('Float64 HF exceeds node error budget')
            errors.append((absolute,relative))
    a=np.array(errors).reshape(-1,2)
    return dict(validation_kind='float64_model',structural_exact=report['all_exact'],within_error_budget=True,dust_threshold_usd=CONTRACT['dust_threshold_usd'],excluded_dust_samples=excluded,exact_reference_node_report=report,account_getter_samples=len(samples),max_absolute_error=float(a[:,0].max()) if len(a) else 0.,max_relative_error=float(a[:,1].max()) if len(a) else 0.,created_at=time.time())


def accept(result,directory):
    from vector_compute import universe
    directory=Path(directory);directory.mkdir(parents=True,exist_ok=True)
    if result['format']!=CONTRACT['format'] or result['factor_input']['generation']!=FACTOR:raise ValueError('Wrong result identity')
    if result.get('dust_cutoff')!=CONTRACT['dust_debt_base_exclusive']:raise ValueError('Wrong debt exclusion policy')
    if digest(result['identity'])!=result['generation'] or result['identity']['contract']!=CONTRACT:raise ValueError('Wrong generation contract')
    for name,sha in result['identity']['sources'].items():
        if checksum(Path(__file__).with_name(name))!=sha:raise ValueError('Worker/acceptance source differs: '+name)
    target=directory/'receipt.json'
    with (directory/'accept.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        if target.exists():return check_receipt(target,result['job'],result['generation'])
        m.ensure_capacity(directory,projected=result['bytes'])
        stored=dict(result)
        for group in ['parts','aggregate_parts']:
            stored[group]=[]
            for i,part in enumerate(result[group]):
                p=directory/(group+'-'+str(i).zfill(4)+'.parquet');raw=part['data']
                if hashlib.sha256(raw).hexdigest()!=part['sha256']:raise ValueError('Transfer hash differs')
                if p.exists():
                    if checksum(p)!=part['sha256']:raise ValueError('Existing artifact conflicts')
                else:
                    temp=p.with_suffix('.incoming')
                    with temp.open('wb') as h:h.write(raw);h.flush();os.fsync(h.fileno())
                    os.replace(temp,p)
                stored[group].append(dict(path=str(p),**{k:v for k,v in part.items() if k!='data'}))
        proof=audit(stored,universe());validation=node_check(result['samples'],result['job'])
        if checksum(result['factor_input']['path'])!=result['factor_input']['sha256']:raise ValueError('Factor receipt changed')
        report=directory/'validation.json';write_json(report,dict(**proof,**{k:v for k,v in validation.items() if k!='structural_exact'}))
        stored.update(job_id=result['job']['id'],start=result['job']['start'],stop=result['job']['stop'],validation=str(report),validation_sha256=checksum(report),structural_exact=True,within_error_budget=True,validation_kind='float64_model',validated_samples=len(result['samples']),created_at=time.time(),production_coverage=True)
        from distributed_hf import publish_result
        return publish_result(stored,target,result['job'],result['generation'],Path('/home/dsc-data/runs/aave-user-history-v3/26407881'),check=lambda *args:stored)


def check_receipt(path,job,generation):
    from vector_compute import universe
    r=m.load(path)
    if r['job']!=job or r['generation']!=generation or digest(r['identity'])!=generation or not r['within_error_budget'] or not r['structural_exact']:raise ValueError('Receipt identity differs')
    if checksum(r['validation'])!=r['validation_sha256'] or not m.load(r['validation'])['within_error_budget']:raise ValueError('Validation report differs')
    audit(r,universe())
    return r
