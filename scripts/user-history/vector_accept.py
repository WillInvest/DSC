"""Node-side vector acceptance: exact logical coverage and real-node samples."""
from array import array
import fcntl,hashlib,os,time,threading
from pathlib import Path
import pyarrow as pa
import pyarrow.compute as pc
from shared.artifacts import checksum,digest,write_json
import hf_vectors as codec
import failover_hf as f
import materialize_accounts as m
from vector_compute import universe
NODE_LOCK=threading.Lock()

def audit_parts(parts,job,identity,samples=()):
    owners=universe();firsts=[x[1] for x in owners]
    import bisect
    n=bisect.bisect_left(firsts,job['stop']);expected=m.row_count(owners[:n],job['start'],job['stop'])
    if digest(owners)!=identity['registry_digest']:raise ValueError('Registry identity changed')
    next_block=array('I',(max(job['start'],b) for _,b in owners[:n]));rows=segments=0;found=set()
    selected={(s['entity_id'],s['block']):s for s in samples}
    for part in parts:
        data=part.get('data')
        if data is None:data=Path(part['path']).read_bytes()
        if hashlib.sha256(data).hexdigest()!=part['sha256']:raise ValueError('Transferred part hash differs')
        vectors=codec.loads(data);decoded=codec.unpack(vectors)
        if decoded.num_rows!=part['hf_values'] or vectors.num_rows!=part['segments'] or decoded.num_rows>identity['buffer_values']:raise ValueError('Logical/physical count differs')
        if codec.logical_sha(decoded)!=part['logical_sha256']:raise ValueError('Lossless semantic identity differs')
        lengths=pc.list_value_length(vectors['hf_values']).to_pylist()
        for uid,start,length in zip(vectors['entity_id'].to_pylist(),vectors['start_offset'].to_pylist(),lengths):
            block=start+codec.ORIGIN
            if uid>=n or next_block[uid]!=block or block+length>job['stop']:raise ValueError('Duplicate/missing/out-of-range HF coverage')
            next_block[uid]=block+length
        for key,s in selected.items():
            table=decoded.filter(pc.and_(pc.equal(decoded['entity_id'],key[0]),pc.equal(decoded['block_number'],key[1])))
            if table.num_rows:
                if key in found or table.num_rows!=1 or table['hf_values'][0].as_py()!=codec.raw_hf(s['account'][5]):raise ValueError('Stored HF differs from sampled account')
                found.add(key)
        rows+=decoded.num_rows;segments+=vectors.num_rows
    if rows!=expected or any(b!=job['stop'] for b in next_block):raise ValueError('Incomplete all-user every-block coverage')
    if found!=set(selected):raise ValueError('Missing node sample in vector files')
    return dict(rows=rows,segments=segments,owner_count=n,owner_first_seen_sha256=digest(owners[:n]))

def node_check(samples,job):
    from accounting_inputs import profiles,profile_at
    plan=m.load(f.ROOT/'plan.json');profs=profiles()
    tasks=[dict(kind='call',function='getUserAccountData',args=[s['user']],block=s['block'],profile=s['profile']) for s in samples]
    if not tasks:tasks=[dict(kind='call',function='getReservesList',args=[],block=b,profile=profile_at(profs,b)['key']) for b in sorted({job['start'],job['stop']-1})]
    abis={p['key']:p['abi'] for p in profs if p['key'] in {x['profile'] for x in tasks}}
    with NODE_LOCK:refs=f.direct_node(plan,dict(operation='snapshots',abis=abis,tasks=tasks))
    if len(refs)!=len(tasks):raise ValueError('Node omitted samples')
    db=m.readonly(f.ROOT/'accounting.sqlite');checks=[]
    try:
        for index,(task,ref) in enumerate(zip(tasks,refs)):
            header=db.execute('select hash from headers where block=?',(task['block'],)).fetchone()
            exact=bool(header and header[0].lower()==ref['block_hash'].lower())
            if samples:
                s=samples[index];actual=[str(x) for x in ref['result']]
                exact=exact and s['account']==actual and s['block_hash'].lower()==ref['block_hash'].lower()
                checks.append(dict(user=s['user'],block=s['block'],profile=s['profile'],predicted=s['account'],node=actual,block_hash=ref['block_hash'],exact=exact))
            else:checks.append(dict(block=task['block'],field='empty_interval_endpoint_hash',exact=exact))
    finally:db.close()
    report=dict(checks=checks,all_exact=all(x['exact'] for x in checks),account_getter_samples=len(samples),empty_interval_endpoint_checks=0 if samples else len(checks),created_at=time.time())
    if not report['all_exact']:raise ValueError('Exact own-node comparison failed')
    return report

def check_receipt(path,job,generation,root=None):
    r=m.load(path)
    if r['generation']!=generation or r['job']!=job or digest(r['identity'])!=generation or not r['all_exact']:raise ValueError('Receipt identity differs')
    if checksum(r['validation'])!=r['validation_sha256'] or not m.load(r['validation'])['all_exact']:raise ValueError('Invalid retained node validation')
    expected=audit_parts(r['parts'],job,r['identity'],r['samples'])
    if any(r[k]!=v for k,v in expected.items()):raise ValueError('Rechecked vector coverage differs')
    factor=r['factor_input']
    if checksum(factor['path'])!=factor['sha256']:raise ValueError('Input factor identity changed')
    return r

def accept(result,directory,interrupt_after=None):
    directory=Path(directory);directory.mkdir(parents=True,exist_ok=True);job=result['job'];gen=result['generation']
    if digest(result['identity'])!=gen:raise ValueError('Result generation differs')
    for name,sha in result['identity']['sources'].items():
        if checksum(Path(__file__).with_name(name))!=sha:raise ValueError('Acceptance source identity differs')
    target=directory/'receipt.json'
    with (directory/'accept.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        if target.exists():return check_receipt(target,job,gen)
        m.ensure_capacity(directory,projected=sum(len(p['data']) for p in result['parts']))
        stored=[]
        for index,part in enumerate(result['parts']):
            p=directory/('part-'+str(index).zfill(4)+'.parquet');data=part['data']
            if hashlib.sha256(data).hexdigest()!=part['sha256']:raise ValueError('Transfer byte hash differs')
            if p.exists():
                if checksum(p)!=part['sha256']:raise ValueError('Preserved existing part conflicts')
            else:
                temp=p.with_suffix('.incoming')
                with temp.open('wb') as h:h.write(data);h.flush();os.fsync(h.fileno())
                os.replace(temp,p)
            stored.append(dict(path=str(p.resolve()),**{k:v for k,v in part.items() if k!='data'}))
            if interrupt_after and len(stored)==interrupt_after:raise InterruptedError('Intentional acceptance checkpoint test')
        values=audit_parts(stored,job,result['identity'],result['samples'])
        if result['rows']!=values['rows'] or result['owner_first_seen_sha256']!=values['owner_first_seen_sha256']:raise ValueError('Worker universe differs')
        report=node_check(result['samples'],job);validation=directory/('node-'+str(time.time_ns())+'.json');write_json(validation,report)
        receipt=dict(format='hf-vector-v1',generation=gen,identity=result['identity'],job=job,job_id=job['id'],start=job['start'],stop=job['stop'],**values,parts=stored,samples=result['samples'],factor_input=result['factor_input'],factor_generation=f.FACTOR,role=result['role'],worker=result.get('worker'),validation=str(validation.resolve()),validation_sha256=checksum(validation),all_exact=True,mismatches=0,validated_samples=report['account_getter_samples'],created_at=time.time(),elapsed_seconds=result['elapsed_seconds'],peak_rss_kib=result['peak_rss_kib'],bytes=sum(Path(p['path']).stat().st_size for p in stored),representation='Full-user end-of-every-block exact HF vectors; other account/asset quantities retained as reconstructible historical factors')
        from distributed_hf import publish_result
        def checked_record(path,*args):
            value=m.load(path)
            if value!=receipt:raise ValueError('Publication metadata differs from fully read-back-verified result')
            return value
        return publish_result(receipt,target,job,gen,f.ROOT,check=checked_record)
