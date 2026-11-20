"""Build queryable all-block nominal/HF histories and publish independent node checks.

Sparse factors preserve exact every-block semantics without materializing a
user x block Cartesian product. Numeric account rows are generated and validated
separately; counters never label factor rows as numeric account observations.
"""
import argparse,fcntl,json,sys,time,hashlib,subprocess
from pathlib import Path
sys.path.insert(0,'/home/dsc-data/scripts')
from accounting_inputs import *
from accounting_query import Evaluator

def coverage(plan,available):
    ready={j['address'] for j in available};missing=[j for j in plan['jobs'] if j['address'] not in ready]
    return min([j['start_block']-1 for j in missing]+[plan['end_block']]),missing

def candidate(db,ev,job,b):
    # Filter the latest row of every owner, rather than a bounded historical
    # sample that could miss an older still-active position.
    row=db.execute("select owner from (select owner,scaled,row_number() over (partition by owner order by block desc,tx desc,log desc) as rank from changes where token=? and block<=?) where rank=1 and scaled!='0' and owner!='0x0000000000000000000000000000000000000000' order by owner limit 1",(job['address'],b)).fetchone()
    return row[0] if row else '0x0000000000000000000000000000000000000001'

def verify_nominal(db,root,plan,profs,ev,job):
    token_profile=read(root/'token-profiles'/(job['address']+'.profiles.json'))
    points={plan['end_block'],min(plan['end_block'],job['start_block']+1000),(job['start_block']+plan['end_block'])//2}
    for version in token_profile['profiles'][1:]:
        points.update([version['block']-1,version['block']])
    comparisons=[];tasks=[]
    for b in sorted(points):
        if not job['start_block']<=b<=plan['end_block']:continue
        owner=candidate(db,ev,job,b);pred=ev.actual(job,owner,b)
        comparisons.append(dict(owner=owner,block=b,predicted=pred))
        tasks.append(dict(kind='token_call',token=job['address'],owner=owner,block=b,profile=profile_at(profs,b)['key']))
    actual=node(plan,dict(operation='snapshots',abis={p['key']:p['abi'] for p in profs},tasks=tasks))
    for value,reference in zip(comparisons,actual):value.update(node_balance=reference['result'],exact=int(value['predicted']['balance'])==int(reference['result']))
    exact=all(value['exact'] for value in comparisons);report=dict(kind='nominal',token=job['address'],rows=comparisons,exact=exact,created_at=time.time(),source='Factorized token events/rates and original Aave math; first/middle/end/upgrade balanceOf comparisons only')
    report['generation']=ev.generation
    path=root/'accounting-validations'/('nominal-'+job['address']+'-'+str(time.time_ns())+'.json');write_json(path,report)
    with db:db.execute('insert or replace into validations values(?,?)',('nominal-'+job['address'],json.dumps(report)))
    if not exact:raise ValueError('Nominal balance mismatch '+str(path))
    receipt=dict(token=job['address'],asset=job['asset'],kind=job['kind'],start=job['start_block'],stop=plan['end_block']+1,representation='factorized_exact_all_block',numeric_rows_materialized=len(comparisons),validation=str(path),source_share_receipt=str(root/'receipts'/(job['address']+'.json')),share_receipt_sha256=checksum(root/'receipts'/(job['address']+'.json')),timestamp_receipt=str(root/'accounting-header-receipt.json'),timestamp_receipt_sha256=checksum(root/'accounting-header-receipt.json'),created_at=time.time())
    receipt['generation']=ev.generation
    path=root/'nominal-receipts'/(job['address']+'-'+str(time.time_ns())+'.json');write_json(path,receipt)
    with db:db.execute('insert into nominal_tokens values(?,?)',(job['address'],str(path)))
    return report

def verify_accounts(db,root,plan,profs,ev,through,available):
    points=sorted({through,*[p['from_block']-1 for p in profs[1:] if START<p['from_block']<=through],*[p['from_block'] for p in profs[1:] if p['from_block']<=through]})
    # Include SVB when the complete account input prefix reaches it.
    if through>=16804999:points.append(16804999)
    reports=[]
    for b in sorted(set(points)):
        key='hf-'+str(b)
        saved=db.execute('select data from validations where id=?',(key,)).fetchone()
        saved=json.loads(saved[0]) if saved else None
        if saved and saved.get('exact') is True and saved.get('generation')==ev.generation:continue
        debt=[j for j in available if j['kind']=='variableDebtToken' and j['start_block']<=b]
        users=[]
        for job in debt:
            user=candidate(db,ev,job,b)
            if user!='0x0000000000000000000000000000000000000001' and user not in users:users.append(user)
            if len(users)==3:break
        if not users:
            for job in available:
                if job['kind']!='aToken' or job['start_block']>b:continue
                user=candidate(db,ev,job,b)
                if user!='0x0000000000000000000000000000000000000001':users.append(user);break
        if not users:continue  # No nonzero position at this checkpoint.
        values=[ev.account(user,b) for user in users];p=profile_at(profs,b)
        tasks=[dict(kind='call',function='getUserAccountData',args=[u],block=b,profile=p['key']) for u in users]
        refs=node(plan,dict(operation='snapshots',abis={p['key']:p['abi'] for p in profs},tasks=tasks))
        comparisons=[dict(value=value,node=[str(v) for v in ref['result']],exact=value['account']==[str(v) for v in ref['result']]) for value,ref in zip(values,refs)]
        report=dict(kind='full_account_HF',block=b,rows=comparisons,exact=all(v['exact'] for v in comparisons),created_at=time.time(),scope='Whole-account six-field node comparison, sparse factors; sample does not establish all historical correctness')
        report['generation']=ev.generation
        path=root/'accounting-validations'/(key+'-'+str(time.time_ns())+'.json');write_json(path,report)
        with db:db.execute('insert or replace into validations values(?,?)',(key,json.dumps(report)))
        if not report['exact']:raise ValueError('Full account HF mismatch '+str(path))
        reports.append(report)
    return reports

def status_inputs(db,generation):
    reports=[json.loads(row[0]) for row in db.execute('select data from validations')]
    reports=[r for r in reports if generation is not None and r.get('generation')==generation]
    count=sum(read(row[0]).get('generation')==generation for row in db.execute('select receipt from nominal_tokens')) if generation is not None else 0
    return reports,count

def run_accounting(once=False):
    current=read(ROOT/'current.json');root=Path(current['root']);plan=read(current['plan']);profs=profiles();started=time.monotonic();db=connect(root)
    for folder in ['accounting-validations','nominal-receipts','hf-receipts']:(root/folder).mkdir(exist_ok=True)
    through=START-1;hf_pointer={};active_generation=None
    def status(state,waiting=None,current=None,error=None):
        validations,nom_done=status_inputs(db,active_generation);nom=[r for r in validations if r['kind']=='nominal'];hf=[r for r in validations if r['kind']=='full_account_HF']
        nominal=dict(state=state,generation=active_generation,run_id=plan['run_id'],start_block=START,end_block=plan['end_block'],completed_blocks=max(0,through-START+1),total_blocks=plan['end_block']-START+1,processed_rows=sum(len(r.get('rows',[])) for r in nom),completed_jobs=nom_done,total_jobs=len(plan['jobs']),validated_samples=sum(len(r.get('rows',[])) for r in nom),mismatches=sum(not r['exact'] for r in nom),updated_at=time.time(),error=error,waiting_for=waiting or [],current=current,representation='factorized exact history; numeric_rows are separately counted samples',numeric_rows_materialized=sum(len(r.get('rows',[])) for r in nom),elapsed_seconds=time.monotonic()-started)
        health=dict(nominal,state='waiting_for_inputs' if waiting else state,processed_rows=sum(len(r['rows']) for r in hf),validated_samples=sum(len(r['rows']) for r in hf),mismatches=sum(sum(not x['exact'] for x in r['rows']) for r in hf),completed_jobs=max(0,through-START+1),total_jobs=plan['end_block']-START+1,representation='factorized whole-account history; numerical HF rows are samples, not dense user x block output',numeric_rows_materialized=sum(len(r['rows']) for r in hf))
        health.update(hf_pointer)
        write_json(root/'nominal-progress.json',nominal);write_json(root/'hf-input-progress.json',health)
    try:
        with (root/'accounting.lock').open('a') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            status('preparing',waiting=['Block timestamps, reserve/configuration snapshots and indexed share histories'])
            headers=prepare_headers(root,plan,status)
            for f in headers['files']:
                if not indexed(db,f['path'],f['sha256']):
                    if checksum(f['path'])!=f['sha256']:raise ValueError('Block timestamp input hash differs')
                    import_file(db,f['path'],'headers',pq.read_table(f['path']).to_pylist());status('indexing_block_times',waiting=['Indexing exact block timestamps'])
            ingest_pool(db,root,plan,profs,status)
            ingest_risk(db,root,plan,Path('/home/dsc-data/artifacts/aave-risk-inputs-20261114/configurator-abi-proof.json'))
            snapshots(db,root,plan,profs,status)
            while True:
                available=import_changes(db,root,plan,status);gho_borrowing_flags(db,plan,profs,status);ev=Evaluator(root,db,plan);active_generation=ev.generation
                try:
                    for job in available:
                        old=db.execute('select receipt from nominal_tokens where token=?',(job['address'],)).fetchone()
                        if old and read(old[0]).get('generation')!=ev.generation:
                            with db:db.execute('delete from nominal_tokens where token=?',(job['address'],))
                            old=None
                        if not old:
                            status('validating',current={'token':job['address'],'kind':job['kind']});verify_nominal(db,root,plan,profs,ev,job)
                    target,missing=coverage(plan,available)
                    status('validating',current={'through_block':target,'stage':'Whole-account HF versus node'})
                    verify_accounts(db,root,plan,profs,ev,target,available)
                    through=target
                    receipt=dict(start=START,stop=through+1,representation='factorized whole-account history; evaluate with accounting_query.py',fixed_cutoff=plan['end_block'],all_account_assets_required=True,inputs=dict(database=str(root/'accounting.sqlite'),block_times_receipt=str(root/'accounting-header-receipt.json'),oracle_plan='/home/dsc-data/runs/aave-oracle-v3/'+plan['run_id']+'/plan.json'),created_at=time.time(),remaining_tokens=[j['address'] for j in missing])
                    receipt['generation']=ev.generation
                    receipt_path=root/'hf-receipts'/('through-'+str(through)+'-'+str(time.time_ns())+'.json');write_json(receipt_path,receipt)
                    hf_pointer=dict(receipt=str(receipt_path),receipt_sha256=checksum(receipt_path),generation=ev.generation)
                    status('factorized_history_ready' if not missing else 'waiting_for_inputs',waiting=[] if not missing else [str(len(missing))+' token histories pending; whole-account coverage through '+str(through)])
                    if not missing or once:return
                finally:ev.close()
                time.sleep(30)
    except Exception as exc:
        try:status('failed',error=type(exc).__name__+': '+str(exc))
        except Exception:
            for stage in ['nominal','hf-input']:
                path=root/(stage+'-progress.json');old=read(path) if path.exists() else {};write_json(path,dict(old,state='failed',updated_at=time.time(),error=type(exc).__name__+': '+str(exc)))
        raise
    finally:db.close()
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--once',action='store_true');a=p.parse_args();run_accounting(a.once)
