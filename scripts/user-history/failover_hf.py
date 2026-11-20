"""Independent HF workers on surviving hosts; no central scheduler or data SSH.

Reuse the existing native calculator, read-only frozen factors, Parquet producer
and strict node/receipt checks. Only assignment, recovery and host progress are
new. Original-generation recovered outputs retain their identities.
"""
import argparse,bisect,fcntl,hashlib,json,multiprocessing,os,socket,sys,time
from pathlib import Path

OLD_GENERATION='b9f354b3dafedde1d478e192712b6ce70e8d89ff811941e73d97b0f5dc3165e5'
IMAGE_SHA='1d1a9be72b345989a4a8667573e929c66320535824f480517e6e440633d0e32d'
FACTOR='5197dae89b140ccbed5090f2f3e6ff39dde3eba5c737bb97b0ec320663532004'
ROOT=Path('/home/dsc-data/runs/aave-user-history-v3/26407881')
CODE_FILES=('failover_hf.py','materialize_accounts.py','native_hf.py','native_batch.py')
RPC='http://10.246.103.160:8545'

def load(path):return json.loads(Path(path).read_text())
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def write(path,data):
    from shared.artifacts import write_json
    write_json(Path(path),data)

def split_job(job,owners,max_rows=10000000):
    firsts=sorted(first for _,first in owners);sums=[0]
    for first in firsts:sums.append(sums[-1]+first)
    def prefix(b):
        n=bisect.bisect_left(firsts,b);return n*b-sums[n]
    result=[];a=job['start']
    while a<job['stop']:
        lo,hi=a+1,job['stop'];base=prefix(a)
        while lo<hi:
            mid=(lo+hi+1)//2
            if prefix(mid)-base<=max_rows:lo=mid
            else:hi=mid-1
        stop=lo
        result.append(dict(id=str(a)+'-'+str(stop-1),start=a,stop=stop,expected_rows=prefix(stop)-base));a=stop
    return result

def assign_jobs(jobs,weights):
    loads={host:0 for host in weights};assigned={}
    for job in sorted(jobs,key=lambda j:(-j['expected_rows'],j['start'])):
        host=min(weights,key=lambda h:(loads[h]/weights[h],h));assigned[job['id']]=host;loads[host]+=max(1,job['expected_rows'])
    return assigned

def merge_inventories(inventories):
    merged={}
    for inventory in inventories:
        for item in inventory['valid']:
            receipt=item['receipt'];key=receipt['job_id'];replica=dict(role=inventory['role'],path=item['path'],receipt_sha256=item.get('receipt_sha256'))
            if key in merged:
                prior=merged[key]['receipt']
                for field in ['start','stop','rows','sha256','generation','owner_first_seen_sha256']:
                    if prior[field]!=receipt[field]:raise ValueError('Conflicting recovered output identity '+key+' '+field)
                merged[key]['replicas'].append(replica)
            else:merged[key]=dict(receipt=receipt,replicas=[replica])
    return merged

def inventory_one(args):
    path,job=args
    import materialize_accounts as m
    try:
        receipt=m.check_receipt(path,job,OLD_GENERATION,ROOT)
        return dict(valid=True,path=str(path),receipt=receipt,receipt_sha256=sha(path))
    except Exception as e:return dict(valid=False,path=str(path),error=type(e).__name__+': '+str(e))

def inventory(state,role):
    from native_materialize import native_layout
    from snapshot_hf import verify
    verify('/home/dsc-data')
    image=Path('/home/dsc-data');
    if sha(image/'snapshot-manifest.json')!=IMAGE_SHA:raise ValueError('Wrong frozen input image')
    jobs=native_layout(16291127,26407882);paths=Path('/home/dsc-data/worker-state/receipts')/OLD_GENERATION
    tasks=[(paths/(j['id']+'.json'),j) for j in jobs if (paths/(j['id']+'.json')).exists()]
    with multiprocessing.get_context('spawn').Pool(1,maxtasksperchild=1) as pool:values=list(pool.imap_unordered(inventory_one,tasks))
    result=dict(role=role,checked_at=time.time(),image_sha256=IMAGE_SHA,factor_generation=FACTOR,valid=[{k:v for k,v in x.items() if k!='valid'} for x in values if x['valid']],excluded=[x for x in values if not x['valid']])
    write(Path(state)/'inventory.json',result);print(json.dumps(dict(role=role,valid=len(result['valid']),excluded=len(result['excluded']),rows=sum(x['receipt']['rows'] for x in result['valid']))),flush=True)

def make_plan(inventory_paths,target):
    import materialize_accounts as m
    from native_materialize import native_layout
    from shared.artifacts import digest
    recovered=merge_inventories([load(p) for p in inventory_paths]);original=native_layout(16291127,26407882);owners=m.owner_universe(ROOT,26407882)
    pending=[];coverage=[]
    for job in original:
        if job['id'] in recovered:coverage.append(dict(job,source='recovered'))
        else:
            parts=split_job(job,owners)
            pending.extend(parts);coverage.extend(dict(j,source='new') for j in parts)
    ordered=sorted(coverage,key=lambda j:j['start'])
    if ordered[0]['start']!=16291127 or ordered[-1]['stop']!=26407882 or any(a['stop']!=b['start'] for a,b in zip(ordered,ordered[1:])):raise ValueError('Failover coverage overlaps or omits blocks')
    weights=dict(node=5,revert=14,web=11);assignments=assign_jobs(pending,weights)
    for value in recovered.values():value['owner']=sorted(value['replicas'],key=lambda x:x['role'])[0]['role']
    sources={name:sha(Path(__file__).with_name(name)) for name in CODE_FILES}
    identity=dict(method='native_integer_independent_hosts_v1',factor_generation=FACTOR,snapshot_manifest_sha256=IMAGE_SHA,sources=sources,cutoff=26407881,max_rows_per_job=10000000,row_group_rows=100000,all_users=True,block_step=1)
    result=dict(identity=identity,generation=digest(identity),jobs=sorted(pending,key=lambda j:(0 if 16800000<=j['start']<16807201 else 1,j['start'])),assignments=assignments,recovered=recovered,coverage=ordered,inventory_identities={str(p):sha(p) for p in inventory_paths},created_at=time.time())
    write(target,result);print(json.dumps(dict(generation=result['generation'],new_jobs=len(pending),recovered_jobs=len(recovered),recovered_rows=sum(r['receipt']['rows'] for r in recovered.values()),assignments={host:sum(h==host for h in assignments.values()) for host in weights})),flush=True)

class LazyNativeAdapter:
    def __init__(self,root,db,plan,payload):
        self.args=(root,db,plan,payload['job']['start'],payload['job']['stop']);self.engine=None;self.block=None;self.generation=payload['factor_generation']
    def account(self,owner,block,include_balances=False):
        from native_batch import BatchEvaluator
        if self.engine is None:self.engine=BatchEvaluator(*self.args)
        for b in range(self.engine.start if self.block is None else self.block+1,block+1):self.engine.advance(b)
        self.block=block;return self.engine.account(owner,include_balances=include_balances)
    def close(self):
        if self.engine is not None:self.engine.close()

def direct_node(plan,payload):
    import contextlib,io,accounting_node
    from accounting_inputs import POOL
    from web3 import Web3,HTTPProvider
    request=dict(payload,end_block=plan['end_block'],end_hash=plan['end_hash'],pool=POOL)
    old_stdin=sys.stdin;old_web=accounting_node.web;output=io.StringIO()
    try:
        sys.stdin=io.StringIO(json.dumps(request));accounting_node.web=lambda:Web3(HTTPProvider(RPC,request_kwargs={'timeout':120}))
        with contextlib.redirect_stdout(output):accounting_node.main()
    finally:sys.stdin=old_stdin;accounting_node.web=old_web
    return json.loads(output.getvalue())

def run_one(payload):
    import accounting_inputs,accounting_query,native_batch,materialize_accounts as m
    directory=Path(payload['directory']);directory.mkdir(parents=True,exist_ok=True)
    write(directory/'worker.json',dict(pid=os.getpid(),started_at=time.time()))
    old_node=accounting_inputs.node;old_evaluator=accounting_query.Evaluator
    try:
        accounting_inputs.node=direct_node;accounting_query.Evaluator=lambda root,db,plan:LazyNativeAdapter(root,db,plan,payload)
        receipt=m.run_job(payload)
        return dict(ok=True,receipt=receipt,pid=os.getpid())
    except Exception as e:return dict(ok=False,job_id=payload['job']['id'],error=type(e).__name__+': '+str(e),pid=os.getpid())
    finally:accounting_inputs.node=old_node;accounting_query.Evaluator=old_evaluator

def run(manifest_path,state,role,workers,public,once=False):
    import materialize_accounts as m
    manifest=load(manifest_path);state=Path(state);state.mkdir(parents=True,exist_ok=True);public=Path(public);public.mkdir(parents=True,exist_ok=True)
    for name,identity in manifest['identity']['sources'].items():
        if sha(Path(__file__).with_name(name))!=identity:raise ValueError('Failover code differs from manifest '+name)
    if sha('/home/dsc-data/snapshot-manifest.json')!=IMAGE_SHA:raise ValueError('Wrong input snapshot')
    plan=load(ROOT/'plan.json');factor,waiting=m.factor_input(ROOT,plan,FACTOR)
    if waiting:raise ValueError(waiting)
    generation=manifest['generation'];receipts=state/'receipts'/generation;receipts.mkdir(parents=True,exist_ok=True)
    directory=Path(plan['output'])/'accounting/hf-numeric'/generation/'jobs'
    owned=[j for j in manifest['jobs'] if manifest['assignments'][j['id']]==role];accepted={};reused=[];active={};start=time.monotonic()
    def status(status,error=None):
        live=[]
        for job,async_result,payload in active.values():
            path=Path(payload['directory'])/'progress.json'
            if path.exists():live.append(load(path))
        combined=reused+list(accepted.values());numeric=sum(r['rows'] for r in combined)
        value=dict(role=role,epoch='20261115-independent',run_id='26407881',manifest_sha256=sha(manifest_path),generation=generation,factor_generation=FACTOR,state=status,error=error,updated_at=time.time(),start_block=16291127,end_block=26407881,total_blocks=10116755,completed_blocks=sum(r['stop']-r['start'] for r in combined),processed_rows=numeric,new_rows=sum(r['rows'] for r in accepted.values()),recovered_rows=sum(r['rows'] for r in reused),completed_jobs=len(combined),total_jobs=len(owned)+len(reused),validated_samples=sum(r['validated_samples'] for r in combined),mismatches=0,inflight_rows=sum(j.get('rows',0) for j in live),workers_configured=workers,active_compute_processes=len(active),current=dict(jobs=live),free_bytes=m.shutil.disk_usage(plan['output']).free,reserved_free_bytes=m.RESERVE_BYTES,elapsed_seconds=time.monotonic()-start,representation='Temporary host-local validated receipts plus explicitly recovered prior-generation worker outputs; all users, every block. Not the unavailable dsc-data acceptance tracker.')
        write(state/'progress.json',value);write(public/'status.json',value)
    with (state/'runner.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        try:
            for item in manifest['recovered'].values():
                if item['owner']!=role:continue
                replica=next(r for r in item['replicas'] if r['role']==role)
                if sha(replica['path'])!=replica['receipt_sha256']:raise ValueError('Recovered receipt changed')
                r=item['receipt'];m.check_receipt(replica['path'],dict(id=r['job_id'],start=r['start'],stop=r['stop']),r['generation'],ROOT);reused.append(r)
            for job in owned:
                path=receipts/(job['id']+'.json')
                if path.exists():accepted[job['id']]=m.check_receipt(path,job,generation,ROOT)
            ready=iter(j for j in owned if j['id'] not in accepted);finished=False
            with multiprocessing.get_context('spawn').Pool(workers,maxtasksperchild=1) as pool:
                def fill():
                    nonlocal finished
                    while len(active)<workers and not finished:
                        job=next(ready,None)
                        if job is None:finished=True;return
                        payload=dict(root=str(ROOT),plan=plan,job=job,generation=generation,factor_generation=FACTOR,factor=factor,directory=str(directory/job['id']),receipt=str(receipts/(job['id']+'.json')),row_group_rows=100000)
                        m.ensure_capacity(plan['output']);active[job['id']]=(job,pool.apply_async(run_one,(payload,)),payload)
                fill();last=0
                while active:
                    if time.monotonic()-last>=5:
                        try:
                            with socket.create_connection(('10.246.103.151',22),timeout=.3):pass
                        except OSError:pass
                        else:raise RuntimeError('Data host returned; pause temporary workers for reconciliation with the unchanged legacy services')
                    for key,(job,result,payload) in list(active.items()):
                        if not result.ready():continue
                        outcome=result.get()
                        if not outcome['ok']:raise ValueError(outcome['error'])
                        accepted[key]=m.check_receipt(payload['receipt'],job,generation,ROOT);del active[key]
                        if once:status('pilot_complete');return
                        fill()
                    if time.monotonic()-last>=5:status('materializing');last=time.monotonic()
                    time.sleep(1)
            status('complete')
        except Exception as e:status('awaiting_user_decision',type(e).__name__+': '+str(e));raise

def main():
    p=argparse.ArgumentParser();sub=p.add_subparsers(dest='action',required=True)
    inv=sub.add_parser('inventory');inv.add_argument('--state',required=True);inv.add_argument('--role',required=True)
    make=sub.add_parser('plan');make.add_argument('--inventory',action='append',required=True);make.add_argument('--output',required=True)
    runp=sub.add_parser('run');runp.add_argument('--manifest',required=True);runp.add_argument('--state',required=True);runp.add_argument('--role',required=True);runp.add_argument('--workers',type=int,required=True);runp.add_argument('--public',required=True);runp.add_argument('--once',action='store_true')
    a=p.parse_args()
    if a.action=='inventory':inventory(a.state,a.role)
    elif a.action=='plan':make_plan(a.inventory,a.output)
    else:run(a.manifest,a.state,a.role,a.workers,a.public,a.once)

if __name__=='__main__':main()
