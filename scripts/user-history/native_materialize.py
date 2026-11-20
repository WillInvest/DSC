"""Bulk local HF producer using existing immutable inputs and acceptance APIs.

Dask.upload_file distributes the small, identified calculation modules, leaving
frozen input images unchanged. Numerical generations differ from reference EVM
outputs. Only data publishes accepted receipts; node is a sampled validator.
"""
import argparse,fcntl,json,os,time
from pathlib import Path
import materialize_accounts as m
from distributed_hf import DistributedExecutor,DeliveredFuture,security,worker_count,verify_attestations,worker_attestation,node_reference
from shared.artifacts import checksum,digest,write_json

FILES=('native_hf.py','native_batch.py','native_materialize.py')

def native_layout(start,stop):
    from accounting_inputs import profiles
    points={start,stop,m.SVB_START,m.SVB_START+1,m.SVB_STOP}
    points.update(range(start,stop,10000))
    for p in profiles():points.update([p['from_block']-1,p['from_block']])
    ordered=sorted(p for p in points if start<=p<=stop)
    jobs=[dict(id=str(a)+'-'+str(b-1),start=a,stop=b) for a,b in zip(ordered,ordered[1:])]
    return sorted(jobs,key=lambda j:(0 if m.SVB_START<=j['start']<m.SVB_STOP else 1,j['start']))

class NativeAdapter:
    def __init__(self,root,db,plan,payload):
        from native_batch import BatchEvaluator
        self.generation=payload['factor_generation'];self.block=None
        self.engine=BatchEvaluator(root,db,plan,payload['job']['start'],payload['job']['stop'])
    def account(self,owner,block,include_balances=False):
        for b in range(self.engine.start if self.block is None else self.block+1,block+1):self.engine.advance(b)
        self.block=block
        return self.engine.account(owner,include_balances=include_balances)
    def close(self):self.engine.close()

def remote_native_job(payload):
    from distributed import get_client,get_worker
    import accounting_inputs,accounting_query
    import native_batch  # Bind reference bootstrap helpers before substitution.
    # Every worker has one compute thread; scope substitutions to this task.
    saved_node=accounting_inputs.node;saved_evaluator=accounting_query.Evaluator
    def compare(plan,request):return get_client().submit(node_reference,plan,request,pure=False,resources={'validator':1}).result()
    private=dict(payload,receipt='/home/dsc-data/worker-state/receipts/'+payload['generation']+'/'+payload['job']['id']+'.json')
    Path(private['receipt']).parent.mkdir(parents=True,exist_ok=True)
    Path(payload['directory']).mkdir(parents=True,exist_ok=True)
    try:
        accounting_inputs.node=compare
        accounting_query.Evaluator=lambda root,db,plan:NativeAdapter(root,db,plan,payload)
        with (Path(payload['directory'])/'distributed.lock').open('a') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX)
            receipt=m.run_job(private)
        return dict(receipt=receipt,role=os.environ['DSC_HF_ROLE'],hostname=os.uname().nodename,worker=get_worker().address)
    finally:accounting_inputs.node=saved_node;accounting_query.Evaluator=saved_evaluator

class NativeExecutor(DistributedExecutor):
    def __init__(self,client):
        super().__init__(client);self.adapter_sha=checksum(__file__)
    def submit(self,function,payload):
        self.payloads.append(payload)
        Path(payload['directory']).mkdir(parents=True,exist_ok=True)
        return DeliveredFuture(self,self.executor.submit(remote_native_job,payload),payload)

def main(config_path,once=False):
    from distributed import Client
    from distributed_hf import rolling_jobs
    from accounting_inputs import ROOT
    config=m.load(config_path);current=m.load(ROOT/'current.json');root=Path(current['root']);plan=m.load(current['plan'])
    factor_generation=config['factor_generation'];factor,waiting=m.factor_input(root,plan,factor_generation)
    if waiting:raise ValueError(waiting)
    if factor['through']!=plan['end_block']:raise ValueError('Full input history must be ready')
    sources={name:checksum(Path(__file__).with_name(name)) for name in FILES}
    identity=dict(method='native_integer_sparse_replay_v1',sources=sources,factor_generation=factor_generation,snapshot_manifest_sha256=config['snapshot_manifest_sha256'],materializer=checksum(m.__file__),plan_sha256=checksum(current['plan']),checkpoint_blocks=10000,all_users=True)
    generation=digest(identity);jobs=native_layout(m.START,plan['end_block']+1);jobmap={j['id']:j for j in jobs}
    directory=Path(plan['output'])/'accounting/hf-numeric'/generation;receipts=root/'hf-numeric'/generation/'receipts';receipts.mkdir(parents=True,exist_ok=True)
    accepted={};active=[];started=time.monotonic();base_rows=0
    write_json(receipts.parent/'calculation.json',dict(identity,generation=generation,jobs=jobs))
    def status(state,error=None):
        c=m.counts(accepted.values(),m.START,plan['end_block']+1);live=[]
        for payload in active:
            path=Path(payload['directory'])/'progress.json'
            if path.exists():live.append(m.load(path))
        elapsed=max(.001,time.monotonic()-started)
        write_json(root/'hf-progress.json',dict(state=state,run_id=plan['run_id'],generation=generation,factor_generation=factor_generation,start_block=m.START,end_block=plan['end_block'],total_blocks=plan['end_block']-m.START+1,total_jobs=len(jobs),**c,numeric_rows_materialized=c['processed_rows'],inflight_rows=sum(v.get('rows',0) for v in live),mismatches=sum(v.get('mismatches',0) for v in live),updated_at=time.time(),error=error,waiting_for=[],current={'jobs':live,'workers':worker_count(config)},representation='Numerical all-user every-block HF; native integer calculation, node sampled every <=10000 blocks and upgrade boundaries',calculation_method='native_integer_sparse_replay_v1',elapsed_seconds=elapsed,rows_per_second=max(0,c['processed_rows']-base_rows)/elapsed,full_eta_seconds=None,free_bytes=m.shutil.disk_usage(plan['output']).free,reserved_free_bytes=m.RESERVE_BYTES,distributed=dict(engine='Dask '+__import__('distributed').__version__,hosts={k:v['workers'] for k,v in config['hosts'].items()},online_hosts=online,scheduler=config['scheduler'],snapshot_manifest_sha256=config['snapshot_manifest_sha256'])))
    online={}
    with (root/'hf-numeric.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        try:
            for path in sorted(receipts.glob('*.json')):
                if path.stem not in jobmap:raise ValueError('Unknown native numeric receipt')
                accepted[path.stem]=m.check_receipt(path,jobmap[path.stem],generation,root)
            base_rows=sum(r['rows'] for r in accepted.values())
            with Client(config['scheduler'],security=security(config)) as client:
                client.wait_for_workers(worker_count(config)+1,timeout=120)
                info=client.scheduler_info(n_workers=-1)['workers'];compute=[a for a,w in info.items() if w.get('resources',{}).get('compute')]
                verify_attestations(config,client.run(worker_attestation,workers=compute))
                for address in compute:
                    role=str(info[address]['name']).split('-')[0];online[role]=online.get(role,0)+1
                for name in FILES:client.upload_file(str(Path(__file__).with_name(name)))
                while True:
                    ready=[j for j in jobs if j['id'] not in accepted]
                    if not ready:status('complete');return
                    svb=[j for j in ready if m.SVB_START<=j['start']<m.SVB_STOP]
                    selected=svb or ready
                    if not accepted:selected=selected[:1]
                    owners=m.owner_universe(root,max(j['stop'] for j in selected))
                    with NativeExecutor(client) as executor:
                        def submit(job):
                            ratios=[r['bytes']/r['rows'] for r in list(accepted.values())[-8:] if r['rows']]
                            outstanding=[p['job'] for p in active]+[job]
                            projected=sum(int(m.row_count(owners,j['start'],j['stop'])*max(ratios)) for j in outstanding) if ratios else 0
                            m.ensure_capacity(plan['output'],projected)
                            payload=dict(root=str(root),plan=plan,job=job,generation=generation,factor_generation=factor_generation,factor=factor,directory=str(directory/'jobs'/job['id']),receipt=str(receipts/(job['id']+'.json')))
                            active.append(payload);return executor.submit(m.run_job,payload)
                        def accept(receipt):
                            accepted[receipt['job_id']]=receipt;active[:]=[p for p in active if p['job']['id']!=receipt['job_id']]
                        def progress(payloads):
                            active[:]=payloads;executor.refresh();status('materializing' if active else 'validating')
                        rolling_jobs(submit,selected,worker_count(config),accept,progress)
                    if once:return
        except Exception as exc:status('awaiting_user_decision' if isinstance(exc,m.CapacityWait) else 'failed',str(exc));raise

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--config',required=True);p.add_argument('--once',action='store_true');a=p.parse_args();main(a.config,a.once)
