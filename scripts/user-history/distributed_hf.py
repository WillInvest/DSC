"""Dask execution adapter for the existing numerical HF producer.

Calculation code, generations, grids and acceptance checks remain unchanged.
Workers see a frozen input image at its original absolute paths through a
systemd mount namespace. Only dsc-data publishes production receipts.
"""
import argparse,fcntl,json,os,sqlite3,sys,time
from collections import Counter
from concurrent.futures import wait,FIRST_COMPLETED
from pathlib import Path

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))

def backup_sqlite(source,destination):
    source,destination=Path(source),Path(destination)
    destination.parent.mkdir(parents=True,exist_ok=True)
    if destination.exists():raise FileExistsError(destination)
    with sqlite3.connect('file:'+str(source.resolve())+'?mode=ro',uri=True) as src:
        with sqlite3.connect(destination) as dst:
            src.backup(dst,pages=4096)
            dst.execute('pragma journal_mode=delete')
            if dst.execute('pragma quick_check').fetchone()!=('ok',):raise ValueError('Invalid SQLite input snapshot')

def worker_count(config):
    values=[v['workers'] for v in config['hosts'].values()]
    if any(not isinstance(x,int) or x<1 for x in values) or not 1<=sum(values)<=32:raise ValueError('Expected 1..32 explicitly assigned compute slots')
    return sum(values)

def publish_result(result,destination,job,generation,root,check=None):
    """Validate received bytes before the sole authoritative receipt publication."""
    from shared.artifacts import write_json
    if check is None:
        from materialize_accounts import check_receipt
        check=check_receipt
    destination=Path(destination)
    if destination.exists():raise FileExistsError(destination)
    temporary=destination.with_name(destination.name+'.incoming')
    write_json(temporary,result)
    checked=check(temporary,job,generation,root)
    # Atomic and exclusive: interruption cannot expose a partial receipt and a
    # second delivery cannot replace an already accepted result.
    os.link(temporary,destination)
    temporary.unlink()
    return checked

def node_reference(plan,payload):
    from accounting_inputs import node
    return node(plan,payload)

class RecyclingEvaluator:
    """Bound the lifetime of local EVM caches; each account is independent."""
    def __init__(self,factory,*args,limit=500):
        self.factory,self.args,self.limit=factory,args,limit;self.used=0
        self.current=factory(*args);self.generation=self.current.generation
    def account(self,*args,**kwargs):
        if self.used>=self.limit:
            self.current.close();self.current=self.factory(*self.args);self.used=0
            if self.current.generation!=self.generation:raise ValueError('Calculation generation changed while recycling runtime')
        value=self.current.account(*args,**kwargs);self.used+=1;return value
    def close(self):
        if self.current is not None:self.current.close();self.current=None

def remote_job(payload):
    from distributed import get_client,get_worker
    import accounting_inputs
    import accounting_query
    from materialize_accounts import run_job
    role=os.environ['DSC_HF_ROLE'];directory=Path(payload['directory']);directory.mkdir(parents=True,exist_ok=True)
    saved=accounting_inputs.node;original_evaluator=accounting_query.Evaluator
    def compare(plan,request):
        # This reserved data worker alone owns node SSH access. Worker images
        # contain no copied SSH keys, credentials or application databases.
        return get_client().submit(node_reference,plan,request,pure=False,resources={'validator':1}).result()
    private=dict(payload,receipt='/home/dsc-data/worker-state/receipts/'+payload['generation']+'/'+payload['job']['id']+'.json')
    Path(private['receipt']).parent.mkdir(parents=True,exist_ok=True)
    try:
        accounting_inputs.node=compare
        accounting_query.Evaluator=lambda *args:RecyclingEvaluator(original_evaluator,*args)
        with (directory/'distributed.lock').open('a') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX)
            receipt=run_job(private)
        return dict(receipt=receipt,role=role,hostname=os.uname().nodename,worker=get_worker().address)
    finally:
        accounting_inputs.node=saved;accounting_query.Evaluator=original_evaluator

def worker_reports(payloads):
    reports={}
    for payload in payloads:
        p=Path(payload['directory'])/'progress.json'
        if p.exists():
            r=json.loads(p.read_text());r['host']=os.environ.get('DSC_HF_ROLE','validation');reports[payload['job']['id']]=r
    return reports

def delivery_workers(workers,result):
    """Each role's workers share its output mount; other hosts do not."""
    peers=[address for address,worker in workers.items()
           if worker.get('resources',{}).get('compute') and str(worker['name']).split('-')[0]==result['role']]
    if not peers:raise ValueError('No attested host peers remain for numerical output delivery')
    return peers

class DeliveredFuture:
    def __init__(self,executor,future,payload):self.executor,self.future,self.payload=executor,future,payload;self.value=None
    def done(self):self.executor.refresh();return self.future.done()
    def result(self):
        if self.value is not None:return self.value
        import dask,fsspec
        result=self.future.result();payload=self.payload
        directory=Path(payload['directory']);directory.mkdir(parents=True,exist_ok=True)
        receipt=result['receipt']
        from materialize_accounts import ensure_capacity
        ensure_capacity(directory,receipt['bytes']+2**20)
        names=['accounts.parquet','output.json',Path(receipt['validation']).name]
        fs=fsspec.filesystem('dask',target_protocol='file',client=None,skip_instance_cache=True)
        # fsspec streams bounded file ranges over the authenticated Dask channel.
        # Worker images intentionally have no data-to-web SSH access or keys.
        peers=delivery_workers(self.executor.client.scheduler_info(n_workers=-1)['workers'],result)
        with dask.annotate(workers=peers,allow_other_workers=False):
            for name in names:
                target=directory/name;temporary=target.with_name(name+'.transferring')
                fs.get_file(str(directory/name),str(temporary));temporary.replace(target)
        receipt=dict(receipt,execution_host=result['role'],execution_hostname=result['hostname'],distributed_adapter_sha256=self.executor.adapter_sha)
        self.value=publish_result(receipt,payload['receipt'],payload['job'],payload['generation'],payload['root'])
        return self.value

class DistributedExecutor:
    def __init__(self,client,**ignored):
        self.client=client;self.executor=client.get_executor(pure=False,resources={'compute':1});self.payloads=[];self.refreshed=0
        from shared.artifacts import checksum
        self.adapter_sha=checksum(__file__)
    def __enter__(self):return self
    def __exit__(self,*exc):self.executor.shutdown(wait=not bool(exc[0]))
    def submit(self,function,payload):
        self.payloads.append(payload)
        return DeliveredFuture(self,self.executor.submit(remote_job,payload),payload)
    def refresh(self):
        if time.monotonic()-self.refreshed<10:return
        self.refreshed=time.monotonic()
        from shared.artifacts import write_json
        info=self.client.scheduler_info(n_workers=-1);selected={}
        for address,w in info['workers'].items():
            if not w.get('resources',{}).get('compute'):continue
            role=str(w['name']).split('-')[0]
            selected.setdefault(role,address)
        reports=self.client.run(worker_reports,self.payloads,workers=list(selected.values())) if selected else {}
        newest={}
        for values in reports.values():
            for key,value in values.items():
                if key not in newest or newest[key]['updated_at']<value['updated_at']:newest[key]=value
        for payload in self.payloads:
            key=payload['job']['id']
            if key in newest and not Path(payload['receipt']).exists():write_json(Path(payload['directory'])/'progress.json',newest[key])

def rolling_jobs(submit,payloads,slots,accept,progress):
    """Refill a free slot without waiting for a slower machine's whole batch."""
    iterator=iter(payloads);pending={}
    def fill():
        while len(pending)<slots:
            payload=next(iterator,None)
            if payload is None:return
            wrapped=submit(payload);pending[wrapped.future]=wrapped
    fill();progress([v.payload for v in pending.values()])
    while pending:
        completed,_=wait(pending,timeout=5,return_when=FIRST_COMPLETED)
        for future in completed:
            wrapped=pending.pop(future);accept(wrapped.result());fill()
        progress([v.payload for v in pending.values()])

def submission_phase(ready,accepted):
    from materialize_accounts import SVB_START,SVB_STOP
    svb=[j for j in ready if SVB_START<=j['start']<SVB_STOP]
    selected=svb or ready
    return selected if any(r['rows'] for r in accepted) else selected[:1]

def run_distributed(client,config):
    """Keep the original grids/checks; heterogeneous hosts need rolling dispatch."""
    import materialize_accounts as m
    from accounting_inputs import ROOT
    from accounting_query import Evaluator
    from shared.artifacts import checksum,digest,write_json
    current=m.load(ROOT/'current.json');root=Path(current['root']);plan=m.load(current['plan']);slots=worker_count(config);jobs=m.layout(m.START,plan['end_block']+1)
    started=time.monotonic();accepted={};active=[];generation=None;factor_generation=None;directory=None
    def status(state,waiting=None,error=None):
        counts=m.counts(list(accepted.values()),m.START,plan['end_block']+1);live=[]
        for payload in active:
            p=Path(payload['directory'])/'progress.json'
            if p.exists():live.append(m.load(p))
        value=dict(state=state,run_id=plan['run_id'],generation=generation,factor_generation=factor_generation,start_block=m.START,end_block=plan['end_block'],total_blocks=plan['end_block']-m.START+1,total_jobs=len(jobs),**counts,numeric_rows_materialized=counts['processed_rows'],inflight_rows=sum(v.get('rows',0) for v in live),mismatches=sum(v.get('mismatches',0) for v in live),updated_at=time.time(),error=error,waiting_for=waiting or [],current={'jobs':live,'workers':slots},representation='Numerical whole-account HF rows; all users seen by each block, including no-debt accounts',elapsed_seconds=time.monotonic()-started,rows_per_second=None,full_eta_seconds=None,free_bytes=m.shutil.disk_usage(plan['output']).free,reserved_free_bytes=m.RESERVE_BYTES)
        write_json(root/'hf-progress.json',value)
    with (root/'hf-numeric.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        try:
            db=m.readonly(root/'accounting.sqlite');ev=None
            try:ev=Evaluator(root,db,plan);factor_generation=ev.generation
            finally:
                if ev is not None:ev.close()
                db.close()
            # This is the original producer's numerical identity, not a new
            # mathematical method or a relabeling of accepted old receipts.
            generation=digest(dict(calculation=factor_generation,materializer=checksum(m.__file__),plan=checksum(current['plan']),chunk_size=100,priority=[m.SVB_START,m.SVB_STOP],all_users=True))
            directory=Path(plan['output'])/'accounting/hf-numeric'/generation;receipts=root/'hf-numeric'/generation/'receipts';receipts.mkdir(parents=True,exist_ok=True)
            jobmap={j['id']:j for j in jobs}
            for path in sorted(receipts.glob('*.json')):
                if path.stem not in jobmap:raise ValueError('Unrecognized accepted numerical interval '+str(path))
                accepted[path.stem]=m.check_receipt(path,jobmap[path.stem],generation,root)
            while True:
                factor,waiting=m.factor_input(root,plan,factor_generation)
                if factor is None:status('waiting_for_inputs',[waiting]);time.sleep(30);continue
                ready=[j for j in jobs if j['id'] not in accepted and j['stop']<=factor['through']+1]
                if not ready:
                    if len(accepted)==len(jobs):status('complete');return
                    status('waiting_for_inputs',['Waiting for additional whole-account factor coverage']);time.sleep(30);continue
                ready=submission_phase(ready,accepted.values())
                owners=m.owner_universe(root,max(j['stop'] for j in ready))
                with DistributedExecutor(client) as executor:
                    def submit(job):
                        ratios=[r['bytes']/r['rows'] for r in list(accepted.values())[-8:] if r['rows']]
                        outstanding=[v['job'] for v in active]+[job]
                        projected=sum(max(8192,int(m.row_count(owners,j['start'],j['stop'])*max(ratios))) for j in outstanding) if ratios else 0
                        m.ensure_capacity(plan['output'],projected)
                        payload=dict(root=str(root),plan=plan,job=job,generation=generation,factor_generation=factor_generation,factor=factor,directory=str(directory/'jobs'/job['id']),receipt=str(receipts/(job['id']+'.json')))
                        active.append(payload);return executor.submit(m.run_job,payload)
                    def accept(receipt):
                        accepted[receipt['job_id']]=receipt
                        active[:]=[p for p in active if p['job']['id']!=receipt['job_id']]
                    def progress(payloads):
                        active[:]=payloads;executor.refresh();status('materializing' if active else 'validating')
                    rolling_jobs(submit,ready,slots,accept,progress)
        except m.CapacityWait as exc:status('awaiting_user_decision',[str(exc)],str(exc))
        except Exception as exc:status('failed',error=type(exc).__name__+': '+str(exc));raise

def security(config):
    from distributed import Security
    return Security(**json.loads(Path(config['security']).read_text()))

def worker_attestation():
    from shared.artifacts import checksum
    path=Path('/home/dsc-data/snapshot-manifest.json');manifest=json.loads(path.read_text())
    return dict(role=os.environ['DSC_HF_ROLE'],manifest_sha256=checksum(path),factor_generation=manifest['factor_generation'])

def verify_attestations(config,attestations):
    roles=Counter(v['role'] for v in attestations.values())
    if dict(roles)!={k:v['workers'] for k,v in config['hosts'].items()}:raise ValueError('Live worker role inventory differs from the plan')
    for value in attestations.values():
        if value['manifest_sha256']!=config['snapshot_manifest_sha256'] or value['factor_generation']!=config['factor_generation']:raise ValueError('Live worker snapshot identity differs')

def probe_accounts(tasks):
    """Bounded deployment check, kept separate from numerical job acceptance."""
    from accounting_inputs import ROOT
    from accounting_query import Evaluator
    from materialize_accounts import readonly,load
    current=load(ROOT/'current.json');root=Path(current['root']);plan=load(current['plan']);db=readonly(root/'accounting.sqlite');ev=Evaluator(root,db,plan)
    try:return dict(role=os.environ['DSC_HF_ROLE'],generation=ev.generation,rows=[ev.account(t['user'],t['block'],include_balances=False) for t in tasks])
    finally:ev.close();db.close()

def main(config_path):
    import shared.artifacts as artifacts
    from distributed import Client
    config=json.loads(Path(config_path).read_text());workers=worker_count(config)
    with Client(config['scheduler'],security=security(config)) as client:
        client.wait_for_workers(workers+1,timeout=120)
        info=client.scheduler_info(n_workers=-1)['workers']
        if sum(int(w.get('resources',{}).get('compute',0)) for w in info.values())!=workers:raise ValueError('Live distributed compute capacity differs from declared slots')
        if sum(int(w.get('resources',{}).get('validator',0)) for w in info.values())!=1:raise ValueError('Exactly one node-validation worker is required')
        compute=[a for a,w in info.items() if w.get('resources',{}).get('compute')]
        verify_attestations(config,client.run(worker_attestation,workers=compute))
        original_writer=artifacts.write_json
        def write_status(path,value):
            if Path(path).name=='hf-progress.json':
                live=client.scheduler_info(n_workers=-1)['workers'];online=Counter(str(w['name']).split('-')[0] for w in live.values() if w.get('resources',{}).get('compute'))
                value=dict(value,distributed=dict(engine='Dask '+__import__('distributed').__version__,hosts={k:v['workers'] for k,v in config['hosts'].items()},online_hosts=dict(online),scheduler=config['scheduler'],snapshot_manifest_sha256=config['snapshot_manifest_sha256']))
            return original_writer(path,value)
        artifacts.write_json=write_status
        try:run_distributed(client,config)
        finally:artifacts.write_json=original_writer

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--config',required=True);a=p.parse_args();main(a.config)
