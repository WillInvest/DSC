"""Data-owned supplemental extraction queue, isolated from production coverage."""
import fcntl,json,subprocess,sys,time,shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
sys.path.insert(0,'/home/dsc-data/scripts')
from pipeline.runtime import ssh,NODE,NODE_PYTHON
from shared.artifacts import checksum,write_json
import pyarrow.parquet as pq

ROOT=Path('/home/dsc-data/runs/aave-user-history-v3')

def selected_jobs(plan,policy):
    assets=policy.get('priority_assets',[])
    if not isinstance(assets,list) or len(assets)!=len(set(assets)) or any(a not in {j['asset'] for j in plan['jobs']} for a in assets):raise ValueError('Invalid priority asset list')
    if policy.get('defer_other_assets') and not assets:raise ValueError('Cannot defer all assets without an explicit focus')
    rank={a:i for i,a in enumerate(assets)}
    selected=[j for j in plan['jobs'] if not policy.get('defer_other_assets') or j['asset'] in rank]
    return sorted(selected,key=lambda j:rank.get(j['asset'],len(rank)))

def run():
    current=json.loads((ROOT/'current.json').read_text());root=Path(current['root']);plan=json.loads(Path(current['plan']).read_text())
    with (root/'queue.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        state_path=root/'jobs.json';states=json.loads(state_path.read_text()) if state_path.exists() else {}
        policy_path=root/'priority.json'
        def policy():return json.loads(policy_path.read_text()) if policy_path.exists() else {}
        for address,state in list(states.items()):
            if state.get('state')=='running':states[address]={**state,'state':'interrupted','note':'Previous coordinator stopped; node artifacts retained for resume'}
        def publish(status='running'):
            control=policy();eligible=selected_jobs(plan,control);focus=set(control.get('priority_assets',[]));focus_jobs=[j for j in plan['jobs'] if j['asset'] in focus]
            write_json(state_path,states)
            write_json(root/'progress.json',dict(state=status,stage='supplemental_token_events',updated_at=time.time(),end_block=plan['end_block'],end_hash=plan['end_hash'],completed_addresses=sum(x.get('state')=='raw_accepted' for x in states.values()),failed_addresses=sum(x.get('state')=='failed' for x in states.values()),total_addresses=len(plan['jobs']),priority_assets=control.get('priority_assets',[]),priority_labels=control.get('labels',{}),priority_total_addresses=len(focus_jobs),priority_completed_addresses=sum(states.get(j['address'],{}).get('state')=='raw_accepted' for j in focus_jobs),deferred_addresses=sum(j not in eligible and states.get(j['address'],{}).get('state')!='raw_accepted' for j in plan['jobs']),user_history_complete=False,current_jobs=[a for a,s in states.items() if s.get('state')=='running'],next_stages=plan['next_stages']))
        def collect(job):
            address=job['address'];destination=Path(job['destination']);destination.mkdir(parents=True,exist_ok=True)
            with (root/(address+'.log')).open('a') as log:
                command=ssh(NODE,[NODE_PYTHON,'/home/node/dsc-extraction/scripts/user-history/node_worker.py',plan['node_plan'],address])
                subprocess.run(command,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=86400)
                subprocess.run(['rsync','-a','--partial','-e','ssh -o BatchMode=yes -o ConnectTimeout=10',NODE+':'+job['node_dir']+'/encoded/',str(destination)+'/'],check=True,stdout=log,stderr=subprocess.STDOUT,timeout=7200)
            r=subprocess.run(ssh(NODE,['cat',job['node_dir']+'/manifest.json']),check=True,capture_output=True,text=True);manifest=json.loads(r.stdout)
            expected=dict(end_block=plan['end_block'],end_hash=plan['end_hash'],chunk_size=plan['chunk_size'],address=address,start_block=job['start_block'])
            if manifest['identity']!=expected:raise ValueError('Transfer identity differs')
            for name,info in manifest['files'].items():
                p=destination/name
                if checksum(p)!=info['sha256'] or pq.read_metadata(p).num_rows!=info['rows']:raise ValueError('Transfer checksum/row mismatch')
            write_json(root/'receipts'/(address+'.json'),manifest)
            return dict(state='raw_accepted',rows=sum(x['rows'] for x in manifest['files'].values()),files=len(manifest['files']),finished_at=time.time(),receipt=str(root/'receipts'/(address+'.json')),validation='raw integrity only; positions/HF pending')
        def accepted(job):
            old=states.get(job['address'],{})
            if old.get('state')!='raw_accepted':return False
            receipt=Path(old['receipt'])
            if not receipt.exists():return False
            manifest=json.loads(receipt.read_text())
            return all((Path(job['destination'])/n).exists() and (Path(job['destination'])/n).stat().st_size==v['bytes'] for n,v in manifest['files'].items())
        remaining={j['address']:j for j in plan['jobs'] if not accepted(j)}
        if shutil.disk_usage('/home/dsc-data/data').free<50*1024**3:raise RuntimeError('Less than 50 GiB free')
        with ThreadPoolExecutor(max_workers=2) as executor:
            pending={}
            def submit():
                candidates=[j for j in selected_jobs(plan,policy()) if j['address'] in remaining]
                if not candidates:return False
                j=remaining.pop(candidates[0]['address'])
                states[j['address']]=dict(state='running',started_at=time.time());pending[executor.submit(collect,j)]=j;return True
            submit();submit();publish()
            while pending:
                ready=[f for f in pending if f.done()]
                if not ready:publish();time.sleep(10);continue
                for future in ready:
                    j=pending.pop(future)
                    try:states[j['address']]=future.result()
                    except Exception as exc:states[j['address']]=dict(state='failed',error=str(exc),finished_at=time.time())
                    submit();publish()
        eligible=selected_jobs(plan,policy());failures=any(states.get(j['address'],{}).get('state')=='failed' for j in eligible)
        publish('failed' if failures else ('priority_inputs_collected' if policy().get('defer_other_assets') else 'supplemental_events_collected'))
        if failures:raise RuntimeError('Supplemental extraction failures; same scope resumes on restart')
if __name__=='__main__':run()
