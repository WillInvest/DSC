"""Data-owned resumable oracle backfill; node extracts, data accepts artifacts."""
import fcntl,json,subprocess,sys,time
from pathlib import Path
sys.path.insert(0,'/home/dsc-data/scripts')
import pyarrow.parquet as pq
from shared.artifacts import write_json,checksum,digest
from pipeline.runtime import ssh,NODE,NODE_PYTHON
from prepare_oracle import prepare,ROOT


def run():
    ROOT.mkdir(parents=True,exist_ok=True)
    with (ROOT/'lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        path=prepare();root=path.parent;plan=json.loads(path.read_text())
        states_path=root/'jobs.json';states=json.loads(states_path.read_text()) if states_path.exists() else {}
        started=time.monotonic();initial=sum(s.get('blocks',0) for s in states.values() if s.get('state')=='accepted' and s.get('price_values',0)>0)
        def publish(state,current=None,error=None):
            done=sum(s.get('blocks',0) for s in states.values() if s.get('state')=='accepted')
            prices=sum(s.get('price_values',0) for s in states.values() if s.get('state')=='accepted')
            priced=sum(s.get('blocks',0) for s in states.values() if s.get('state')=='accepted' and s.get('price_values',0)>0)
            elapsed=time.monotonic()-started;rate=(priced-initial)/max(elapsed,1)
            write_json(root/'progress.json',dict(state=state,scope=plan['scope'],run_id=plan['run_id'],start_block=plan['start_block'],end_block=plan['end_block'],end_hash=plan['end_hash'],total_blocks=plan['end_block']-plan['start_block']+1,accepted_blocks=done,price_values=prices,accepted_intervals=sum(s.get('state')=='accepted' for s in states.values()),total_intervals=len(plan['jobs']),requests=plan['requests'],blocks_per_second=rate,elapsed_seconds=elapsed,current=current,updated_at=time.time(),error=error,user_HF_complete=False))
        for job in plan['jobs']:
            old=states.get(job['id'],{})
            if old.get('state')=='accepted':
                receipt=Path(old['receipt']);m=json.loads(receipt.read_text())
                if checksum(receipt)!=old['receipt_sha256'] or any(checksum(Path(job['destination'])/kind/f['name'])!=f[kind+'_sha256'] for f in m['files'] for kind in ['raw','decoded']):raise ValueError('Accepted oracle receipt/output identity changed')
                continue
            destination=Path(job['destination']);destination.mkdir(parents=True,exist_ok=True)
            try:
                with (root/(job['id']+'.log')).open('a') as log:
                    child=subprocess.Popen(ssh(NODE,[NODE_PYTHON,'/home/node/dsc-extraction/scripts/user-history/oracle_node_worker.py','/home/node/dsc-extraction/aave-oracle-v3/'+plan['run_id']+'/plan.json',job['id']]),stdout=log,stderr=subprocess.STDOUT)
                    try:
                        while child.poll() is None:
                            status=subprocess.run(ssh(NODE,['cat',job['node_dir']+'/progress.json']),capture_output=True,text=True,timeout=30)
                            current=json.loads(status.stdout) if status.returncode==0 else dict(id=job['id'],state='starting',completed_blocks=0)
                            publish('collecting',current);time.sleep(5)
                        if child.returncode:raise RuntimeError('Oracle node worker failed; see interval log')
                    finally:
                        if child.poll() is None:child.terminate();child.wait(timeout=60)
                result=subprocess.run(ssh(NODE,['cat',job['node_dir']+'/manifest.json']),capture_output=True,text=True,check=True)
                m=json.loads(result.stdout)
                if m['identity']!=digest(dict(job=job,end_hash=plan['end_hash'],chunk_size=plan['chunk_size'])):raise ValueError('Oracle manifest identity differs')
                publish('transferring',dict(id=job['id'],state='transferring',completed_blocks=m['block_count'],total_blocks=m['block_count']))
                for kind in ['raw','decoded']:
                    (destination/kind).mkdir(exist_ok=True)
                    subprocess.run(['rsync','-a','--partial','-e','ssh -o BatchMode=yes -o ConnectTimeout=10',NODE+':'+m[kind+'_root']+'/',str(destination/kind)+'/'],check=True,stdout=subprocess.DEVNULL)
                for f in m['files']:
                    for kind in ['raw','decoded']:
                        target=destination/kind/f['name']
                        if checksum(target)!=f[kind+'_sha256'] or pq.read_metadata(target).num_rows!=f['rows']:raise ValueError('Destination oracle hash/row count differs')
                    table=pq.read_table(destination/'decoded'/f['name']);blocks=table['block_number'].to_pylist()
                    if len(set(blocks))!=f['rows'] or set(blocks)!=set(range(f['start'],f['stop'])):raise ValueError('Destination oracle block coverage differs')
                    if any(len(v)!=len(job['assets']) for v in table['prices'].to_pylist()):raise ValueError('Destination oracle price vector differs')
                receipt=root/'receipts'/(job['id']+'.json');write_json(receipt,m)
                states[job['id']]=dict(state='accepted',blocks=m['block_count'],price_values=m['price_values'],zero_price_values=m['zero_price_values'],receipt=str(receipt),receipt_sha256=checksum(receipt),finished_at=time.time())
                write_json(states_path,states);publish('collecting')
            except Exception as exc:
                states[job['id']]=dict(state='failed',error=str(exc),updated_at=time.time());write_json(states_path,states);publish('failed',dict(id=job['id']),str(exc));raise
        publish('complete')

if __name__=='__main__':run()
