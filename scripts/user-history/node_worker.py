"""Separate, resumable Cryo supplement worker; never touches the main queue."""
import argparse,fcntl,json,subprocess,sys,time,signal
from pathlib import Path
sys.path.insert(0,'/home/node/dsc-extraction/scripts')
sys.path.insert(0,'/home/node/dsc-extraction/scripts/event-cryo-extraction')
from node import inventory,report_successes
from common import chunks
from shared.artifacts import checksum,write_json
import pyarrow.parquet as pq
from web3 import Web3,HTTPProvider

def run(plan,job):
    root=Path(job['node_dir']);root.mkdir(parents=True,exist_ok=True)
    with (root/'lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        identity={k:plan[k] for k in ['end_block','end_hash','chunk_size']};identity.update(address=job['address'],start_block=job['start_block'])
        if (root/'identity.json').exists() and json.loads((root/'identity.json').read_text())!=identity:raise ValueError('Identity changed')
        write_json(root/'identity.json',identity)
        w=Web3(HTTPProvider('http://127.0.0.1:8545',request_kwargs={'timeout':60}))
        assert w.eth.chain_id==1 and w.eth.get_block(plan['end_block']).hash.to_0x_hex()==plan['end_hash']
        if not w.eth.get_code(Web3.to_checksum_address(job['address']),block_identifier=plan['end_block']):raise ValueError('Target has no contract code')
        output=root/'encoded';reports=root/'reports';output.mkdir(exist_ok=True);reports.mkdir(exist_ok=True)
        expected=chunks(job,plan)
        def progress(state,error=None):
            found=inventory(output,expected);empty=report_successes(reports,expected)
            write_json(root/'progress.json',dict(state=state,updated_at=time.time(),address=job['address'],written_chunks=len(set(found)|empty),total_chunks=len(expected),rows=sum(v['rows'] for v in found.values()),error=error))
        found=inventory(output,expected);evidence=report_successes(reports,expected)
        if set(found)|evidence!=set(expected):
            log=(root/'cryo.log').open('a')
            child=subprocess.Popen([plan['cryo'],'logs','--rpc','http://127.0.0.1:8545','--contract',job['address'],'--blocks',f"{job['start_block']}:{plan['end_block']+1}",'--chunk-size',str(plan['chunk_size']),'--inner-request-size','1000','--max-concurrent-requests','2','--max-concurrent-chunks','2','--include-columns','block_hash','--hex','--output-dir',str(output),'--report-dir',str(reports)],stdout=log,stderr=subprocess.STDOUT)
            try:
                while child.poll() is None:progress('extracting');time.sleep(10)
                if child.returncode:raise RuntimeError('Cryo failed: '+str(child.returncode))
            finally:
                if child.poll() is None:child.terminate();child.wait(timeout=60)
                log.close()
        found=inventory(output,expected);evidence=report_successes(reports,expected)
        if set(found)|evidence!=set(expected):raise ValueError('Missing output/empty-chunk evidence')
        files={}
        for name,info in found.items():
            path=output/name;bounds=name.removesuffix('.parquet').split('__')[-1].split('_to_');first,last=map(int,bounds)
            seen=set()
            for batch in pq.ParquetFile(path).iter_batches(columns=['block_number','address','block_hash','log_index']):
                for row in batch.to_pylist():
                    if row['address'].lower()!=job['address'].lower() or not first<=row['block_number']<=last or not row['block_hash']:raise ValueError('Output value out of scope')
                    key=(row['block_number'],row['log_index'])
                    if key in seen:raise ValueError('Duplicate log')
                    seen.add(key)
            files[name]={**info,'sha256':checksum(path),'start':first,'stop':last+1}
        assert w.eth.get_block(plan['end_block']).hash.to_0x_hex()==plan['end_hash']
        write_json(root/'manifest.json',dict(identity=identity,files=files,empty_chunks=sorted(set(expected)-set(files)),total_chunks=len(expected),validation='raw event scope, duplicates and checksums; user reconstruction not validated'))
        progress('extracted')

if __name__=='__main__':
    def interrupted(signum,frame):raise SystemExit(128+signum)
    for signum in [signal.SIGTERM,signal.SIGHUP,signal.SIGINT]:signal.signal(signum,interrupted)
    p=argparse.ArgumentParser();p.add_argument('plan');p.add_argument('address');a=p.parse_args();plan=json.loads(Path(a.plan).read_text());run(plan,next(j for j in plan['jobs'] if j['address']==a.address))
