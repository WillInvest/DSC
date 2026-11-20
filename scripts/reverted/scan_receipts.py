"""Receipt-only scan, bounded concurrency and immutable compressed block batches."""
import argparse
import concurrent.futures as cf
import fcntl
import gzip
import json
import os
from pathlib import Path
import shutil
import sys
import threading
import time
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from reverted.common import make_rpc
from shared.artifacts import checksum,digest,write_json

LOCAL=threading.local()

def receipt_record(number):
    if not hasattr(LOCAL,'rpc'): LOCAL.rpc=make_rpc('http://127.0.0.1:8545')
    receipts=LOCAL.rpc('eth_getBlockReceipts',[hex(number)])
    if not isinstance(receipts,list): raise ValueError('Receipts are not a list')
    block_hash=receipts[0]['blockHash'] if receipts else None
    failed=[];seen=set()
    for index,r in enumerate(receipts):
        if (int(r['blockNumber'],16)!=number or r['blockHash']!=block_hash
            or int(r['transactionIndex'],16)!=index or r['transactionHash'] in seen
            or r['status'] not in ('0x0','0x1')): raise ValueError('Receipt identity/status mismatch')
        seen.add(r['transactionHash'])
        if r['status']=='0x0': failed.append(r)
    if not receipts:
        header=LOCAL.rpc('eth_getBlockByNumber',[hex(number),False])
        if int(header['number'],16)!=number or header['transactions']: raise ValueError('Empty receipts for nonempty block')
        block_hash=header['hash']
    return dict(block_number=number,block_hash=block_hash,transaction_count=len(receipts),
                failed_transaction_count=len(failed),failed_index_sum=sum(int(r['transactionIndex'],16)+1 for r in failed),failed_receipts=failed)


def batch(pool,start,stop,heartbeat=None):
    pending={pool.submit(receipt_record,n) for n in range(start,stop)};rows=[]
    while pending:
        done,pending=cf.wait(pending,timeout=2,return_when=cf.FIRST_COMPLETED)
        rows.extend(f.result() for f in done)
        if heartbeat: heartbeat(len(rows))
    return sorted(rows,key=lambda r:r['block_number'])


def scan(args):
    root=args.output;root.mkdir(parents=True,exist_ok=True)
    for folder in ('chunks','metadata'): (root/folder).mkdir(exist_ok=True)
    rpc=make_rpc('http://127.0.0.1:8545')
    assert int(rpc('eth_chainId',[]),16)==1
    assert int(rpc('eth_getBlockByNumber',['finalized',False])['number'],16)>=args.stop-1
    end_hash=getattr(args,'end_hash',None)
    if end_hash and rpc('eth_getBlockByNumber',[hex(args.stop-1),False])['hash']!=end_hash:
        raise ValueError('Captured finalized target hash changed')
    scope=dict(schema=1,chain_id=1,start=args.start,stop=args.stop,chunk_size=1000,stage='receipts_only')
    if end_hash: scope['end_hash']=end_hash
    identity=digest(scope)
    if (root/'plan.json').exists() and json.loads((root/'plan.json').read_text())!=scope: raise ValueError('Different receipt scan scope')
    write_json(root/'plan.json',scope)
    status=dict(state='running',start=args.start,stop=args.stop,total_blocks=args.stop-args.start,
                completed_blocks=0,failed_transactions=0,compressed_bytes=0,workers=args.workers,started_at=time.time(),inflight_completed=0)
    begun=time.monotonic();fresh=0;last=0
    def publish(force=False):
        nonlocal last
        if not force and time.monotonic()-last<2:return
        last=time.monotonic();status.update(updated_at=time.time(),blocks_per_second=fresh/max(.001,time.monotonic()-begun))
        write_json(root/'progress.json',status)
    try:
        with (cf.ProcessPoolExecutor if args.executor=='process' else cf.ThreadPoolExecutor)(max_workers=args.workers) as pool:
            for start in range(args.start,args.stop,1000):
                stop=min(start+1000,args.stop);name=f'{start}-{stop-1}'
                path=root/'chunks'/(name+'.jsonl.gz');meta=root/'metadata'/(name+'.json')
                status.update(current_start=start,current_stop=stop,inflight_completed=0)
                if meta.exists():
                    info=json.loads(meta.read_text())
                    if info['identity']!=identity or checksum(path)!=info['sha256']:raise ValueError('Invalid existing receipt batch')
                else:
                    if shutil.disk_usage(root).free<20*1024**3:raise RuntimeError('Receipt disk free space below 20 GiB')
                    def tick(count):status.update(inflight_completed=count);publish()
                    rows=batch(pool,start,stop,tick)
                    temp=path.with_suffix('.tmp')
                    with gzip.open(temp,'wt',compresslevel=1) as stream:
                        for row in rows:stream.write(json.dumps(row,separators=(',',':'))+'\n')
                    with temp.open('rb') as stream:os.fsync(stream.fileno())
                    temp.replace(path)
                    info=dict(identity=identity,start=start,stop=stop,blocks=len(rows),
                              failed_transactions=sum(r['failed_transaction_count'] for r in rows),
                              bytes=path.stat().st_size,sha256=checksum(path))
                    write_json(meta,info);fresh+=len(rows)
                status['completed_blocks']+=info['blocks'];status['failed_transactions']+=info['failed_transactions'];status['compressed_bytes']+=info['bytes']
                status['inflight_completed']=0;publish(True)
        status['state']='complete';publish(True)
    except Exception as error:
        status.update(state='error',error=str(error));publish(True);raise


def benchmark(args):
    results=[]
    # Reverse order on a second pass reduces a simple warm-cache ordering bias.
    for round_number,levels in enumerate(([8,16,32,64],[64,32,16,8]),1):
        for workers in levels:
            started=time.monotonic();failed=0
            with (cf.ProcessPoolExecutor if args.executor=='process' else cf.ThreadPoolExecutor)(max_workers=workers) as pool:
                for start in (11000000,18000000,26265000):
                    rows=batch(pool,start,start+100);failed+=sum(r['failed_transaction_count'] for r in rows)
            elapsed=time.monotonic()-started
            result=dict(executor=args.executor,round=round_number,workers=workers,blocks=300,seconds=elapsed,blocks_per_second=300/elapsed,failed_transactions=failed)
            results.append(result);print(json.dumps(result),flush=True)
            write_json(args.output,results)


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--benchmark',action='store_true');parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--start',type=int,default=10008355);parser.add_argument('--stop',type=int,default=26266552);parser.add_argument('--workers',type=int,default=16)
    parser.add_argument('--executor',choices=['thread','process'],default='thread')
    parser.add_argument('--end-hash')
    args=parser.parse_args()
    if args.benchmark: benchmark(args)
    else:
        if args.start<4370000 or args.stop<=args.start or args.workers<1:parser.error('Invalid range or worker count')
        args.output.mkdir(parents=True,exist_ok=True)
        with (args.output/'.lock').open('w') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);scan(args)
