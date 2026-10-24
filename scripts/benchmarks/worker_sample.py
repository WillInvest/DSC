#!/usr/bin/env python3
"""Compare complete node workers, including both canonical header checks."""
import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import sys
import threading
import time

from sweep import POOLS, SCRIPTS, execute, monitor, rpc


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root',type=Path,required=True)
    p.add_argument('--workers',type=int,choices=[1,2,4,8,16],default=1)
    p.add_argument('--requests',type=int,choices=[4,8,16,32],default=8)
    p.add_argument('--contracts',type=Path,help='Fixed DEX pool registry for larger sweeps')
    p.add_argument('--start',type=int,default=20000000)
    p.add_argument('--blocks',type=int,default=250)
    args=p.parse_args();args.root.mkdir(parents=True,exist_ok=False)
    sys.path.insert(0,str(SCRIPTS))
    from orchestration.coordinator import fingerprint, registry
    from orchestration.artifacts import verify
    import pyarrow.parquet as pq
    pools = POOLS
    if args.contracts:
        rows = registry(args.contracts)
        if any(r['protocol'] not in ('uniswap-v2','uniswap-v3') or
               r['creation_block'] > args.start for r in rows):
            raise ValueError('benchmark requires deployed Uniswap pools')
        pools = [(r['protocol'],r['address']) for r in rows]

    def job(item):
        protocol,address=item;output=args.root/address
        spec={'kind':'state','chain_id':1,'protocol':protocol,'address':address,
              'start':args.start,'stop':args.start+args.blocks,'profile':None,
              'config':fingerprint('state',protocol)}
        request=base64.b64encode(json.dumps({'action':'extract','spec':spec,'output':str(output)}).encode()).decode()
        elapsed=execute([sys.executable,str(SCRIPTS/'orchestration/worker.py'),request],
                        args.root/(address+'.log'),dict(os.environ,MAX_CONCURRENT_REQUESTS=str(args.requests)))
        verify(output,spec)
        rows=[]
        for path in output.glob('*.parquet'):
            rows+=pq.ParquetFile(path).read(columns=['block_number','contract_address','call_data','output_data','chain_id']).to_pylist()
        rows.sort(key=lambda row:(row['block_number'],row['call_data']))
        return {'address':address,'seconds':elapsed,'rows':len(rows),
                'digest':hashlib.sha256(json.dumps(rows,sort_keys=True).encode()).hexdigest()}

    assert rpc('eth_syncing',[]) is False
    samples=[];stop=threading.Event();thread=threading.Thread(target=monitor,args=(stop,samples));thread.start()
    started=time.monotonic();result={'settings':{k:str(v) if isinstance(v,Path) else v for k,v in vars(args).items()},'workload':'complete-node-state-workers'}
    try:
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            result['cases']=list(pool.map(job,pools))
        result['valid']=True
    except Exception as exc:
        result.update(valid=False,error=str(exc))
    finally:
        stop.set();thread.join()
    result.update(seconds=time.monotonic()-started,samples=samples)
    result['healthy']=rpc('eth_syncing',[]) is False and all(s['available_gib']>16 and s['disk_await_ms']<100 for s in samples)
    result['units_per_second']=len(pools)*args.blocks/result['seconds'] if result['valid'] else 0
    (args.root/'result.json').write_text(json.dumps(result,indent=2)+'\n')
    assert json.loads((args.root/'result.json').read_text())==result
    print(json.dumps({k:v for k,v in result.items() if k not in ('samples','cases')}))
    if not result['valid'] or not result['healthy']:
        raise SystemExit(1)


if __name__=='__main__':
    main()
