#!/usr/bin/env python3
"""Bounded native-tool benchmarks on dsc-node; never writes production coverage."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
from urllib.request import Request, urlopen

SCRIPTS = Path(__file__).resolve().parents[1]
POOLS = [
    ('uniswap-v2', '0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc'),
    ('uniswap-v2', '0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852'),
    ('uniswap-v3', '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640'),
    ('uniswap-v3', '0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8'),
]


def rpc(method, params):
    request = Request('http://127.0.0.1:8545', json.dumps(
        {'jsonrpc':'2.0','id':1,'method':method,'params':params}).encode(),
        {'Content-Type':'application/json'})
    with urlopen(request, timeout=10) as response:
        result = json.load(response)
    if 'error' in result:
        raise ValueError(result['error'])
    return result['result']


def counters():
    cpu = list(map(int, Path('/proc/stat').read_text().splitlines()[0].split()[1:9]))
    disk = next(line.split()[3:] for line in Path('/proc/diskstats').read_text().splitlines()
                if line.split()[2] == 'vda')
    mem = dict(line.split(':', 1) for line in Path('/proc/meminfo').read_text().splitlines())
    return cpu, list(map(int, disk)), int(mem['MemAvailable'].split()[0]) / 1024**2


def monitor(stop, samples):
    old_cpu, old_disk, _ = counters()
    last = time.monotonic()
    while not stop.wait(1):
        cpu, disk, available = counters()
        now = time.monotonic()
        delta = [a-b for a,b in zip(cpu,old_cpu)]
        ticks = max(sum(delta), 1)
        ios = disk[0]+disk[4]-old_disk[0]-old_disk[4]
        samples.append({'cpu_busy_pct':100*(ticks-delta[3]-delta[4])/ticks,
                        'iowait_pct':100*delta[4]/ticks,
                        'disk_busy_pct':100*(disk[9]-old_disk[9])/((now-last)*1000),
                        'disk_await_ms':(disk[3]+disk[7]-old_disk[3]-old_disk[7])/max(ios,1),
                        'available_gib':available})
        old_cpu, old_disk, last = cpu, disk, now


def execute(command, log, env=None):
    started = time.monotonic()
    with log.open('w') as stream:
        proc = subprocess.Popen(command, stdout=stream, stderr=subprocess.STDOUT,
                                env=env, start_new_session=True)
        try:
            code = proc.wait(timeout=180)
        except BaseException:
            os.killpg(proc.pid, signal.SIGTERM)
            proc.wait(timeout=10)
            raise
    if code:
        raise RuntimeError(f'command failed ({code}); see {log}')
    return time.monotonic()-started


def state_job(item, args, root):
    import pyarrow.parquet as pq
    protocol, address = item
    output = root/address
    env = dict(os.environ, OUTPUT_DIR=str(output), PYTHON_BIN=sys.executable,
               MAX_CONCURRENT_REQUESTS=str(args.requests), MAX_CONCURRENT_CHUNKS='8',
               CHUNK_SIZE=str(args.chunk))
    elapsed = execute(['bash',str(SCRIPTS/'extraction/extract_pool_state.sh'),protocol,
                       address,f'{args.start}:{args.start+args.blocks}'], root/(address+'.log'),env)
    rows = []
    for path in output.glob('*.parquet'):
        rows += pq.ParquetFile(path).read(columns=[
            'block_number','contract_address','call_data','output_data','chain_id']).to_pylist()
    rows.sort(key=lambda row:(row['block_number'],row['call_data']))
    return {'address':address,'seconds':elapsed,'rows':len(rows),
            'digest':hashlib.sha256(json.dumps(rows,sort_keys=True).encode()).hexdigest()}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--kind', choices=['state','trace','headers'], default='state')
    p.add_argument('--root', type=Path, required=True)
    p.add_argument('--start', type=int, default=20000000)
    p.add_argument('--blocks', type=int, default=200)
    p.add_argument('--pool-workers', type=int, choices=[1,2,4], default=1)
    p.add_argument('--requests', type=int, choices=[8,16,32], default=8)
    p.add_argument('--chunk', type=int, default=1000)
    p.add_argument('--trace-workers', type=int, choices=[2,4,8], default=2)
    args = p.parse_args()
    args.root.mkdir(parents=True, exist_ok=False)
    if rpc('eth_syncing',[]) is not False:
        raise RuntimeError('node is syncing; skip benchmark')
    samples=[]; stop=threading.Event()
    thread=threading.Thread(target=monitor,args=(stop,samples));thread.start()
    started=time.monotonic(); result={'settings':vars(args)|{'root':str(args.root)}}
    try:
        if args.kind=='state':
            with ThreadPoolExecutor(max_workers=args.pool_workers) as pool:
                result['cases']=list(pool.map(lambda item:state_job(item,args,args.root),POOLS))
            units=args.blocks*len(POOLS)
        elif args.kind=='trace':
            execute([sys.executable,str(SCRIPTS/'reverted/extract_reverted.py'),
                     f'{args.start}:{args.start+args.blocks}','--output',str(args.root/'archive'),
                     '--workers',str(args.trace_workers)],args.root/'trace.log')
            sys.path.insert(0,str(SCRIPTS))
            from reverted.extract_reverted import load,validate_archive
            result['cases']=[]
            for block in range(args.start,args.start+args.blocks):
                data=validate_archive(load(args.root/'archive/blocks'/f'{block//1000:09d}'/f'{block}.json.gz'))
                data.pop('elapsed_seconds',None)
                result['cases'].append({'block':block,'digest':hashlib.sha256(
                    json.dumps(data,sort_keys=True).encode()).hexdigest()})
            units=args.blocks
        else:
            sys.path.insert(0,str(SCRIPTS))
            from orchestration.worker import headers,rpc as web3_rpc
            result['headers']=len(headers(web3_rpc(),args.start,args.start+args.blocks))
            units=args.blocks
        result.update(valid=True,units_per_second=units/(time.monotonic()-started))
    except Exception as exc:
        result.update(valid=False,error=str(exc),units_per_second=0)
    finally:
        result['seconds']=time.monotonic()-started
        stop.set();thread.join()
    result['samples']=samples
    result['healthy']=rpc('eth_syncing',[]) is False and all(
        s['available_gib']>16 and s['disk_await_ms']<100 for s in samples)
    (args.root/'result.json').write_text(json.dumps(result,indent=2)+'\n')
    assert json.loads((args.root/'result.json').read_text())==result
    print(json.dumps({k:v for k,v in result.items() if k not in ('samples','cases')},sort_keys=True))
    if not result['valid'] or not result['healthy']:
        raise SystemExit(1)


if __name__=='__main__':
    main()
