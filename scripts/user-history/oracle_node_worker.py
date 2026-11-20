"""Cryo extraction and ABI readback for one immutable oracle interval on node."""
import fcntl,json,signal,sys,time
from pathlib import Path
sys.path.insert(0,'/home/node/dsc-extraction/scripts')
from web3 import Web3,HTTPProvider
from eth_abi import decode
import pyarrow as pa
import pyarrow.parquet as pq
from shared.artifacts import write_json,checksum,digest
from oracle_common import ORACLE_ABI,raw_files
from pipeline.runtime import run as run_command
sys.path.insert(0,'/home/node/dsc-extraction/scripts/event-cryo-extraction')
from node import inventory


def run(plan,job):
    source_sha256=checksum(__file__)
    root=Path(job['node_dir']);root.mkdir(parents=True,exist_ok=True)
    with (root/'lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        identity=digest(dict(job=job,end_hash=plan['end_hash'],chunk_size=plan['chunk_size']))
        if (root/'identity.json').exists() and json.loads((root/'identity.json').read_text())['digest']!=identity:raise ValueError('Oracle job identity changed')
        write_json(root/'identity.json',dict(digest=identity))
        w=Web3(HTTPProvider('http://127.0.0.1:8545',request_kwargs={'timeout':60}))
        assert w.eth.chain_id==1 and w.eth.get_block(plan['end_block']).hash.to_0x_hex()==plan['end_hash']
        first_hash=w.eth.get_block(job['start']).hash.to_0x_hex();last_hash=w.eth.get_block(job['stop']-1).hash.to_0x_hex()
        raw=Path(job.get('reuse_raw',root/'raw'));raw.mkdir(parents=True,exist_ok=True)
        decoded=root/'decoded';decoded.mkdir(exist_ok=True)
        count=job['stop']-job['start'];started=time.monotonic()
        def completed_blocks():
            blocks=0 if job['assets'] else count
            for path in raw_files(raw) if job['assets'] else []:
                try:blocks+=pq.read_metadata(path).num_rows
                except (OSError,pa.ArrowInvalid):pass  # Cryo may still be writing its footer.
            return blocks
        def progress(state,error=None):
            blocks=completed_blocks()
            write_json(root/'progress.json',dict(state=state,updated_at=time.time(),id=job['id'],completed_blocks=blocks,total_blocks=count,asset_count=len(job['assets']),requests=plan['requests'],elapsed_seconds=time.monotonic()-started,error=error))
        files=[];zero_values=0;checks=[]
        if job['assets']:
            oracle=w.eth.contract(address=job['oracle'],abi=ORACLE_ABI)
            data=oracle.functions.getAssetsPrices(job['assets'])._encode_transaction_data()
            if not job.get('reuse_raw'):
                # Reuse the active event runner's interrupted-Parquet quarantine
                # before Cryo's file-exists resume can skip an incomplete file.
                inventory(raw,{p.name:0 for p in raw_files(raw)})
                command=['/home/node/.cargo/bin/cryo','eth_calls','--rpc','http://127.0.0.1:8545','--contract',job['oracle'],'--call-data',data,'--blocks',f"{job['start']}:{job['stop']}",'--chunk-size',str(plan['chunk_size']),'--max-concurrent-requests',str(plan['requests']),'--max-concurrent-chunks','2','--max-retries','5','--output-dir',str(raw),'--report-dir',str(root/'reports'),'--no-verbose']
                run_command(command,root/'cryo.log',lambda:progress('extracting'),progress=completed_blocks)
            seen=set();references={};schema=pa.schema([('block_number',pa.uint32()),('prices',pa.list_(pa.string()))])
            for path in raw_files(raw):
                rows=[]
                for row in pq.read_table(path).to_pylist():
                    b=row['block_number']
                    if not job['start']<=b<job['stop'] or b in seen:raise ValueError('Missing/duplicate/out-of-range oracle block')
                    if row['chain_id']!=1 or row['contract_address'].hex().lower()!=job['oracle'][2:].lower() or row['call_data'].hex().lower()!=data[2:].lower():raise ValueError('Oracle raw call identity differs')
                    prices=decode(['uint256[]'],row['output_data'])[0]
                    if len(prices)!=len(job['assets']):raise ValueError('Price vector length differs')
                    zero_values+=sum(v==0 for v in prices);seen.add(b)
                    if b in {job['start'],(job['start']+job['stop']-1)//2,job['stop']-1}:references[b]=list(prices)
                    rows.append(dict(block_number=b,prices=[str(v) for v in prices]))
                target=decoded/path.name;temporary=target.with_suffix('.tmp')
                pq.write_table(pa.Table.from_pylist(rows,schema=schema),temporary,compression='zstd')
                if pq.read_table(temporary).to_pylist()!=rows:raise ValueError('Decoded oracle readback differs')
                temporary.replace(target)
                files.append(dict(name=path.name,rows=len(rows),start=min(r['block_number'] for r in rows),stop=max(r['block_number'] for r in rows)+1,raw_sha256=checksum(path),decoded_sha256=checksum(target),raw_bytes=path.stat().st_size,decoded_bytes=target.stat().st_size))
                progress('decoding')
            if seen!=set(range(job['start'],job['stop'])):raise ValueError('Incomplete oracle interval')
            for block,expected in references.items():
                actual=oracle.functions.getAssetsPrices(job['assets']).call(block_identifier=block)
                if actual!=expected:raise ValueError('Node bulk oracle re-read differs')
                for i in sorted({0,len(expected)-1}):
                    value=oracle.functions.getAssetPrice(job['assets'][i]).call(block_identifier=block)
                    checks.append(dict(block=block,asset=job['assets'][i],expected=str(expected[i]),actual=str(value),exact=value==expected[i]))
            if not all(c['exact'] for c in checks):raise ValueError('Individual oracle getter differs')
        else:
            from oracle_common import POOL,POOL_ABI
            pool=w.eth.contract(address=POOL,abi=POOL_ABI)
            assert not pool.functions.getReservesList().call(block_identifier=job['start'])
            assert not pool.functions.getReservesList().call(block_identifier=job['stop']-1)
        assert w.eth.get_block(job['start']).hash.to_0x_hex()==first_hash and w.eth.get_block(job['stop']-1).hash.to_0x_hex()==last_hash
        manifest=dict(identity=identity,worker_sha256=source_sha256,job=job,block_count=count,price_values=count*len(job['assets']),zero_price_values=zero_values,raw_root=str(raw),decoded_root=str(decoded),files=files,checks=checks,empty_market=not job['assets'],start_hash=first_hash,end_hash=last_hash,finished_at=time.time(),validation='Complete block coverage, ABI/readback, endpoint hashes and sampled direct getters; zero prices retained and flagged, never fabricated')
        write_json(root/'manifest.json',manifest);progress('extracted')

if __name__=='__main__':
    def interrupted(signum,frame):raise SystemExit(128+signum)
    for s in [signal.SIGTERM,signal.SIGHUP,signal.SIGINT]:signal.signal(s,interrupted)
    plan=json.loads(Path(sys.argv[1]).read_text());run(plan,next(j for j in plan['jobs'] if j['id']==sys.argv[2]))
