"""Memory-only numerical HF tasks, using the frozen native evaluator unchanged."""
import bisect,hashlib,multiprocessing,os,resource,time
from pathlib import Path
from functools import lru_cache
import pyarrow as pa
from shared.artifacts import checksum,digest
import failover_hf as f
import materialize_accounts as m
from native_batch import BatchEvaluator
import hf_vectors as codec

@lru_cache(maxsize=1)
def universe():return m.owner_universe(f.ROOT,codec.STOP)

def compute(payload):
    started=time.monotonic();identity=payload['identity'];job=payload['job']
    if checksum('/home/dsc-data/snapshot-manifest.json')!=f.IMAGE_SHA:raise ValueError('Wrong frozen image')
    for name,value in identity['sources'].items():
        if checksum(Path(__file__).with_name(name))!=value:raise ValueError('Wrong task code: '+name)
    plan=m.load(f.ROOT/'plan.json');factor,waiting=m.factor_input(f.ROOT,plan,f.FACTOR)
    if waiting:raise ValueError(waiting)
    owners=universe();registry=digest(owners)
    if registry!=identity['registry_digest']:raise ValueError('Wrong owner dictionary')
    firsts=[x[1] for x in owners];n=bisect.bisect_left(firsts,job['stop']);expected=m.row_count(owners[:n],job['start'],job['stop'])
    db=m.readonly(f.ROOT/'accounting.sqlite');ev=None;parts=[];samples={};total=0;ids=[];blocks=[];hfs=[]
    def flush():
        nonlocal ids,blocks,hfs
        if not ids:return
        raw=pa.table({'entity_id':pa.array(ids,type=pa.uint32()),'block_number':pa.array(blocks,type=pa.uint32()),'hf_values':pa.array(hfs,type=pa.binary())})
        vectors=codec.pack(raw);decoded=codec.unpack(vectors)
        if not codec.canonical(decoded).equals(codec.canonical(raw)):raise ValueError('Lossless HF encoding differs')
        data=codec.dumps(vectors)
        parts.append(dict(data=data,sha256=hashlib.sha256(data).hexdigest(),logical_sha256=codec.logical_sha(raw),hf_values=len(ids),segments=vectors.num_rows))
        ids=[];blocks=[];hfs=[];pa.default_memory_pool().release_unused()
    try:
        if expected:
            ev=BatchEvaluator(f.ROOT,db,plan,job['start'],job['stop']);checkpoints={job['start'],(job['start']+job['stop']-1)//2,job['stop']-1}
            for block in range(job['start'],job['stop']):
                ev.advance(block)
                for uid in range(bisect.bisect_right(firsts,block)):
                    owner=owners[uid][0];value=ev.account(owner);account=value['account']
                    ids.append(uid);blocks.append(block);hfs.append(codec.raw_hf(account[5]));total+=1
                    if block in checkpoints:
                        label='debt' if int(account[1]) else 'no_debt';key=str(block)+':'+label
                        if key not in samples:samples[key]=dict(user=owner,entity_id=uid,block=block,profile=value['profile'],block_hash=value['block_hash'],account=account)
                    if len(ids)>=identity['buffer_values']:flush()
            flush()
        if total!=expected:raise ValueError('Logical HF universe differs')
        return dict(job=job,generation=payload['generation'],identity=identity,role=os.environ['DSC_HF_ROLE'],registry_digest=registry,owner_count=n,owner_first_seen_sha256=digest(owners[:n]),rows=total,parts=parts,samples=list(samples.values()),factor_input=factor,elapsed_seconds=time.monotonic()-started,peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    finally:
        if ev is not None:ev.close()
        db.close()

def task(payload):
    """One fresh child per task bounds allocator and evaluator lifetimes."""
    from distributed import get_worker
    with multiprocessing.get_context('spawn').Pool(1,maxtasksperchild=1) as pool:result=pool.apply(compute,(payload,))
    result['worker']=get_worker().address
    return result
