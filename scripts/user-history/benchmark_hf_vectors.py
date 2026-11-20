"""HF-only vector format pilot. Keeps exact HF; other metrics remain in factors.

Uses Arrow ordered hash-list aggregation, list flattening, parent indices and
Parquet. IDs/offset semantics and uint256 byte adaptation are project-specific.
"""
import gc,json,resource,sqlite3,sys,time
from pathlib import Path
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from benchmark_lossless import sha,save,write
ORIGIN=16291127
STOP=26407882
SPAN=4096
MAX=2**256-1

def encode(values,encoding):
    if encoding=='string':return values
    data=[]
    for value in values.to_pylist():
        n=int(value)
        if str(n)!=value or not 0<=n<=MAX:raise ValueError('Noncanonical uint256')
        data.append(n.to_bytes(32 if encoding=='binary32' else max(1,(n.bit_length()+7)//8),'big'))
    return pa.array(data,type=pa.binary(32) if encoding=='binary32' else pa.binary())

def pack(source,registry,encoding):
    ids=pc.index_in(source['user'],value_set=registry['user'])
    if ids.null_count:raise ValueError('Unknown user ID')
    blocks=source['block_number'].cast(pa.uint32())
    if pc.any(pc.less(blocks,ORIGIN)).as_py() or pc.any(pc.greater_equal(blocks,STOP)).as_py():raise ValueError('Outside frozen target')
    table=pa.table({'user_id':ids.cast(pa.uint32()),'block_number':blocks,'hf':encode(source['healthFactor'],encoding)})
    table=table.sort_by([('user_id','ascending'),('block_number','ascending')])
    unique=table.group_by(['user_id','block_number']).aggregate([('hf','count')])
    if unique.num_rows!=table.num_rows:raise ValueError('Duplicate user-block key')
    same_user=pc.equal(table['user_id'].slice(1),table['user_id'].slice(0,table.num_rows-1))
    consecutive=pc.equal(table['block_number'].slice(1),pc.add(table['block_number'].slice(0,table.num_rows-1),1))
    if pc.any(pc.and_(same_user,pc.invert(consecutive))).as_py():raise ValueError('Gap within user history sample')
    offset=pc.subtract(table['block_number'],pa.scalar(ORIGIN,pa.uint32()))
    bucket=pc.divide(offset,pa.scalar(SPAN,pa.uint32())).cast(pa.uint32())
    table=table.append_column('bucket',bucket)
    groups=table.group_by(['user_id','bucket'],use_threads=False).aggregate([('block_number','min'),('block_number','max'),('block_number','count'),('hf','list')])
    length=pc.list_value_length(groups['hf_list']).cast(pa.int64())
    if not pc.all(pc.equal(length,pc.add(pc.subtract(groups['block_number_max'],groups['block_number_min']),1))).as_py():raise ValueError('Gap within user vector')
    if not pc.all(pc.equal(length,groups['block_number_count'])).as_py() or pc.max(length).as_py()>SPAN:raise ValueError('Vector length differs')
    vectors=pa.table({'user_id':groups['user_id'],'start_offset':pc.subtract(groups['block_number_min'],pa.scalar(ORIGIN,pa.uint32())),'hf':groups['hf_list']})
    return vectors.sort_by([('user_id','ascending'),('start_offset','ascending')])

def unpack(vectors,registry,encoding):
    values_array=vectors['hf'].combine_chunks()
    lengths=pc.list_value_length(values_array).cast(pa.int64())
    parents=pc.list_parent_indices(values_array)
    starts=pc.subtract(pc.cumulative_sum(lengths),lengths)
    n=pc.sum(lengths).as_py()
    offset=pc.subtract(pa.array(range(n),type=pa.int64()),pc.take(starts,parents))
    blocks=pc.add(pc.add(pc.take(vectors['start_offset'],parents),ORIGIN),offset).cast(pa.uint32())
    ids=pc.take(vectors['user_id'],parents)
    flat=pc.list_flatten(values_array)
    values=flat if encoding=='string' else pa.array([str(int.from_bytes(x,'big')) for x in flat.to_pylist()],type=pa.string())
    return pa.table({'user':pc.take(registry['user'],ids),'block_number':blocks,'healthFactor':values})

def check():
    registry=pa.table({'user':['a','b']})
    rows=[]
    for user in ['a','b']:
        for block in range(ORIGIN+4094,ORIGIN+4101):
            rows.append(dict(user=user,block_number=block,healthFactor=str(MAX if user=='a' else 10**18+block)))
    table=pa.Table.from_pylist(rows).cast(pa.schema([('user',pa.string()),('block_number',pa.uint32()),('healthFactor',pa.string())]))
    for encoding in ['string','binary_min','binary32']:
        vectors=pack(table,registry,encoding);assert vectors.num_rows==4
        assert unpack(vectors,registry,encoding).equals(table)
    gap=table.take(pa.array([0,2]))
    try:pack(gap,registry,'string')
    except ValueError:pass
    else:raise AssertionError('Gap was accepted')
    duplicate=pa.concat_tables([table,table.slice(0,1)])
    try:pack(duplicate,registry,'string')
    except ValueError:pass
    else:raise AssertionError('Duplicate was accepted')

def main():
    config=json.loads(Path(sys.argv[1]).read_text());root=Path(config['vector_output']);root.mkdir(exist_ok=False);check()
    db=sqlite3.connect('file:/home/dsc-data/runs/aave-user-history-v3/26407881/position-owners.sqlite?mode=ro&immutable=1',uri=True)
    owners=db.execute('select lower(owner),min(first_block) from owners where lower(owner)!=? group by lower(owner) having min(first_block)<? order by min(first_block),lower(owner)',('0x'+'0'*40,STOP)).fetchall();db.close()
    registry=pa.table({'user_id':pa.array(range(len(owners)),type=pa.uint32()),'user':[r[0] for r in owners],'first_offset':pa.array([r[1]-ORIGIN for r in owners],type=pa.uint32())})
    write(registry,root/'users.parquet',9)
    save(root/'dataset.json',dict(format=config.get('format_id','hf-vector-pilot-v1'),physical_columns=['entity_id','start_offset','hf_values'],sample_only=True,value_encoding=config.get('value_encoding','see per-file variant names'),entity_type='user',block_origin=ORIGIN,stop_exclusive=STOP,block_step=1,max_values_per_segment=SPAN,raw_hf_scale=str(10**18),uint256_max=str(MAX),registry_sha256=sha(root/'users.parquet'),registry_rows=len(owners),id_rule='Fixed first-seen/address order at pinned cutoff; future registry must append IDs without renumbering',note='HF-only materialization, remaining metrics require retained historical factors; not a lossless14-column replacement'))
    results=[]
    for item in config['inputs']:
        path=Path(item['path']);assert sha(path)==item['sha256'];chunks=[];count=0
        for batch in pq.ParquetFile(path).iter_batches(batch_size=100000):
            batch=batch.slice(0,min(batch.num_rows,config['max_rows']-count));chunks.append(batch);count+=batch.num_rows
            if count>=config['max_rows']:break
        full=pa.Table.from_batches(chunks);source=full.select(['user','block_number','healthFactor']);reference=source.sort_by([('user','ascending'),('block_number','ascending')]);del chunks
        directory=root/item['label'];directory.mkdir();variants=[]
        for kind in config.get('variants',['full14_baseline','scalar_hf_blockorder','scalar_hf_sorted','vector_string1','vector_string9','vector_binary_min1','vector_binary_min9','vector_binary321','vector_binary329']):
            target=directory/(kind+'.parquet');start=time.perf_counter();cpu=time.process_time()
            if kind.startswith('vector_'):
                encoding=kind[len('vector_'):-1];vectors=pack(source,registry,encoding);write(vectors.rename_columns(['entity_id','start_offset','hf_values']),target,int(kind[-1]));physical_rows=vectors.num_rows;del vectors
            else:
                encoding=None;table=full if kind=='full14_baseline' else reference if kind=='scalar_hf_sorted' else source
                write(table,target,9 if kind=='scalar_hf_sorted' else None,['user','block_hash','profile'] if kind=='full14_baseline' else ['user']);physical_rows=table.num_rows
            write_s=time.perf_counter()-start;write_cpu=time.process_time()-cpu;start=time.perf_counter();stored=pq.read_table(target)
            restored=unpack(stored.rename_columns(['user_id','start_offset','hf']),registry,encoding) if encoding else stored.select(source.column_names)
            restored=restored.select(reference.column_names).sort_by([('user','ascending'),('block_number','ascending')])
            if not restored.equals(reference):raise ValueError('HF vector roundtrip differs')
            v=dict(variant=kind,bytes=target.stat().st_size,sha256=sha(target),physical_rows=physical_rows,hf_values=count,write_seconds=write_s,write_cpu_seconds=write_cpu,read_restore_validate_seconds=time.perf_counter()-start,all_hf_user_block_exact=True)
            variants.append(v);print(json.dumps(dict(sample=item['label'],**v)),flush=True)
            del stored,restored;gc.collect();pa.default_memory_pool().release_unused()
        assert sha(path)==item['sha256']
        results.append(dict(sample=item['label'],hf_values=count,input_sha256=item['sha256'],source_unchanged=True,variants=variants))
        save(root/'results.json',dict(samples=results,registry_bytes=(root/'users.parquet').stat().st_size,registry_rows=len(owners),registry_shared_once=True,pyarrow=pa.__version__,peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,scope='HF-only sample format, exact HF and keys; Other11fields from the original14-column table are not encoded here and remain recoverable from full historical factors; not production',timing='Warm-cache single CPU, includes grouping/sorting/conversion, no durable-write throughput claim'))
        del full,source,reference;gc.collect();pa.default_memory_pool().release_unused()
if __name__=='__main__':main()
