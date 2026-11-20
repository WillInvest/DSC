"""Bounded lossless layout experiments on immutable HF Parquet samples.

Uses Arrow sorting/index/take, native run-end decoding and Parquet compression. Custom adapters only
normalize our user/block keys, encode full uint256, and expand exact state runs.
Never changes input files, numerical values, or production acceptance.
"""
import gc
import hashlib
import json
import resource
import sys
import time
from pathlib import Path
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

FIELDS=['totalCollateralBase','totalDebtBase','availableBorrowsBase','currentLiquidationThreshold','ltv','healthFactor']
META=['block_hash','timestamp','profile']
BINARY=FIELDS+['user_configuration']

def sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for chunk in iter(lambda:f.read(2**20),b''):h.update(chunk)
    return h.hexdigest()

def save(path,value):
    p=Path(path);tmp=p.with_suffix('.tmp');tmp.write_text(json.dumps(value,indent=2)+'\n');tmp.replace(p)

def write(table,path,level,dictionary=True):
    pq.write_table(table,path,compression='zstd',compression_level=level,
                   use_dictionary=dictionary,row_group_size=100000)

def normalized(source):
    users=pa.table({'user':pc.unique(source['user'])})
    ids=pc.cast(pc.index_in(source['user'],value_set=users['user']),pa.uint32())
    blocks=source.select(['block_number']+META).group_by('block_number',use_threads=False).aggregate([(name,'first') for name in META])
    blocks=blocks.rename_columns(['block_number']+META)
    bi=pc.index_in(source['block_number'],value_set=blocks['block_number'])
    for name in META:
        assert pc.take(blocks[name],bi).equals(source[name]),'Block metadata is not a function of block number'
    unique=source.select(['user','block_number','has_debt']).group_by(['user','block_number']).aggregate([('has_debt','count')])
    assert unique.num_rows==source.num_rows,'Duplicate user-block key'
    rows=source.drop(['user']+META).append_column('user_id',ids)
    return rows,users,blocks

def collapse(rows):
    n=rows.num_rows
    if not n:raise ValueError('Empty sample')
    nums=rows['block_number']
    same=pa.array([True]*(n-1))
    for name in rows.column_names:
        if name=='block_number':continue
        same=pc.and_(same,pc.equal(rows[name].slice(1),rows[name].slice(0,n-1)))
    same=pc.and_(same,pc.equal(nums.slice(1),pc.add(nums.slice(0,n-1),1)))
    start_mask=pa.concat_arrays([pa.array([True]),pc.invert(same).combine_chunks()]) if isinstance(same,pa.ChunkedArray) else pa.concat_arrays([pa.array([True]),pc.invert(same)])
    starts=pc.indices_nonzero(start_mask)
    ends=pa.concat_arrays([pc.subtract(starts.slice(1),1).cast(pa.int64()),pa.array([n-1],type=pa.int64())])
    runs=rows.take(starts).drop(['block_number'])
    runs=runs.append_column('start_block',pc.take(nums,starts).cast(pa.uint32()))
    runs=runs.append_column('stop_block',pc.add(pc.take(nums,ends),1).cast(pa.uint32()))
    return runs

def expand(runs,limit):
    starts=runs['start_block'].combine_chunks().cast(pa.int64())
    lengths=pc.subtract(runs['stop_block'].combine_chunks().cast(pa.int64()),starts)
    if pc.any(pc.less_equal(lengths,0)).as_py() or pc.sum(lengths).as_py()!=limit:raise ValueError('Run coverage differs')
    ends=pc.cumulative_sum(lengths)
    indices=pc.run_end_decode(pa.RunEndEncodedArray.from_arrays(ends,pa.array(range(len(starts)),type=pa.int64())))
    offsets=pc.subtract(pa.array(range(limit),type=pa.int64()),pc.take(pc.subtract(ends,lengths),indices))
    rows=runs.drop(['start_block','stop_block']).take(indices)
    return rows.append_column('block_number',pc.add(pc.take(starts,indices),offsets).cast(pa.uint32()))

def binary_encode(rows):
    for name in BINARY:
        values=[]
        for text in rows[name].to_pylist():
            value=int(text)
            if str(value)!=text or not 0<=value<2**256:raise ValueError('Noncanonical uint256')
            values.append(value.to_bytes(32,'big'))
        rows=rows.set_column(rows.schema.get_field_index(name),name,pa.array(values,type=pa.binary(32)))
    return rows

def binary_decode(rows):
    for name in BINARY:
        values=[str(int.from_bytes(x,'big')) for x in rows[name].to_pylist()]
        rows=rows.set_column(rows.schema.get_field_index(name),name,pa.array(values,type=pa.string()))
    return rows

def restore(rows,users,blocks,columns):
    ids=rows['user_id']
    assert ids.null_count==0
    bi=pc.index_in(rows['block_number'],value_set=blocks['block_number'])
    if bi.null_count:raise ValueError('Missing block metadata')
    result=rows.drop(['user_id']).append_column('user',pc.take(users['user'],ids))
    for name in META:result=result.append_column(name,pc.take(blocks[name],bi))
    return result.select(columns)

def selfcheck():
    # Exact repeats must not bridge user changes or missing blocks; uint256 MAX
    # and small consecutive integer differences must round-trip without floats.
    rows=[]
    for user in ['0xa','0xb']:
        for block in [1,2,4,5]:
            row=dict(user=user,block_number=block,block_hash=str(block),timestamp=block,profile='p',emode=0,user_configuration='0',has_debt=False)
            row.update({k:'0' for k in FIELDS});row['healthFactor']=str(2**256-1)
            if user=='0xb':row['totalCollateralBase']=str(2**200+block)
            rows.append(row)
    table=pa.Table.from_pylist(rows).set_column(1,'block_number',pa.array([r['block_number'] for r in rows],type=pa.uint32()))
    n,u,b=normalized(table);r=collapse(n);assert r.num_rows==6
    d=restore(expand(binary_decode(binary_encode(r)),table.num_rows),u,b,table.column_names)
    assert d.equals(table),'Synthetic exact run/uint256 roundtrip failed'

def run_sample(item,root,limit):
    path=Path(item['path']);before=sha(path)
    if before!=item['sha256']:raise ValueError('Input SHA mismatch')
    batches=[];count=0
    for batch in pq.ParquetFile(path).iter_batches(batch_size=100000):
        batch=batch.slice(0,min(batch.num_rows,limit-count));batches.append(batch);count+=batch.num_rows
        if count>=limit:break
    source=pa.Table.from_batches(batches);del batches
    sort_start=time.perf_counter();reference=source.sort_by([('user','ascending'),('block_number','ascending')]);sort_seconds=time.perf_counter()-sort_start;ordered=reference
    original_schema=source.schema
    report=dict(label=item['label'],input=str(path),input_sha256=before,source_file_bytes=path.stat().st_size,
                source_file_rows=pq.read_metadata(path).num_rows,sample_rows=source.num_rows,
                sample_blocks=len(pc.unique(source['block_number'])),sample_users=len(pc.unique(source['user'])),
                schema=str(source.schema),initial_sort_seconds=sort_seconds,variants=[])
    directory=root/item['label'];directory.mkdir()
    variants=['baseline','zstd9','user_sorted1','user_sorted3','user_sorted9','user_sorted_dictionary9','normalized9','exact_runs9','exact_runs_binary9']
    for variant in variants:
        target=directory/variant;target.mkdir();start=time.perf_counter();cpu=time.process_time()
        rle=variant.startswith('exact_runs');norm=variant in ['normalized9','exact_runs9','exact_runs_binary9'];binary=variant.endswith('binary9')
        table=source if variant in ['baseline','zstd9'] else ordered
        if norm:
            table,users,blocks=normalized(table)
            if rle:table=collapse(table)
            if binary:table=binary_encode(table)
            write(users,target/'users.parquet',9);write(blocks,target/'blocks.parquet',9)
            write(table,target/'states.parquet',9)
            physical_rows=table.num_rows
            del table,users,blocks
        else:
            write(table,target/'accounts.parquet',None if variant=='baseline' else (1 if variant=='user_sorted1' else 3 if variant=='user_sorted3' else 9),True if variant=='user_sorted_dictionary9' else ['user','block_hash','profile'])
            physical_rows=table.num_rows
            del table
        write_s=time.perf_counter()-start;write_cpu=time.process_time()-cpu
        read_start=time.perf_counter()
        if norm:
            table=pq.read_table(target/'states.parquet');users=pq.read_table(target/'users.parquet');blocks=pq.read_table(target/'blocks.parquet')
            if binary:table=binary_decode(table)
            if rle:table=expand(table,source.num_rows)
            restored=restore(table,users,blocks,source.column_names)
            del table,users,blocks
        else:restored=pq.read_table(target/'accounts.parquet')
        read_s=time.perf_counter()-read_start
        check_start=time.perf_counter();restored=restored.cast(original_schema).sort_by([('user','ascending'),('block_number','ascending')])
        if not restored.equals(reference):
            bad=[name for name in reference.column_names if not restored[name].equals(reference[name])]
            raise ValueError('Lossless roundtrip differs: '+str(bad))
        validation_s=time.perf_counter()-check_start
        del restored
        block_values=pc.unique(source['block_number']);target_block=block_values[len(block_values)//2].as_py()
        point_start=time.perf_counter()
        if norm:
            filters=[('start_block','<=',target_block),('stop_block','>',target_block)] if rle else [('block_number','=',target_block)]
            point=pq.read_table(target/'states.parquet',filters=filters)
            if binary:point=binary_decode(point)
            if rle:point=point.drop(['start_block','stop_block']).append_column('block_number',pa.array([target_block]*point.num_rows,type=pa.uint32()))
            point=restore(point,pq.read_table(target/'users.parquet'),pq.read_table(target/'blocks.parquet'),source.column_names)
        else:point=pq.read_table(target/'accounts.parquet',filters=[('block_number','=',target_block)])
        expected_point=reference.filter(pc.equal(reference['block_number'],target_block))
        assert point.cast(original_schema).sort_by([('user','ascending'),('block_number','ascending')]).equals(expected_point),'Single-block reconstruction differs'
        point_s=time.perf_counter()-point_start
        point_rows=point.num_rows
        del point,expected_point
        files={p.name:dict(bytes=p.stat().st_size,sha256=sha(p)) for p in target.glob('*.parquet')}
        result=dict(variant=variant,physical_state_rows=physical_rows,logical_rows=source.num_rows,
                    bytes=sum(x['bytes'] for x in files.values()),files=files,write_seconds=write_s,write_cpu_seconds=write_cpu,write_including_sort_seconds=write_s+(sort_seconds if variant not in ['baseline','zstd9'] else 0),
                    read_restore_seconds=read_s,validation_seconds=validation_s,point_block=target_block,point_rows=point_rows,point_read_restore_validate_seconds=point_s,all_columns_exact=True,point_exact=True)
        report['variants'].append(result);save(directory/'report.json',report)
        print(json.dumps(dict(sample=item['label'],**{k:v for k,v in result.items() if k!='files'})),flush=True)
        gc.collect();pa.default_memory_pool().release_unused()
    if sha(path)!=before:raise ValueError('Original source changed')
    report['source_unchanged']=True
    baseline=report['variants'][0]['bytes']
    for v in report['variants']:v.update(bytes_per_logical_row=v['bytes']/source.num_rows,savings_fraction=1-v['bytes']/baseline)
    save(directory/'report.json',report)
    return report

def main():
    config=json.loads(Path(sys.argv[1]).read_text());root=Path(config['output']);root.mkdir(exist_ok=False)
    selfcheck();results=[]
    for item in config['inputs']:
        results.append(run_sample(item,root,config.get('max_rows',1000000)))
        save(root/'results.json',dict(samples=results,pyarrow=pa.__version__,peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
             scope='Isolated sample layout experiment, all original fields/integers recover exactly, no production coverage change',timing_notes='Warm-cache, single process; write time includes layout preparation, read includes reconstruction, validation includes canonical key sort. No durable fsync throughput claim. Baseline uses the production default Zstd level and production100k row groups/dictionary selection.'))
if __name__=='__main__':main()
