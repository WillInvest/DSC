"""Versioned float64 vector tiles using Arrow lists and Parquet metadata.

Every row retains the user's true first index. The file's declared contiguous
block range determines which part of that logical vector the row represents.
"""
import re
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
ORIGIN=16291127
STOP=26407882
FORMAT='hf-vector-f64-v2'
INCREMENTAL_FORMAT='hf-vector-f64-v3'


def _tile_contract(scope):
    if scope is None:
        return ORIGIN, STOP, {b'format':FORMAT.encode()}
    from shared.artifacts import digest
    start, stop = scope['start'], scope['stop']
    if not STOP<=start<stop or any(
            not re.fullmatch(r'[0-9a-f]{64}',scope[field])
            for field in ('window_sha256','wallet_registry_sha256')):
        raise ValueError('Invalid incremental wallet scope')
    metadata = {b'format':INCREMENTAL_FORMAT.encode(),b'scope_sha256':digest(scope).encode(),
                b'window_sha256':scope['window_sha256'].encode(),
                b'wallet_registry_sha256':scope['wallet_registry_sha256'].encode()}
    return start, stop, metadata


def encode_users(matrix,start,first_blocks,wallet_start=0,*,scope=None):
    matrix=np.asarray(matrix,dtype=np.float64);first=np.asarray(first_blocks,dtype=np.int64)
    lower,upper,contract=_tile_contract(scope)
    if matrix.ndim!=2 or matrix.shape[1]!=len(first) or not lower<=start<start+matrix.shape[0]<=upper:raise ValueError('Invalid tile scope')
    stop=start+matrix.shape[0];eligible=np.flatnonzero(first<stop);first=first[eligible];matrix=matrix[:,eligible]
    valid=np.arange(start,stop)[None,:]>=first[:,None];values=matrix.T[valid]
    if np.any(np.isnan(values)) or np.any(values<0):raise ValueError('Missing/invalid user HF')
    lengths=np.sum(valid,axis=1,dtype=np.int64);offsets=np.concatenate(([0],np.cumsum(lengths)))
    if len(values)>1_000_000:raise ValueError('Unbounded HF tile')
    lists=pa.ListArray.from_arrays(pa.array(offsets,type=pa.int32()),pa.array(values,type=pa.float64()))
    metadata={**contract,b'chunk_start':str(start).encode(),b'chunk_stop':str(stop).encode(),b'block_origin':str(ORIGIN).encode(),b'wallet_start':str(wallet_start).encode(),b'wallet_stop':str(wallet_start+len(first_blocks)).encode()}
    table=pa.table({'wallet_id':pa.array(eligible+wallet_start,type=pa.uint32()),'first_block_index':pa.array(first-ORIGIN,type=pa.uint32()),'hf_vector':lists}).replace_schema_metadata(metadata)
    sink=pa.BufferOutputStream();pq.write_table(table,sink,compression='zstd',compression_level=9,row_group_size=1024)
    return sink.getvalue().to_pybytes()


def decode_users(data,first_blocks,*,scope=None):
    t=pq.read_table(pa.BufferReader(data));meta=t.schema.metadata or {}
    lower,upper,contract=_tile_contract(scope)
    if any(meta.get(k)!=v for k,v in contract.items()) or t.column_names!=['wallet_id','first_block_index','hf_vector']:raise ValueError('Wrong float64 vector schema or scope')
    start=int(meta[b'chunk_start']);stop=int(meta[b'chunk_stop']);first=np.asarray(first_blocks,dtype=np.int64)
    if meta.get(b'block_origin')!=str(ORIGIN).encode() or not lower<=start<stop<=upper:raise ValueError('Invalid block range/origin')
    low=int(meta[b'wallet_start']);high=int(meta[b'wallet_stop'])
    if not 0<=low<=high<=len(first):raise ValueError('User tile outside dictionary')
    expected=np.arange(low,high)[first[low:high]<stop]
    ids=t['wallet_id'].to_numpy();indices=t['first_block_index'].to_numpy();lists=t['hf_vector'].combine_chunks()
    if not np.array_equal(ids,expected) or not np.array_equal(indices,first[expected]-ORIGIN):raise ValueError('Wrong user dictionary/first index')
    if t.schema.field('wallet_id').type!=pa.uint32() or t.schema.field('first_block_index').type!=pa.uint32() or lists.type!=pa.list_(pa.float64()) or lists.null_count:raise ValueError('Missing user vector')
    lengths=np.diff(lists.offsets.to_numpy());required=stop-np.maximum(first[expected],start)
    if not np.array_equal(lengths,required):raise ValueError('Missing/duplicate logical HF values')
    values=lists.values.to_numpy()
    if lists.values.null_count or np.any(np.isnan(values)) or np.any(values<0):raise ValueError('Missing/invalid user value')
    result=np.full((stop-start,len(expected)),np.nan);valid=np.arange(start,stop)[None,:]>=first[expected,None]
    transposed=result.T;transposed[valid]=values
    return start,stop,expected,result


def encode_aggregates(blocks,reserve_hf,numerators,denominators,reserve_ids,excluded_users=None,excluded_debt=None):
    """Explicit nullable reserve/protocol results and sufficient statistics."""
    blocks=np.asarray(blocks,dtype=np.uint32);n=np.asarray(numerators,dtype=np.float64);d=np.asarray(denominators,dtype=np.float64);h=np.asarray(reserve_hf,dtype=np.float64)
    if n.shape!=d.shape or h.shape!=n.shape or n.shape!=(len(blocks),len(reserve_ids)):raise ValueError('Aggregate dimensions differ')
    if len(blocks) and not np.array_equal(blocks,np.arange(int(blocks[0]),int(blocks[0])+len(blocks))):raise ValueError('Aggregate block gap')
    if np.any(d<0) or np.any(n<0) or np.any((d==0)&(n!=0)) or not np.all(np.isfinite(n)) or not np.all(np.isfinite(d)):raise ValueError('Invalid aggregate proof')
    valid=d>0
    if np.any(~np.isfinite(h[valid])) or not np.allclose(h[valid],(n/d.clip(min=np.finfo(float).tiny))[valid],atol=1e-10,rtol=1e-10):raise ValueError('Aggregate formula differs')
    # One compact row per reserve or protocol, one element per block.
    offsets=pa.array(np.arange(len(reserve_ids)+1)*len(blocks),type=pa.int32())
    columns={'scope_id':pa.array(reserve_ids,type=pa.uint16()),'first_block_index':pa.array(np.zeros(len(reserve_ids),dtype=np.uint32()))}
    for name,a in [('hf_vector',h),('numerator_vector',n),('denominator_vector',d)]:
        values=pa.array(a.T.ravel(),type=pa.float64(),mask=(~valid).T.ravel() if name=='hf_vector' else None)
        columns[name]=pa.ListArray.from_arrays(offsets,values)
    if excluded_users is not None:
        counts=np.asarray(excluded_users);debt=np.asarray(excluded_debt,dtype=np.float64)
        if counts.shape!=(len(blocks),) or debt.shape!=counts.shape or np.any(counts<0) or np.any(counts!=np.floor(counts)) or np.any(debt<0) or not np.all(np.isfinite(debt)):raise ValueError('Invalid exclusion evidence')
        columns['excluded_users_vector']=pa.array([None]*(len(reserve_ids)-1)+[counts.tolist()],type=pa.list_(pa.uint32()))
        columns['excluded_debt_base_vector']=pa.array([None]*(len(reserve_ids)-1)+[debt.tolist()],type=pa.list_(pa.float64()))
    t=pa.table(columns).replace_schema_metadata({b'format':b'hf-aggregate-f64-v1',b'chunk_start':str(int(blocks[0])).encode(),b'chunk_stop':str(int(blocks[-1])+1).encode(),b'metric':b'debt_weighted_user_hf_not_official_aave'})
    sink=pa.BufferOutputStream();pq.write_table(t,sink,compression='zstd',compression_level=9);return sink.getvalue().to_pybytes()
