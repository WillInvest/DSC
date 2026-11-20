"""Bounded casts of explicitly located, hash-verified retained exact outputs."""
from pathlib import Path
import numpy as np
import pyarrow.parquet as pq
from shared.artifacts import checksum
from hf_f64_contract import FACTOR
MAX=2**256-1


def read_source(source,owners,start,stop):
    receipt=source['receipt'];kind=source['kind'];files=source['files']
    if not receipt['all_exact'] or receipt.get('mismatches',0) or receipt['factor_generation']!=FACTOR:raise ValueError('Unaccepted retained source')
    if not receipt['start']<=start<stop<=receipt['stop']:raise ValueError('Retained source scope differs')
    original=[(p['sha256'],p['hf_values']) for p in receipt['parts']] if kind=='integer_vector' else [(receipt['sha256'],receipt['rows'])]
    if [(p['sha256'],p['values']) for p in files]!=original:raise ValueError('Source locator identity differs')
    n=int(np.searchsorted([b for _,b in owners],stop,side='left'));out=np.full((stop-start,n),np.nan)
    user_ids={a:i for i,(a,_) in enumerate(owners)}
    for part in files:
        path=Path(part['path'])
        if checksum(path)!=part['sha256']:raise ValueError('Retained source hash differs')
        if kind=='integer_vector':
            import hf_vectors
            tables=[hf_vectors.unpack(hf_vectors.loads(path.read_bytes()))]
        elif kind=='integer_table':
            tables=pq.ParquetFile(path).iter_batches(batch_size=100000,columns=['user','block_number','healthFactor'])
        else:raise ValueError('Unknown retained format')
        count=0
        for table in tables:
            count+=table.num_rows;blocks=table['block_number'].to_numpy();selected=(blocks>=start)&(blocks<stop)
            indices=np.flatnonzero(selected)
            if not len(indices):continue
            selected_table=table.take(indices)
            if kind=='integer_vector':
                ids=selected_table['entity_id'].to_numpy();values=np.fromiter((int.from_bytes(v,'big') for v in selected_table['hf_values'].to_pylist()),dtype=object,count=len(indices))
            else:
                ids=np.array([user_ids[a.lower()] for a in selected_table['user'].to_pylist()]);values=np.fromiter((int(v) for v in selected_table['healthFactor'].to_pylist()),dtype=object,count=len(indices))
            bs=blocks[indices]-start
            if np.any(ids>=n) or len(np.unique(bs*n+ids))!=len(ids) or np.any(~np.isnan(out[bs,ids])):raise ValueError('Duplicate/out-of-range retained keys')
            out[bs,ids]=np.fromiter((np.inf if v==MAX else v/1e18 for v in values),dtype=np.float64,count=len(values))
        if count!=part['values']:raise ValueError('Retained file population differs')
    valid=np.arange(start,stop)[:,None]>=np.array([b for _,b in owners[:n]])[None,:]
    if np.any(np.isnan(out[valid])) or np.any(~np.isnan(out[~valid])):raise ValueError('Incomplete/misaligned retained HF')
    return out
