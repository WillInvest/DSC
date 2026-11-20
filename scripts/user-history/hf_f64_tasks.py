"""Dask adapters using fresh children and the existing authenticated transport."""
import multiprocessing
from pathlib import Path
from shared.artifacts import checksum
from hf_f64_accept import accept,check_receipt
import materialize_accounts as m

def accept_remote(result,directory):
    with multiprocessing.get_context('spawn').Pool(1,maxtasksperchild=1) as pool:r=pool.apply(accept,(result,directory))
    return dict(receipt=r,sha256=checksum(Path(directory)/'receipt.json'))

def inspect(jobs,base,generation,retained):
    old=dict(retained);found=[]
    for job in jobs:
        path=Path(base)/'outputs'/generation/'jobs'/job['id']/'receipt.json'
        if not path.exists():
            if job['id'] in old:raise ValueError('Accepted receipt missing')
            continue
        sha=checksum(path)
        if job['id'] in old and old[job['id']]!=sha:raise ValueError('Committed receipt changed')
        # One-time restart readback is deliberate, never fresh node verification.
        r=check_receipt(path,job,generation);found.append(dict(receipt=r,sha256=sha))
    return found

def capacity(base):return dict(free_bytes=m.ensure_capacity(base))
