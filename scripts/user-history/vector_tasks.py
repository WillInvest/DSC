"""Dask dependency adapters; numerical and acceptance kernels remain pinned."""
import multiprocessing
from pathlib import Path
from shared.artifacts import checksum
import materialize_accounts as m
from vector_accept import accept, check_receipt
from vector_coordinator import retained_check


def accept_remote(result, directory):
    # A fresh child also releases Arrow's validation allocations after each job.
    with multiprocessing.get_context('spawn').Pool(1, maxtasksperchild=1) as pool:
        receipt = pool.apply(accept, (result, directory))
    return dict(receipt=receipt, sha256=checksum(Path(directory)/'receipt.json'))


def inspect_receipts(jobs, base, generation, retained=()):
    """Explicit plan keys only; never infer logical coverage from directory globs."""
    root = Path(base)/'outputs'/generation/'jobs'
    old = dict(retained)
    found = []
    for job in jobs:
        path = root/job['id']/'receipt.json'
        if not path.exists():
            if job['id'] in old:
                raise ValueError('Catalog accepted receipt is missing')
            continue
        sha = checksum(path)
        if job['id'] in old:
            if sha != old[job['id']]:
                raise ValueError('Catalog receipt hash changed')
            r = retained_check(path, job, generation)
        else:
            r = check_receipt(path, job, generation)
        found.append(dict(receipt=r, sha256=sha))
    return found


def storage_status(base):
    return dict(free_bytes=m.ensure_capacity(base), storage_role='node')
