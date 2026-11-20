import csv
import json
import math
import tempfile
from pathlib import Path

import pyarrow.parquet as pq

from read_archive import restore

root = Path.home() / 'aave-research-dataset'
provenance = root / '04_Validation_and_Provenance'
vectors = root / '01_HF_Vectors'
sample = json.loads((provenance / 'rebuilt-account-22434005.json').read_text())
block = sample['block']
address = sample['user']
batch = '3c4e71fb0df22139098dc66d'
primary = Path.home() / 'hf-flow-20261116/storage/primary-archives' / batch
member_proof = json.loads((root / '.data-cleanup-member-proof-20261119/node-archive-member-match.json').read_text())
index_sha = member_proof['archives'][batch]['index_or_manifest_sha256']
with (vectors / 'CANONICAL_JOBS.csv').open(newline='') as f:
    job = next(r['job_id'] for r in csv.DictReader(f)
               if int(r['start_block_inclusive']) <= block < int(r['stop_block_exclusive']))
users = pq.read_table(vectors / 'dictionaries/users.parquet', filters=[('address', '=', address.lower())])
if users.num_rows != 1:
    raise ValueError('Delivered wallet dictionary lookup is ambiguous')
wallet = users['wallet_id'][0].as_py()
index = json.loads((primary / 'pack-index.json').read_text())
prefix = next(n.rsplit('/', 1)[0] for n in index['members'] if n.endswith(f'/jobs/{job}/receipt.json'))
del index
with tempfile.TemporaryDirectory(prefix='delivered-vector-sample-', dir=root / '.replay-validation') as tmp:
    tmp = Path(tmp)
    evidence = []
    def member(name):
        path = tmp / name
        evidence.append(restore(primary, f'{prefix}/{name}', path, index_sha))
        return path
    receipt = json.loads(member('receipt.json').read_text())
    part = next(x for x in receipt['parts'] if x['wallet_start'] <= wallet < x['wallet_stop']
                and x['start'] <= block < x['stop'])
    tile = pq.read_table(member(Path(part['path']).name), filters=[('wallet_id', '=', wallet)])
    if tile.num_rows != 1:
        raise ValueError('Missing or duplicated archived wallet row')
    meta = tile.schema.metadata
    first = max(int(meta[b'chunk_start']), int(meta[b'block_origin']) + tile['first_block_index'][0].as_py())
    observed = tile['hf_vector'][0].as_py()[block - first]
    expected = int(sample['account'][-1]) / 10**18
    if sample['analysis_debt_excluded'] or not math.isclose(observed, expected, rel_tol=1e-10, abs_tol=1e-10):
        raise ValueError('Delivered wallet value differs from node-validated reconstruction')
    part = next(x for x in receipt['aggregate_parts'] if x['start'] <= block < x['stop'])
    aggregates = pq.read_table(member(Path(part['path']).name))
    offset = block - int(aggregates.schema.metadata[b'chunk_start'])
    mapping = json.loads((vectors / 'dictionaries/dataset.json').read_text())['aggregate_scope_mapping']
    scopes = {r['scope_id']: r['namespace'] for r in mapping}
    metrics = {}
    for row in aggregates.to_pylist():
        value, numerator, denominator = [row[k][offset] for k in
            ('hf_vector', 'numerator_vector', 'denominator_vector')]
        if row['scope_id'] in metrics or ((value is None or not math.isclose(value, numerator / denominator, rel_tol=1e-10))
                                         if denominator else (value is not None or numerator != 0)):
            raise ValueError('Archive aggregate formula or null semantics differ')
        metrics[row['scope_id']] = (value, numerator, denominator)
    if set(scopes) != set(metrics):
        raise ValueError('Archive aggregate scopes differ from the delivered dictionary')
    reserves = [metrics[i] for i in scopes if scopes[i] == 'reserve']
    [protocol] = [metrics[i] for i in scopes if scopes[i] == 'protocol']
    if not all(math.isclose(protocol[j], math.fsum(v[j] for v in reserves), rel_tol=1e-10)
               for j in (1, 2)):
        raise ValueError('Reserve totals do not match the archived protocol vector')
    for record in evidence:
        record['member']=Path(record.pop('output')).name
result = dict(block=block, wallet=address, wallet_id=wallet, job_id=job, batch=batch, index_sha256=index_sha,
              delivered_wallet_hf=observed, node_reference_hf=expected,
              wallet_absolute_error=abs(observed - expected), reserve_scopes=len(reserves),
              protocol_hf=protocol[0], all_aggregate_formulas_match=True,
              protocol_matches_reserve_sums=True, restored_members=evidence, rpc_this_run=0)
with (provenance / 'vector-reader-sample-22434005.json').open('x') as out:
    json.dump(result, out, indent=2, sort_keys=True)
print(json.dumps(result), flush=True)
