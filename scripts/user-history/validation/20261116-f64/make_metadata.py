from pathlib import Path
import os
import pyarrow as pa
import pyarrow.parquet as pq
from shared.artifacts import checksum,write_json,digest
from vector_compute import universe
from hf_batch_f64 import FloatBatch
from hf_f64_contract import CONTRACT
import materialize_accounts as m
b=Path('/var/lib/dsc-aave-hf-f64/20261116-v2');p=m.load(b/'f64-plan.json');owners=universe();first=CONTRACT['block_origin']
t=pa.table({'wallet_id':pa.array(range(len(owners)),type=pa.uint32()),'address':[x[0] for x in owners],'first_block_index':pa.array([x[1]-first for x in owners],type=pa.uint32())})
f=b/'users.parquet'
if f.exists():
 if not pq.read_table(f).equals(t):raise ValueError('Dictionary conflicts')
else:
 temp=f.with_suffix('.incoming');pq.write_table(t,temp,compression='zstd',compression_level=9)
 with temp.open('rb') as h:os.fsync(h.fileno())
 temp.replace(f)
reserves=FloatBatch(owners,m.load('/home/dsc-data/runs/aave-user-history-v3/26407881/plan.json')['jobs']).reserves
data=dict(generation=p['generation'],contract=CONTRACT,registry_sha256=checksum(f),registry_digest=digest(owners),wallets=len(owners),reserve_dictionary=reserves,aggregate_scope_mapping=[dict(scope_id=i,namespace='reserve',entity_id=i,address=a) for i,a in enumerate(reserves)]+[dict(scope_id=len(reserves),namespace='protocol',entity_id=0)],storage_base=p['storage_base'],physical_tiles='User ranges and contiguous time ranges; vector first_block_index is global user first appearance; chunk_start/stop metadata selects the saved segment',source_manifest_sha256=p['identity']['source_manifest_sha256'],old_exact_sources_retained=True,indicators_are_official_aave=False)
if (b/'dataset.json').exists():
 if m.load(b/'dataset.json')!=data:raise ValueError('Metadata conflicts')
else:write_json(b/'dataset.json',data)
print('metadata verified',len(owners),len(reserves),checksum(f))
