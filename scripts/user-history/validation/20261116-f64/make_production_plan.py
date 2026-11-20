import json,time
from pathlib import Path
from shared.artifacts import checksum,digest,write_json
from hf_f64_contract import CONTRACT,source_assignments
B=Path('/var/lib/dsc-aave-hf-f64/20261116-v2');old=Path('/var/lib/dsc-aave-hf-f64/20261116');src=old/'reconciliation/source-manifest.json';frozen=json.loads(src.read_text());jobs,sources,roles=source_assignments(frozen)
first=json.loads((old/'f64-plan.json').read_text())['first_jobs']
cp=[];sha='0810f82a8e84ce532eac4b946412e3f0d96b8cc3f75f429858f6240725f525f5'
for role,path in [('data',str(old/'checkpoints/before-25199900.json.gz')),('node','/home/node/hf-f64-v2-20261116/checkpoints/before-25199900.json.gz'),('revert','/home/dsc-revert/hf-f64-v2-20261116/checkpoints/before-25199900.json.gz'),('web','/home/fao/hf-f64-v2-20261116/checkpoints/before-25199900.json.gz')]:cp.append(dict(role=role,path=path,block=25199900,sha256=sha))
files=['hf_batch_f64.py','hf_aggregates.py','hf_f64_vectors.py','hf_f64_compute.py','hf_f64_accept.py','hf_f64_sources.py','hf_replay_checkpoint.py','hf_f64_contract.py','hf_f64_tasks.py','hf_f64_queue.py','native_hf.py','native_batch.py','materialize_accounts.py','failover_hf.py','hf_vectors.py','vector_compute.py','vector_accept.py','vector_coordinator.py']
wheel=next((old/'wheels').glob('numpy-*.whl'))
identity=dict(contract=CONTRACT,method='numpy_sparse_edges_f64_v2_dust_001usd',numpy='2.2.6',numpy_wheel_sha256=checksum(wheel),sources={n:checksum(B/'code'/n) for n in files},source_manifest_sha256=checksum(src),jobs_sha256=digest(jobs),sources_sha256=digest(sources),source_roles_sha256=digest(roles),checkpoints_sha256=digest(cp))
p=dict(generation=digest(identity),identity=identity,jobs=jobs,sources=sources,source_roles=roles,source_manifest=str(src),first_jobs=first,workers={'data':2,'node':4,'revert':2,'web':2},private='/var/lib/dsc-aave-hf-data/5197dae89b140ccb/state/private',scheduler='tls://10.246.103.151:18896',storage_base='/home/node/hf-f64-v2-20261116/bulk',max_inflight=10,max_inflight_bytes=1_000_000_000,max_job_seconds=1200,checkpoints=cp,created_at=time.time(),canary_generation_retained='800c6a2a3c1ce015784aa70e899c86b9c2790d5b46733fb3d3dfe9b3fe2e4fb7',archive_sources={n:checksum(B/'code'/n) for n in ['vector_archive.py','hf_f64_archive.py','backup_hf.py']})
if (B/'f64-plan.json').exists():raise ValueError('Refuse overwrite pinned plan')
write_json(B/'f64-plan.json',p);print(json.dumps(dict(generation=p['generation'],manifest_sha256=checksum(B/'f64-plan.json'),jobs=len(jobs),dust_policy=CONTRACT['dust_policy'])))
