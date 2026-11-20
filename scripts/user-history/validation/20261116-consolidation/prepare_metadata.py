"""Preserve original validation bytes through the existing rsync backup API."""
import argparse
from pathlib import Path
from backup_hf import HOMES,active_manifest,checksum,read
from shared.artifacts import write_json
p=argparse.ArgumentParser();p.add_argument('role',choices=['revert','web']);p.add_argument('requests');p.add_argument('stamp');a=p.parse_args();home=Path(HOMES[a.role]);d=read(a.requests);V=Path('/home/dsc-data/data/aave-user-history-v3/26407881/accounting/hf-numeric');W=home/'aave-hf-distributed/5197dae89b140ccb/outputs';base=home/'hf-failover-20261115';old=active_manifest(base);folder=base/'backup-checkpoints'/a.stamp
if d['role']!=a.role or folder.exists():raise ValueError('Metadata scope conflict')
files={}
for name,item in d['files'].items():
 src=W/Path(name).relative_to(V);src.resolve().relative_to(W.resolve())
 if src.is_symlink() or not src.is_file():raise ValueError('Missing original metadata')
 sha=checksum(src)
 if item['sha256'] is not None and sha!=item['sha256']:raise ValueError('Original validation report changed')
 files[str(src.relative_to(home))]=dict(bytes=src.stat().st_size,sha256=sha)
folder.mkdir(parents=True);manifest=dict(role=a.role,generation=read(old)['generation'],plan_sha256=checksum(old),files=files,new_rows=0,recovered_rows=0,numeric_generation=d['generation'],requests_sha256=checksum(a.requests),scope='Original validation and output sidecar bytes; not reserialized and not numerical coverage')
write_json(folder/'manifest.json',manifest);print(dict(files=len(files),bytes=sum(x['bytes'] for x in files.values()),manifest_sha256=checksum(folder/'manifest.json')),flush=True)
