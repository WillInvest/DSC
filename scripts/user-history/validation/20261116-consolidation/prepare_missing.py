"""Adapt a node-selected missing list to the existing bounded rsync backup API."""
import argparse,json,time
from pathlib import Path
from backup_hf import HOMES,active_manifest,checksum,read
from shared.artifacts import write_json
p=argparse.ArgumentParser();p.add_argument('role',choices=['revert','web']);p.add_argument('selection');p.add_argument('inventory');p.add_argument('stamp');a=p.parse_args()
home=Path(HOMES[a.role]);selection=read(a.selection)
if selection['role']!=a.role or selection['inventory_sha256']!=checksum(a.inventory):raise ValueError('Frozen source selection differs')
base=home/'hf-failover-20261115';old=active_manifest(base);old_plan=read(old);checkpoint=base/'backup-checkpoints'/a.stamp
if checkpoint.exists():raise ValueError('Preserve existing consolidation checkpoint')
for name,item in selection['files'].items():
 src=home/name;src.resolve().relative_to((home/'aave-hf-distributed/5197dae89b140ccb/outputs').resolve())
 if src.is_symlink() or not src.is_file() or src.stat().st_size!=item['bytes']:raise ValueError('Source missing or changed; preserve scope')
manifest=dict(role=a.role,generation=old_plan['generation'],plan_sha256=checksum(old),created_at=time.time(),files=selection['files'],new_rows=0,recovered_rows=0,selection_sha256=checksum(a.selection),numeric_generation=selection['numeric_generation'],scope='Missing immutable accepted source files selected from frozen numeric source manifest; archive transport only, no numerical coverage')
checkpoint.mkdir(parents=True);write_json(checkpoint/'manifest.json',manifest)
print(json.dumps(dict(checkpoint=str(checkpoint),manifest_sha256=checksum(checkpoint/'manifest.json'),files=len(manifest['files']),bytes=sum(v['bytes'] for v in manifest['files'].values()))),flush=True)
