"""Bind independently verified node content and unchanged identities to a plan."""
import argparse,datetime,hashlib,json,subprocess,time
from pathlib import Path
from shared.artifacts import checksum,write_json


def build_proof(raw_plan,raw_selection,root,started_before):
 plan=json.loads(raw_plan);selection=json.loads(raw_selection)
 if plan['kind']!='consumed_source' or plan['role']!=selection['role'] or plan['inventory_sha256']!=selection['inventory_sha256'] or started_before>=selection['created_at']:raise ValueError('Retirement/selection provenance differs')
 entries=selection['verified_existing'];known={x['path']:x for x in entries}
 if len(known)!=len(entries):raise ValueError('Ambiguous source verification')
 prior=selection['prior_backup_receipt'];prior_time=datetime.datetime.fromisoformat(prior['verified_at']).timestamp()
 if not prior['all_exact'] or prior_time>started_before:raise ValueError('Prior content proof must predate selection')
 checks=[]
 for item in plan['files']:
  path=Path(item['backup']);name=str(path.relative_to(root));path.resolve().relative_to(root.resolve());s=path.stat();entry=known.get(name)
  exact=bool(entry and entry['sha256']==item['sha256'] and entry['kind'] in ['fresh_sha256','prior_verified_content_and_unchanged_ctime'] and path.is_file() and not path.is_symlink() and s.st_size==item['bytes'])
  method=entry['kind'] if entry else 'unverified'
  bound=prior_time if method=='prior_verified_content_and_unchanged_ctime' else started_before
  if exact and s.st_ctime>bound:
   actual=checksum(path);after=path.stat()
   exact=actual==item['sha256'] and (s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns)==(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns)
   method='fresh_sha256_after_selection'
  checks.append(dict(source=item['source'],backup=str(path),sha256=item['sha256'],exact=exact,content_verification=method,current_identity=dict(dev=s.st_dev,ino=s.st_ino,bytes=s.st_size,ctime_ns=s.st_ctime_ns,mtime_ns=s.st_mtime_ns)))
 return dict(plan_sha256=hashlib.sha256(raw_plan).hexdigest(),selection_sha256=hashlib.sha256(raw_selection).hexdigest(),selection_start_bound=started_before,selection_created_at=selection['created_at'],verification_kind='Independent content proof plus unchanged-since-selection-start identity; rehash newer files',checked_at=time.time(),files=checks,all_exact=all(x['exact'] for x in checks),bytes=sum(x['bytes'] for x in plan['files']))


if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('plan');p.add_argument('selection');p.add_argument('output');a=p.parse_args()
 raw_plan=Path(a.plan).read_bytes();raw_selection=Path(a.selection).read_bytes();selection=json.loads(raw_selection)
 inventory=Path('/home/node/hf-centralized/20261116/frozen-source-inventory.json')
 if checksum(inventory)!=selection['inventory_sha256']:raise ValueError('Frozen inventory changed')
 journal=subprocess.run(['sudo','journalctl','-u','dsc-hf-consolidation-select-missing-v2-20261116.service','-o','json','--no-pager'],check=True,capture_output=True,text=True)
 starts=[float(x['__REALTIME_TIMESTAMP'])/1e6 for line in journal.stdout.splitlines() if (x:=json.loads(line)).get('MESSAGE','').startswith('Started /usr/bin/python3 /home/node/hf-centralized/20261116/select_missing.py')]
 if len(starts)!=1:raise ValueError('Selection start evidence is ambiguous')
 anchor=inventory.stat().st_ctime
 if anchor>starts[0]-60:raise ValueError('Inventory identity does not establish a conservative pre-selection bound')
 report=build_proof(raw_plan,raw_selection,Path('/home/node/hf-backups/lan-20261116')/selection['role'],anchor)
 report['start_evidence']=dict(node_inventory_sha256=checksum(inventory),node_inventory_ctime_ns=inventory.stat().st_ctime_ns,systemd_start_journal_epoch=starts[0],unit='dsc-hf-consolidation-select-missing-v2-20261116.service')
 out=Path(a.output)
 if out.exists():raise ValueError('Preserve prior verification')
 write_json(out,report);print(json.dumps(dict(report=str(out),sha256=checksum(out),all_exact=report['all_exact'],files=len(report['files']),bytes=report['bytes'],verification_kind=report['verification_kind'])),flush=True)
