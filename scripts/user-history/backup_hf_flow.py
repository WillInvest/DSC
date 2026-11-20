"""Online control snapshots with streaming SSH/rsync and full node SHA readback.

Bulk SQLite bytes never enter the scheduler task graph. Data remains authority.
"""
def run(config_path):
 import pathlib,json,time,subprocess
 cfg=json.load(open(config_path));r=pathlib.Path(cfg['control_root']);from distributed_hf import backup_sqlite
 from shared.artifacts import checksum,write_json
 stamp=str(time.time_ns());local=r/'backups'/('direct-'+stamp);local.mkdir();backup_sqlite(r/'flow.sqlite',local/'flow.sqlite')
 for n in ['flow-config.json','numeric-plan.json','audits/last-success.json','pilot-completed.json','packed-pilot-completed.json']:
  if not (r/n).exists():continue
  p=local/n;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes((r/n).read_bytes())
 for n in cfg['code_files']:
  p=local/'code'/n;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes((pathlib.Path(cfg['code_root'])/n).read_bytes())
 m={str(p.relative_to(local)):dict(bytes=p.stat().st_size,sha256=checksum(p)) for p in local.rglob('*') if p.is_file()};write_json(local/'manifest.json',m)
 dest=str(pathlib.Path(cfg['node_flow_root'])/'control-backups'/stamp)
 subprocess.run(['ssh','-oBatchMode=yes',cfg.get('backup_node_ssh','node@10.246.103.160'),'mkdir','-p',dest],check=True)
 subprocess.run(['rsync','-a','--partial','--checksum','-e','ssh -oBatchMode=yes -oConnectTimeout=15',str(local)+'/', cfg.get('backup_node_ssh','node@10.246.103.160')+':'+dest+'/'],check=True)
 code="import sys;sys.path[:0]=["+repr(str(pathlib.Path(cfg['node_flow_root'])/'code-v5'))+",'/usr/local/libexec/dsc-hf-flow'];from shared.artifacts import checksum;import pathlib,json;r=pathlib.Path("+repr(dest)+");m=json.load(open(r/'manifest.json'));assert all((r/n).stat().st_size==v['bytes'] and checksum(r/n)==v['sha256'] for n,v in m.items());print(json.dumps(dict(files=len(m),bytes=sum(v['bytes'] for v in m.values()),all_hashes_exact=True)))"
 v=json.loads(subprocess.check_output(['ssh',cfg.get('backup_node_ssh','node@10.246.103.160'),'python3 -'],input=code,text=True));v.update(data_remains_authority=True,created_at=time.time(),node_base=dest,local_base=str(local),transport='rsync over existing LAN SSH, full destination SHA readback; no Dask graph',manifest=m);p=r/'backups'/('control-'+stamp+'.json');write_json(p,v);print(json.dumps(dict(report=str(p),sha256=checksum(p),bytes=v['bytes'],files=v['files'],all_hashes_exact=True)))

if __name__=="__main__":
 import argparse
 p=argparse.ArgumentParser();p.add_argument("--config",required=True);a=p.parse_args();run(a.config)
