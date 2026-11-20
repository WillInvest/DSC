"""Consistent compute-control snapshots; rsync bytes directly, TLS verifies hashes."""
import argparse
import json
import pathlib
import shlex
import shutil
import subprocess
import sys
import time


def verify_node_backup(directory, manifest):
    from pathlib import Path
    from shared.artifacts import checksum
    root = Path('/home/node/hf-flow-20261116/compute-control-backups')
    destination = Path(directory)
    assert destination.parent == root
    for name, value in manifest.items():
        p = destination/name
        assert p.resolve().is_relative_to(destination.resolve())
        assert p.stat().st_size == value['bytes'] and checksum(p) == value['sha256'], name
    return dict(all_hashes_exact=True, files=len(manifest), bytes=sum(v['bytes'] for v in manifest.values()), node_base=str(destination))


def run(config):
    from distributed import Client
    from vector_coordinator import security
    from distributed_hf import backup_sqlite
    from shared.artifacts import checksum, write_json
    cfg=json.loads(pathlib.Path(config).read_text());root=pathlib.Path(cfg['control_root'])
    stamp=str(time.time_ns());local=root/'backups'/stamp;local.mkdir(parents=True)
    backup_sqlite(root/'flow.sqlite',local/'flow.sqlite')
    names=['flow-config.json','numeric-plan.json','authority-epoch.json','reconciled-transfers.json',
           'catalog-catch-up.json','audits/last-success.json','pilot-completed.json','packed-pilot-completed.json']
    for n in names:
        if (root/n).is_file():
            p=local/n;p.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(root/n,p)
    for n in cfg['code_files']:
        p=local/'code'/n;p.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(pathlib.Path(cfg['code_root'])/n,p)
    manifest={str(p.relative_to(local)):dict(bytes=p.stat().st_size,sha256=checksum(p)) for p in local.rglob('*') if p.is_file()}
    write_json(local/'manifest.json',manifest)
    ssh=['ssh','-oBatchMode=yes','-oIdentitiesOnly=yes','-oStrictHostKeyChecking=yes',
         '-oUserKnownHostsFile='+str(root/'private/node-known-host'),'-i',str(root/'private/authority-backup-ed25519')]
    subprocess.run(['rsync','-a','--partial','--checksum','-e',shlex.join(ssh),str(local)+'/',
                    'node@10.246.103.160:'+stamp+'/'],check=True)
    with Client(cfg['scheduler'],security=security(cfg['private']),set_as_default=False) as client:
        workers=[a for a,v in client.scheduler_info()['workers'].items() if v['name']=='flow-io-node']
        assert len(workers)==1
        report=next(iter(client.run(verify_node_backup,'/home/node/hf-flow-20261116/compute-control-backups/'+stamp,manifest,workers=workers).values()))
    report.update(created_at=time.time(),generation=cfg['generation'],authority_epoch=cfg['authority_epoch'],local_base=str(local),manifest_sha256=checksum(local/'manifest.json'))
    write_json(root/'backups'/('verified-'+stamp+'.json'),report)
    print(json.dumps(report),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--config',required=True);a=p.parse_args();run(a.config)
