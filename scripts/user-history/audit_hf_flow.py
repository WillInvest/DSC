"""Sample new flow outputs against own RETH, restoring archived samples if needed."""
import argparse,fcntl,json,time
from pathlib import Path
from shared.artifacts import checksum,digest,write_json
from hf_flow_queue import read_snapshot


def locked_remote_audit(payload):
    """Keep every audited job readable until the numerical child exits."""
    from audit_hf_f64 import remote_audit
    from hf_flow_retire import job_locks
    directories = [Path(item['path']).parent for item in payload['receipts']]
    with job_locks(directories, exclusive=False):
        missing=[]
        for item in payload['receipts']:
            if 'sha256' in item and checksum(item['path'])!=item['sha256']:raise ValueError('Audit receipt changed')
            receipt=json.loads(Path(item['path']).read_text())
            if any(not Path(p['path']).exists() for p in receipt.get('parts',[])+receipt.get('aggregate_parts',[])):
                missing.append(receipt['job_id'])
        if missing:return dict(state='waiting_for_cloud_commit',missing_jobs=missing,extra_node_rpc=0)
        return remote_audit(payload)


def release_restored(payload,config):
    from hf_flow_artifacts import seal,combine,PROOF_FORMAT
    from hf_flow_cloud import _retire
    home=Path(config['node_home']);certs=[];probes=[]
    for value in payload['values']:
        r=value['receipt'];files={}
        for p in r['parts']+r['aggregate_parts']:
            path=Path(p['path']);files[path.name]=dict(bytes=path.stat().st_size,sha256=p['sha256'])
            probes.append(dict(path=str(path.relative_to(home)),sha256=p['sha256'],exact=True))
        if files:certs.append(seal(Path(value['path']).parent,files))
    if not certs:return dict(removed_files=0)
    cert=combine(home,certs)
    proof=dict(format=PROOF_FORMAT,source_certificate_sha256=digest(cert),manifest_sha256=digest(cert['files']),files=cert['files'],all_exact=True,
        destination=payload['remote'],verification='rclone_size_md5_sha256_restore',all_remote_size_md5_exact=True,restore_probes=probes,
        recovery='All restored files SHA256 checked against accepted manifest before this certificate')
    request=Path(config['node_flow_root'])/'restores'/payload['id']/'audit.cleanup.json';request.parent.mkdir(parents=True,exist_ok=True)
    return _retire(cert,proof,request)



def select_new_rows(rows,prior):
    latest={}
    for row in rows:latest[row[2]]=row
    seen=prior.get('receipt_sha256_by_role',{})
    return [row for role,row in latest.items() if json.loads(row[3])['sha256']!=seen.get(role)]


def run(config_path,budget):
    cfg=json.loads(Path(config_path).read_text());out=Path(cfg['control_root'])/'audits';out.mkdir(exist_ok=True)
    with (out/'audit.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        return _run(cfg,out,budget)


def _run(cfg,out,budget):
    from distributed import Client
    from vector_coordinator import security
    from hf_flow_cloud import restore_jobs
    root=Path(cfg['control_root']);marker=out/'last-success.json'
    prior=json.loads(marker.read_text()) if marker.exists() else {}
    with read_snapshot(root/'flow.sqlite') as db:
        rows=db.execute('select n.sequence,j.job_id,j.role,j.node_value,j.cloud_batch from jobs j join node_acceptance n on j.job_id=n.job_id where n.sequence>? and j.local_value is not null order by n.sequence',(prior.get('node_sequence',0),)).fetchall()
    selected=select_new_rows(rows,prior)
    if not selected:
        report=dict(state='no_new_accepted_output',extra_node_rpc=0,created_at=time.time())
    else:
        values=[json.loads(r[3]) for r in selected];restored={}
        with Client(cfg['scheduler'],security=security(cfg['private']),set_as_default=False) as client:
            info=client.scheduler_info(n_workers=-1)['workers'];workers=[a for a,w in info.items() if str(w['name']).startswith('flow-io-node')]
            if not workers:raise ValueError('Node audit worker unavailable')
            submit=dict(pure=False,workers=workers,allow_other_workers=False,resources={'flow_io':1})
            def restore_committed():
                with read_snapshot(root/'flow.sqlite') as db:
                    batches={k:json.loads(v) for k,v in db.execute('select batch_id,report from cloud')};grouped={}
                    for row,value in zip(selected,values):
                        current=db.execute('select node_value,cloud_batch from jobs where job_id=?',(row[1],)).fetchone()
                        if json.loads(current[0])['sha256']!=value['sha256']:raise ValueError('Selected audit source changed')
                        if current[1] is not None:grouped.setdefault(current[1],[]).append(value)
                for batch,items in grouped.items():
                    if batch in restored:continue
                    payload=dict(id='audit-'+str(time.time_ns()),values=items,remote=batches[batch]['remote'],archive_format=batches[batch].get('archive_format','raw'),index_sha256=batches[batch].get('index_sha256'))
                    client.submit(restore_jobs,payload,cfg,**submit).result(timeout=600);restored[batch]=payload
            receipts=[dict(path=v['path'],sha256=v['sha256']) for v in values];deadline=time.monotonic()+600
            while True:
                restore_committed()
                report=client.submit(locked_remote_audit,dict(receipts=receipts,generation=cfg['generation'],storage_base=cfg['node_home'],rpc_budget=budget,max_samples=24),**submit).result(timeout=600)
                if not report.get('missing_jobs') or time.monotonic()>=deadline:break
                time.sleep(5)
            if not report.get('missing_jobs'):
                if report.get('within_error_budget') is not True:raise ValueError('Fresh own-node HF audit failed')
                for payload in restored.values():client.submit(release_restored,payload,cfg,**submit).result(timeout=600)
                seen=dict(prior.get('receipt_sha256_by_role',{}));seen.update({row[2]:value['sha256'] for row,value in zip(selected,values)})
                write_json(marker,dict(node_sequence=max(row[0] for row in rows),generation=cfg['generation'],receipt_sha256_by_role=seen,checked_at=time.time()))
    path=out/('audit-'+str(time.time_ns())+'.json');write_json(path,report)
    print(json.dumps(dict(report=str(path),sha256=checksum(path),**{k:v for k,v in report.items() if k not in ['checks','checked_receipts']})),flush=True)

if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--config',required=True);p.add_argument('--rpc-budget',type=int,default=200);a=p.parse_args();run(a.config,a.rpc_budget)
