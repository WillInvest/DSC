"""Read-only catalog sampling and fresh, bounded own-node HF verification."""
import argparse,collections,hashlib,json,multiprocessing,sqlite3,tempfile,threading,time
from pathlib import Path
from unittest.mock import patch
import numpy as np
from shared.artifacts import checksum,write_json
import materialize_accounts as m
from hf_f64_contract import CONTRACT

def verify_node(payload):
    from vector_compute import universe
    from hf_f64_accept import check_receipt
    from hf_f64_vectors import decode_users
    from accounting_inputs import profiles,profile_at
    from web3 import HTTPProvider
    import failover_hf as f
    owners=universe();first=[b for _,b in owners];samples={};checked=[];empty=[]
    for item in payload['receipts']:
        path=Path(item['path']);path.resolve().relative_to(Path(payload['storage_base']).resolve())
        if checksum(path)!=item['sha256']:raise ValueError('Audited receipt identity differs')
        r=m.load(path);check_receipt(path,r['job'],payload['generation']);checked.append(dict(job_id=r['job_id'],receipt_sha256=item['sha256']))
        if not r['parts']:empty.extend([r['start'],r['stop']-1]);continue
        chosen=sorted({0,len(r['parts'])//2,len(r['parts'])-1})
        for index in chosen:
            part=r['parts'][index];start,stop,ids,x=decode_users(Path(part['path']).read_bytes(),first);finite=np.argwhere(np.isfinite(x));infinite=np.argwhere(np.isposinf(x))
            candidates=[]
            if len(finite):
                vals=x[finite[:,0],finite[:,1]];candidates.append(finite[int(np.argmin(np.abs(vals-1)))])
            if len(infinite):candidates.append(infinite[len(infinite)//2])
            for t,u in candidates:
                uid=int(ids[u]);block=start+int(t);value=float(x[t,u]);samples[(uid,block)]=dict(user=owners[uid][0],block=block,hf='+inf' if np.isposinf(value) else value)
    samples=list(samples.values())[:payload['max_samples']];profs=profiles();tasks=[]
    for s in samples:tasks.append(dict(kind='call',function='getUserAccountData',args=[s['user']],block=s['block'],profile=profile_at(profs,s['block'])['key']))
    if not tasks:
        for b in sorted(set(empty))[:6]:tasks.append(dict(kind='call',function='getReservesList',args=[],block=b,profile=profile_at(profs,b)['key']))
    abis={p['key']:p['abi'] for p in profs if p['key'] in {t['profile'] for t in tasks}}
    counts=collections.Counter();lock=threading.Lock();original=HTTPProvider.make_request
    def counted(self,method,params):
        with lock:
            if sum(counts.values())>=payload['rpc_budget']:raise ValueError('Extra audit RPC budget exhausted')
            counts[str(method)]+=1
        return original(self,method,params)
    if tasks:
        try:
            with patch.object(HTTPProvider,'make_request',counted):refs=f.direct_node(m.load(f.ROOT/'plan.json'),dict(operation='snapshots',abis=abis,tasks=tasks))
        except Exception as exc:return dict(generation=payload['generation'],structural_exact=True,within_error_budget=False,error_type=type(exc).__name__,provider_methods=dict(counts),extra_node_rpc=sum(counts.values()),created_at=time.time())
    else:refs=[]
    if len(refs)!=len(tasks):raise ValueError('Node omitted audit samples')
    db=m.readonly(f.ROOT/'accounting.sqlite');checks=[]
    try:
        for i,(task,ref) in enumerate(zip(tasks,refs)):
            header=db.execute('select hash from headers where block=?',(task['block'],)).fetchone();valid=bool(header and header[0].lower()==ref['block_hash'].lower());check=dict(block=task['block'],block_hash=ref['block_hash'],block_identity_exact=valid)
            if samples:
                s=samples[i];actual=np.inf if s['hf']=='+inf' else s['hf'];debt=int(ref['result'][1]);expected=np.inf if debt<CONTRACT['dust_debt_base_exclusive'] else int(ref['result'][5])/1e18
                valid=valid and (np.isposinf(actual) if np.isposinf(expected) else np.isfinite(actual) and abs(actual-expected)<=1e-8+1e-6*abs(expected))
                check.update(user=s['user'],stored_hf=s['hf'],node_account=[str(v) for v in ref['result']],dust_excluded=0<debt<CONTRACT['dust_debt_base_exclusive'])
                if np.isfinite(expected) and np.isfinite(actual):check.update(absolute_error=abs(actual-expected),relative_error=abs(actual-expected)/max(abs(expected),1e-300))
            check['within_error_budget']=bool(valid);checks.append(check)
    finally:db.close()
    absolute=[c['absolute_error'] for c in checks if 'absolute_error' in c];relative=[c['relative_error'] for c in checks if 'relative_error' in c]
    return dict(generation=payload['generation'],checked_receipts=checked,structural_exact=True,within_error_budget=all(c['within_error_budget'] for c in checks),validation_kind='float64_model',dust_threshold_usd=CONTRACT['dust_threshold_usd'],checks=checks,hf_max_absolute_error=max(absolute,default=0.),hf_p99_absolute_error=float(np.quantile(absolute,.99)) if absolute else 0.,hf_max_relative_error=max(relative,default=0.),provider_methods=dict(counts),extra_node_rpc=sum(counts.values()),created_at=time.time())

def remote_audit(payload):
    from audit_hf_f64 import verify_node as child_verify
    with multiprocessing.get_context('spawn').Pool(1,maxtasksperchild=1) as pool:return pool.apply(child_verify,(payload,))

def run(base,budget):
    from distributed import Client
    from distributed_hf import backup_sqlite
    from vector_coordinator import security
    base=Path(base);p=m.load(base/'f64-plan.json');out=base/'audits';out.mkdir(exist_ok=True);marker=out/'last-success.json';prior=m.load(marker) if marker.exists() else {};previous=prior.get('rowid',0) if prior.get('generation')==p['generation'] else 0
    with tempfile.TemporaryDirectory(prefix='audit-snapshot-',dir=base) as folder:
        path=Path(folder)/'ledger.sqlite';backup_sqlite(base/'tracker.sqlite',path)
        with sqlite3.connect('file:'+str(path)+'?mode=ro',uri=True) as db:rows=db.execute('select rowid,receipt,sha256 from accepted where generation=? and rowid>? order by rowid',(p['generation'],previous)).fetchall()
    if not rows:r=dict(generation=p['generation'],state='no_new_accepted_output',extra_node_rpc=0,created_at=time.time())
    else:
        chosen=rows[-3:];receipts=[]
        for rowid,path,sha in chosen:
            if checksum(path)!=sha:raise ValueError('Data accepted receipt changed')
            value=m.load(path);receipts.append(dict(path=str(Path(p['storage_base'])/'outputs'/p['generation']/'jobs'/value['job_id']/'receipt.json'),sha256=sha))
        with Client(p['scheduler'],security=security(p['private']),set_as_default=False) as client:r=client.submit(remote_audit,dict(receipts=receipts,generation=p['generation'],storage_base=p['storage_base'],rpc_budget=budget,max_samples=12),pure=False,resources={'f64_accept':1}).result(timeout=300)
        if r['within_error_budget']:write_json(marker,dict(rowid=rows[-1][0],generation=p['generation'],checked_at=time.time()))
    report=out/('audit-'+str(time.time_ns())+'.json');write_json(report,r);print(json.dumps(dict(report=str(report),sha256=checksum(report),**{k:v for k,v in r.items() if k not in ['checks','checked_receipts']})),flush=True)
    if r.get('within_error_budget') is False:raise SystemExit('Fresh node audit failed; stop only the new producer and preserve all evidence')
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--base',required=True);p.add_argument('--rpc-budget',type=int,default=200);a=p.parse_args()
    try:run(a.base,a.rpc_budget)
    except Exception as exc:
        out=Path(a.base)/'audits';out.mkdir(exist_ok=True);write_json(out/('failure-'+str(time.time_ns())+'.json'),dict(state='audit_failed',error_type=type(exc).__name__,extra_node_rpc=None,rpc_budget=a.rpc_budget,created_at=time.time()));raise
