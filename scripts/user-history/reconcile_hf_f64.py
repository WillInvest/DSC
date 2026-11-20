"""Freeze explicit old exact sources for conversion and weight reconstruction."""
import argparse,json,sqlite3,time
from pathlib import Path
from distributed_hf import backup_sqlite
from shared.artifacts import checksum,write_json,digest
from hf_f64_contract import CONTRACT,reconcile
from materialize_accounts import load

def run(base,target):
    base=Path(base);target=Path(target);target.mkdir(parents=True,exist_ok=True)
    predecessor=load(base/'vector-plan.json');snapshot=target/'exact-ledger.sqlite';backup_sqlite(base/'tracker.sqlite',snapshot)
    with sqlite3.connect('file:'+str(snapshot)+'?mode=ro',uri=True) as db:rows=db.execute('select job_id,receipt,sha256 from accepted where generation=?',(predecessor['generation'],)).fetchall()
    values=[]
    for key,path,sha in rows:
        if checksum(path)!=sha:raise ValueError('Retained Data receipt differs')
        values.append(dict(receipt=load(path),receipt_path=path,receipt_sha256=sha))
    jobs=reconcile(predecessor,values);sources={kind:sum(j['expected_rows'] for j in jobs if j['user_source']==kind) for kind in ['integer_table','integer_vector','new_float64']}
    record=dict(contract=CONTRACT,contract_sha256=digest(CONTRACT),predecessor_manifest_sha256=checksum(base/'vector-plan.json'),snapshot_sha256=checksum(snapshot),jobs=jobs,source_value_counts=sources,created_at=time.time(),float64_user_values_accepted=0,aggregate_blocks_accepted=0,production_enabled=False)
    # Preserve the explicit relocation proof for the two retired source paths.
    mapping=base/'retained-web-location-bootstrap.json'
    if mapping.exists():record['retained_location_mapping']=dict(path=str(mapping),sha256=checksum(mapping),data=load(mapping))
    out=target/'source-manifest.json'
    if out.exists():raise ValueError('Refuse overwrite frozen source manifest')
    write_json(out,record);print(json.dumps(dict(path=str(out),sha256=checksum(out),jobs=len(jobs),sources=sources,new_coverage=0)))
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--base',required=True);p.add_argument('--target',required=True);a=p.parse_args();run(a.base,a.target)
