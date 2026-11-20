"""Hourly read-only node comparisons; writes only separate audit reports."""
import datetime,json,sys,hashlib,shutil,subprocess,time
from pathlib import Path
sys.path.insert(0,'/home/dsc-data/scripts')
import pyarrow.dataset as ds
from shared.artifacts import checksum,write_json
from pipeline.runtime import rpc

ROOT=Path('/home/dsc-data/runs/aave-user-history-v3')
def normalized(row):
    return (int(row['block_number']),row['block_hash'].lower(),int(row['transaction_index']),row['transaction_hash'].lower(),int(row['log_index']),row['address'].lower(),tuple(row.get('topic'+str(i)) for i in range(4) if row.get('topic'+str(i)) is not None),row['data'].lower())
def rpc_row(row):
    result=dict(block_number=int(row['blockNumber'],16),block_hash=row['blockHash'],transaction_index=int(row['transactionIndex'],16),transaction_hash=row['transactionHash'],log_index=int(row['logIndex'],16),address=row['address'],data=row['data'])
    result.update({'topic'+str(i):t for i,t in enumerate(row['topics'])});return result
def main():
    stamp=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    result=dict(created_at=stamp,scope='read-only supplemental token input audit; not user/HF validation',samples=[],errors=[],rpc_calls=0,user_history_complete=False)
    try:
        current=json.loads((ROOT/'current.json').read_text());root=Path(current['root']);plan=json.loads(Path(current['plan']).read_text());progress=json.loads((root/'progress.json').read_text());states=json.loads((root/'jobs.json').read_text()) if (root/'jobs.json').exists() else {}
        result.update(run_id=current['run_id'],progress=progress,free_bytes=shutil.disk_usage('/home/dsc-data/data').free)
        for stage in ['decode','position']:
            path=root/(stage+'-progress.json')
            result[stage+'_progress']=json.loads(path.read_text()) if path.exists() else None
            path=root/(stage+'-jobs.json')
            jobs=json.loads(path.read_text()) if path.exists() else {}
            for address,status in jobs.items():
                if status.get('state')=='failed':result['errors'].append(stage+' failed: '+address+' '+status.get('error',''))
            service=subprocess.run(['systemctl','show','dsc-aave-v3-user-'+('decode' if stage=='decode' else 'positions')+'.service','-p','ActiveState','-p','SubState','-p','ExecMainStatus'],capture_output=True,text=True,check=True)
            result[stage+'_service']=service.stdout
        header=rpc('eth_getBlockByNumber',[hex(plan['end_block']),False]);result['rpc_calls']+=1
        if header['hash']!=plan['end_hash']:result['errors'].append('Pinned ending block hash differs')
        service=subprocess.run(['systemctl','show','dsc-aave-v3-user-inputs.service','-p','ActiveState','-p','SubState','-p','ExecMainStatus'],capture_output=True,text=True,check=True);result['service']=service.stdout
        if progress['state']=='running' and time.time()-progress['updated_at']>600:result['errors'].append('Coordinator heartbeat older than 10 minutes')
        accepted=[j for j in plan['jobs'] if states.get(j['address'],{}).get('state')=='raw_accepted']
        control=json.loads((root/'priority.json').read_text()) if (root/'priority.json').exists() else {}
        focus=control.get('priority_assets',[])
        if focus:
            accepted=[j for j in accepted if j['asset'] in focus]
            groups={asset:[j for j in accepted if j['asset']==asset] for asset in focus}
            result['pending_priority_assets']=[asset for asset,jobs in groups.items() if not jobs]
            # Represent each available priority asset, rotating its token type.
            accepted=[jobs[int(hashlib.sha256((stamp[:11]+asset).encode()).hexdigest(),16)%len(jobs)] for asset,jobs in groups.items() if jobs]
        accepted.sort(key=lambda j:hashlib.sha256((stamp[:11]+j['address']).encode()).hexdigest())
        for job in accepted[:4]:
            manifest=json.loads(Path(states[job['address']]['receipt']).read_text());entries=list(manifest['files'].items())
            if not entries:continue
            # Rotate a bounded selection of accepted, explicitly listed staging files.
            entries.sort(key=lambda x:hashlib.sha256((stamp[:11]+x[0]).encode()).hexdigest())
            positive=[x for x in entries if x[1]['rows']];empty=[x for x in entries if not x[1]['rows']]
            chosen=positive[:1]+empty[:1]
            if len(chosen)<2:chosen=(positive or empty)[:2]
            for name,info in chosen:
                path=Path(job['destination'])/name
                if checksum(path)!=info['sha256']:
                    result['errors'].append('Staging checksum differs: '+str(path));continue
                table=ds.dataset(str(path),format='parquet');rows=table.to_table().to_pylist()
                block=rows[len(rows)//2]['block_number'] if rows else info['start']
                chain=rpc('eth_getLogs',[dict(address=job['address'],fromBlock=hex(block),toBlock=hex(block))]);result['rpc_calls']+=1
                expected=sorted(normalized(r) for r in rows if r['block_number']==block)
                actual=sorted(normalized(rpc_row(r)) for r in chain)
                exact=expected==actual
                block_hash=rows[len(rows)//2]['block_hash'] if rows else rpc('eth_getBlockByNumber',[hex(block),False])['hash']
                if not rows:result['rpc_calls']+=1
                result['samples'].append(dict(address=job['address'],asset=job['asset'],kind=job['kind'],block=block,block_hash=block_hash,file=str(path),stored_rows=len(expected),node_rows=len(actual),all_fields_exact=exact))
                if not exact:result['errors'].append('Node log mismatch: '+job['address']+' '+str(block))
        result['validation_state']='failed' if result['errors'] else ('raw_samples_passed' if result['samples'] else 'no_accepted_inputs_to_sample_yet')
        result['pending_stages']=plan['next_stages']
    except Exception as exc:
        result['errors'].append(type(exc).__name__+': '+str(exc));result['validation_state']='audit_error'
    # No configuration, job, receipt, catalog or data file is modified by this audit.
    write_json(ROOT/'audits'/(stamp+'.json'),result);write_json(ROOT/'audit-latest.json',result)
    print(json.dumps(result))
    return 1 if result['errors'] else 0
if __name__=='__main__':raise SystemExit(main())
