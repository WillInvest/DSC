#!/usr/bin/env python3
"""Process one ready task; systemd's timer rescans after it exits."""
import fcntl
from pathlib import Path
import subprocess
import sys
import time
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'event-cryo-extraction'))
from automation.common import AUTO, ROOT, SCRIPTS, PYTHON, PROFILES, paths, read, select
from shared.artifacts import checksum, write_json
from run import validate_manifest, validate_destination


def main():
    AUTO.mkdir(parents=True,exist_ok=True)
    lock=(AUTO/'pipeline.lock').open('w')
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    plan=read(ROOT/'plan.json'); received=read(ROOT/'jobs.json',{})
    states=read(AUTO/'jobs.json',{})
    for item in plan['jobs']:
        state=states.get(item['address'],{})
        receipt=ROOT/'receipts'/f"{item['address']}.json"
        if state.get('decoded') and (not receipt.exists() or state.get('extraction_receipt')!=checksum(receipt)):
            state.update(decoded=False,reconstructed=False)
    # Existing manually decoded datasets are adopted only after the normal
    # resumable decoder has revalidated their receipts and checksums once.
    phase,job=select(plan,received,states,time.time())
    status=dict(state=phase,updated_at=time.time(),current=job)
    write_json(AUTO/'progress.json',status)
    if job is None:
        return
    address=job['address'];state=states.setdefault(address,{})
    encoded,decoded,output=paths(job)
    try:
        manifest=read(ROOT/'receipts'/f'{address}.json')
        validate_manifest(manifest,plan,job)
        if phase=='decode':
            validate_destination(manifest,encoded)
        if phase=='decode':
            cmd=[PYTHON,str(SCRIPTS/'decoding/decode_event_files.py'),'--protocol',job['protocol'],
                 '--input',str(encoded),'--output',str(decoded),'--workers','32']
            if job['protocol'].startswith('aave-'):
                cmd+=['--profiles',str(PROFILES)]
            progress_file=decoded/'decode-progress.json'
            status['reused_rows']=sum(read(p)['rows'] for p in decoded.glob('*/receipt.json'))
            write_json(AUTO/'progress.json',status)
        else:
            cmd=[PYTHON,str(SCRIPTS/'automation/reconstruct.py'),address]
            progress_file=output/'reconstruction-progress.json'
        with (AUTO/f'{address}-{phase}.log').open('a') as log:
            child=subprocess.Popen(cmd,stdout=log,stderr=subprocess.STDOUT)
            last_change=time.monotonic();last_stamp=None
            try:
                while child.poll() is None:
                    stamp=progress_file.stat().st_mtime_ns if progress_file.exists() else None
                    if stamp!=last_stamp:
                        last_stamp=stamp;last_change=time.monotonic()
                    if time.monotonic()-last_change>3600:
                        raise TimeoutError('No completed work or phase change for one hour')
                    status['updated_at']=time.time();write_json(AUTO/'progress.json',status)
                    time.sleep(10)
                if child.returncode:
                    raise RuntimeError(f'{phase} exited {child.returncode}; see {address}-{phase}.log')
            finally:
                if child.poll() is None:
                    child.terminate()
                    try: child.wait(timeout=30)
                    except subprocess.TimeoutExpired: child.kill();child.wait()
        if phase=='decode':
            progress=read(progress_file)
            if progress['state']!='complete' or progress['decoded_rows']!=sum(f['rows'] for f in manifest['files'].values()):
                raise ValueError('Decoded rows do not match accepted extraction')
            state['decoded']=True
            state['extraction_receipt']=checksum(ROOT/'receipts'/f'{address}.json')
        else:
            state['reconstructed']=read(progress_file)['state']=='complete'
        state.pop('error',None);state.pop('retry_after',None)
        status['state']='ready'
    except Exception as exc:
        state.update(error=str(exc),retry_after=time.time()+300)
        status.update(state='retry_wait',error=str(exc))
        print(str(exc),flush=True)
    write_json(AUTO/'jobs.json',states)
    status['updated_at']=time.time();write_json(AUTO/'progress.json',status)
    return phase if status['state']=='ready' else None


if __name__=='__main__':
    while main() == 'reconstruct':
        pass
