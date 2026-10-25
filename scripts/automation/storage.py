"""Read-only filesystem usage for data/node/revert, cached once per minute."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import json
import shutil
import subprocess
import time

FILE=Path('/home/dsc-data/runs/event-pipeline/storage.json')


def usage(name, destination=None):
    try:
        if destination:
            result=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=5',destination,
                'df -B1 --output=size,used,avail /'],capture_output=True,text=True,check=True,timeout=10)
            total,used,free=map(int,result.stdout.splitlines()[-1].split())
        else:
            total,used,free=shutil.disk_usage('/')
        return dict(server=name,total_bytes=total,used_bytes=used,available_bytes=free,
                    percent=100*used/total,mount='/',updated_at=time.time(),error=None)
    except (OSError,ValueError,subprocess.SubprocessError):
        return dict(server=name,mount='/',updated_at=time.time(),error='Storage unavailable')


def collect():
    if FILE.exists() and time.time()-FILE.stat().st_mtime<60: return
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures=[pool.submit(usage,*item) for item in [('dsc-data',None),
            ('dsc-node','node@10.246.103.160'),('dsc-revert','dsc-revert@10.246.103.26')]]
        results=[f.result() for f in futures]
    FILE.parent.mkdir(parents=True,exist_ok=True)
    temp=FILE.with_suffix('.tmp');temp.write_text(json.dumps(results));temp.replace(FILE)
