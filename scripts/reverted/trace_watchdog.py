"""Watch only the enabled hosted trace service; explicit disable is respected."""
import json
from pathlib import Path
import subprocess
import time
UNIT='dsc-hosted-traces.service'
if subprocess.run(['systemctl','is-enabled','--quiet',UNIT]).returncode==0:
    path=Path('/home/dsc-revert/data/archive/reverted/drpc-v1/progress.json')
    try:progress=json.loads(path.read_text())
    except (OSError,ValueError):progress={}
    active=subprocess.run(['systemctl','is-active','--quiet',UNIT]).returncode==0
    if not active and progress.get('state')!='complete':subprocess.run(['systemctl','start','--no-block',UNIT],check=True)
    elif active and time.time()-progress.get('updated_at',time.time())>300:subprocess.run(['systemctl','restart','--no-block',UNIT],check=True)
