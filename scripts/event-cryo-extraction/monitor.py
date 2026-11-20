"""Cache one small JSON snapshot from dsc-data; serve the existing private monitor URL."""
import asyncio
from contextlib import asynccontextmanager
import json
import os
from pathlib import Path
import subprocess
import shutil
import time

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse

snapshot = None
fetched_at = 0
error = None
hf_fallback_error = None
hf_fallback_fetched_at = 0
try:
    cached=json.loads(Path(__file__).with_name('failover-baseline.json').read_text())
    snapshot,fetched_at=cached['data'],cached['fetched_at']
except (OSError,ValueError,KeyError):
    pass


def collect():
    global snapshot, fetched_at, error, hf_fallback_error, hf_fallback_fetched_at
    try:
        result = subprocess.run(['ssh', '-i', os.environ['MONITOR_SSH_KEY'],
                 '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes', '-o', 'ConnectTimeout=5',
                 'dsc-data@10.246.103.151', 'report'], check=True, capture_output=True, text=True, timeout=15)
        data = json.loads(result.stdout)
        if data.get('total_addresses') != 102:
            raise ValueError('Unexpected progress format')
        total,used,free=shutil.disk_usage('/')
        data.setdefault('storage',[]).append(dict(server='dsc-web',mount='/',total_bytes=total,
            used_bytes=used,available_bytes=free,percent=100*used/total,updated_at=time.time(),error=None))
        snapshot, fetched_at, error = data, time.time(), None
    except (OSError, ValueError, subprocess.SubprocessError):
        error = 'Cannot refresh from dsc-data'
    config=Path(__file__).with_name('hf-failover-sources.json')
    if config.exists():
        try:
            from failover_monitor import collect as collect_hf
            hf,hf_fallback_error=collect_hf(json.loads(config.read_text()))
            if hf is not None:
                data=dict(snapshot or {});data['aave_hf']=hf
                data['aave_hf_inputs']=dict(state='factorized_history_ready',completed_blocks=10116755,total_blocks=10116755,waiting_for=[],error=None)
                snapshot=data;hf_fallback_fetched_at=time.time()
        except (OSError,ValueError):hf_fallback_error='Cannot refresh independent HF reports'


@asynccontextmanager
async def lifespan(app):
    async def poll():
        while True:
            await asyncio.to_thread(collect)
            await asyncio.sleep(10)
    task = asyncio.create_task(poll())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)


@app.get('/api/progress')
def progress():
    from failover_monitor import age_cached_report,with_recovery_notice
    data=dict(snapshot) if snapshot is not None else None
    if data is not None and data.get('aave_hf'):
        data['aave_hf']=age_cached_report(data['aave_hf'])
        try:
            notice=json.loads(Path(__file__).with_name('hf-recovery-notice.json').read_text())
            data['aave_hf']=with_recovery_notice(data['aave_hf'],notice)
        except (OSError,ValueError):
            pass
    return JSONResponse({'data': data, 'fetched_at': fetched_at, 'error': error,
                         'hf_fallback_error':hf_fallback_error,'hf_fallback_fetched_at':hf_fallback_fetched_at},
                        headers={'Cache-Control': 'no-store'})


@app.get('/')
def index():
    return FileResponse(Path(__file__).with_name('index.html'), headers={'Cache-Control': 'no-store'})
