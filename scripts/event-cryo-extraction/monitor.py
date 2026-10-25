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


def collect():
    global snapshot, fetched_at, error
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
    return JSONResponse({'data': snapshot, 'fetched_at': fetched_at, 'error': error},
                        headers={'Cache-Control': 'no-store'})


@app.get('/')
def index():
    return FileResponse(Path(__file__).with_name('index.html'), headers={'Cache-Control': 'no-store'})
