"""Tailnet-only read-only UI, backed by cached fixed-command SSH reports."""
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import time

from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles

snapshot = None
last_success = 0
collection_error = False


def collect():
    global snapshot, last_success, collection_error
    try:
        result = subprocess.run(['ssh','-i',os.environ['MONITOR_SSH_KEY'],'-o','BatchMode=yes',
            '-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=5','dsc-data@10.246.103.151','report'],
            check=True,capture_output=True,text=True,timeout=20)
        data = json.loads(result.stdout)
        if not isinstance(data.get('sources'), list):
            raise ValueError('invalid report')
        snapshot = data
        last_success = time.time()
        collection_error = False
    except (OSError,ValueError,subprocess.SubprocessError):
        collection_error = True


@asynccontextmanager
async def lifespan(app):
    async def poll():
        while True:
            await asyncio.to_thread(collect)
            await asyncio.sleep(30)
    task = asyncio.create_task(poll())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)


def current():
    if snapshot is None:
        raise HTTPException(503, 'Waiting for a report from dsc-data')
    return snapshot


def selected(run_id):
    for source in current()['sources']:
        for run in source['runs']:
            if run['id'] == run_id:
                return run
    raise HTTPException(404, 'Run not found')


@app.get('/api/overview')
def overview():
    data = current()
    age = max(0,time.time()-last_success)
    sources = []
    for source in data['sources']:
        heartbeat = source.get('heartbeat')
        runner = 'Not started'
        if heartbeat:
            seconds = (datetime.now(timezone.utc)-datetime.fromisoformat(heartbeat['timestamp'])).total_seconds()
            runner = 'Stopped' if heartbeat['state']=='stopped' else ('Heartbeat missing' if seconds>60 else 'Running')
        runs = [{**run, 'contracts':[{k:v for k,v in c.items() if k!='intervals'} for c in run['contracts']]} for run in source['runs']]
        sources.append({**source, 'runs':runs, 'runner':runner})
    return {**data, 'sources':sources, 'age_seconds':age, 'stale':age>90, 'collection_error':collection_error}


@app.get('/api/contracts')
def contracts(run: str):
    return [{k:v for k,v in c.items() if k!='intervals'} for c in selected(run)['contracts']]


@app.get('/api/jobs')
def jobs(run: str):
    data = selected(run)
    return {'counts':data['job_counts'], 'jobs':data['jobs']}


@app.get('/api/coverage')
def coverage(run: str, addresses: str, start: int = Query(ge=0), count: int = Query(default=100,ge=1,le=1000)):
    requested = addresses.lower().split(',')
    if len(set(requested)) != len(requested) or not 1 <= len(requested) <= 10:
        raise HTTPException(422,'Request one to ten distinct contracts')
    data = selected(run)
    lookup = {c['address'].lower():c for c in data['contracts']}
    if any(address not in lookup for address in requested):
        raise HTTPException(404,'Contract not in this run')
    rows = []
    for n in range(start,start+count):
        cells=[]
        for address in requested:
            c=lookup[address]
            if n<int(c['creation_block']):
                cells.append('N/A')
            elif not c['start']<=n<c['stop']:
                cells.append('Outside run')
            else:
                cells.append(next((mask for a,b,mask in c['intervals'] if a<=n<b),0))
        rows.append({'block':n,'values':cells})
    return {'addresses':requested,'rows':rows}


@app.get('/healthz')
def health():
    return {'service':'ok','report_available':snapshot is not None}


app.mount('/',StaticFiles(directory=Path(__file__).parent/'static',html=True),name='ui')
