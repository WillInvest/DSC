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


def heartbeat_status(heartbeat):
    if not heartbeat:
        return 'Not started'
    if heartbeat.get('state') in ('stopped','complete'):
        return heartbeat['state'].capitalize()
    try:
        stamp = datetime.fromisoformat(heartbeat['timestamp'])
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=timezone.utc)
        seconds = (datetime.now(timezone.utc)-stamp).total_seconds()
    except (KeyError,TypeError,ValueError):
        return 'Heartbeat missing'
    return 'Running' if heartbeat.get('state') == 'running' and 0 <= seconds <= 60 else 'Heartbeat missing'


def source_report(source):
    lanes = {name:{**lane,'status':heartbeat_status(lane.get('heartbeat'))}
             for name,lane in source.get('lanes',{}).items()}
    statuses = [lane['status'] for lane in lanes.values()]
    runner = heartbeat_status(source.get('heartbeat'))
    if statuses:
        if all(s == 'Complete' for s in statuses):
            runner = 'Complete'
        elif all(s in ('Running','Complete') for s in statuses):
            runner = 'Running'
        elif len(set(statuses)) == 1:
            runner = statuses[0]
        else:
            runner = 'Needs attention'
    runs = []
    for run in source['runs']:
        contracts = []
        for contract in run['contracts']:
            work = []
            for job in contract.get('active_jobs',[]):
                if lanes:
                    lane = lanes[job['lane']]
                    progress = lane.get('progress') or {}
                    live = lane['status'] == 'Running' and progress.get('run_id') == run.get('local_id')
                    live = live and job['start'] < progress.get('stop',0) and job['stop'] > progress.get('start',0)
                else:
                    active = source.get('active') or {}
                    live = runner == 'Running' and active.get('run_id') == run.get('local_id')
                if live:
                    work.append(job)
            contracts.append({k:v for k,v in contract.items() if k not in ('intervals','active_jobs')} |
                             {'running':bool(work), 'current_work':work})
        # Stable sorts provide descending activity with an ascending address tie-break.
        contracts.sort(key=lambda c:c['address'])
        contracts.sort(key=lambda c:c.get('updated') or '',reverse=True)
        contracts.sort(key=lambda c:c['running'],reverse=True)
        runs.append({**run,'contracts':contracts})
    return {**source,'lanes':lanes,'runs':runs,'runner':runner}


@app.get('/api/overview')
def overview():
    data = current()
    age = max(0,time.time()-last_success)
    return {**data, 'sources':[source_report(s) for s in data['sources']],
            'age_seconds':age, 'stale':age>90, 'collection_error':collection_error}


@app.get('/api/contracts')
def contracts(run: str):
    for source in current()['sources']:
        for candidate in source_report(source)['runs']:
            if candidate['id'] == run:
                return candidate['contracts']
    raise HTTPException(404,'Run not found')


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
