#!/usr/bin/env python3
"""Systemd watchdog: restart only our two units after a lost heartbeat."""
import json
from pathlib import Path
import subprocess
import time


def check(unit, file, timeout, complete=False):
    active=subprocess.run(['systemctl','is-active','--quiet',unit]).returncode==0
    # oneshot units report activating while their process is running.
    sub=subprocess.check_output(['systemctl','show','-p','SubState','--value',unit],text=True).strip()
    active=active or sub in ('start','running')
    try:
        state=json.loads(Path(file).read_text())
        stale=time.time()-state['updated_at']>timeout
    except (OSError,ValueError,KeyError):
        state={};stale=False
    if active and stale:
        subprocess.run(['systemctl','restart','--no-block',unit],check=True)
    elif not active and not (complete and state.get('state')=='complete'):
        subprocess.run(['systemctl','start','--no-block',unit],check=True)


check('dsc-sequential-events.service','/home/dsc-data/runs/sequential-events/progress.json',300,True)
check('dsc-event-pipeline.service','/home/dsc-data/runs/event-pipeline/progress.json',300)
