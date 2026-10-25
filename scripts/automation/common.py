"""Shared paths and pure selection for the fixed historical event plan."""
import json
from pathlib import Path

SCRIPTS = Path('/home/dsc-data/scripts')
ROOT = Path('/home/dsc-data/runs/sequential-events')
AUTO = Path('/home/dsc-data/runs/event-pipeline')
PYTHON = str(SCRIPTS / '.venv-abi/bin/python')
PROFILES = Path('/home/dsc-data/runs/decode-two/profiles')


def read(path, default=None):
    try:
        return json.loads(Path(path).read_text())
    except FileNotFoundError:
        return default


def paths(job):
    encoded = Path(job['destination'])
    base = encoded.parent.parent
    return encoded, base / 'decoded' / encoded.name, base / 'reconstructed' / encoded.name


def select(plan, received, states, now):
    ready = [j for j in plan['jobs'] if received.get(j['address'], {}).get('state') == 'complete']
    pending = [j for j in ready if states.get(j['address'], {}).get('decoded') is not True]
    phase = 'decode' if pending else 'reconstruct'
    if not pending:
        pending = [j for j in ready if states.get(j['address'], {}).get('reconstructed') is not True]
    for job in pending:
        if states.get(job['address'], {}).get('retry_after', 0) <= now:
            return phase, job
    return ('retry_wait' if pending else 'waiting'), None
