"""OpenSSH forced command for the Node coordinator's dedicated Revert key."""
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys

from pipeline.revert_worker import PROTOCOL, parse_scope

ROOT = Path('/home/dsc-revert/data/node-coordinated-worker')
LOG_ROOT = Path('/home/dsc-revert/.local/state/dsc-revert-worker')
SCRIPTS = Path('/home/dsc-revert/scripts')
PYTHON = SCRIPTS / '.venv-web3/bin/python'
RECEIPT_SOURCE = 'node@10.246.103.160:/home/node/dsc-extraction/verification'
PUBLIC_PROGRESS = {
    'state', 'total_blocks', 'completed_blocks', 'archived_blocks',
    'failed_transactions', 'selected_transactions', 'providers', 'updated_at',
}


def parse_command(original):
    if len(original) > 512:
        raise ValueError('Command is too long')
    args = shlex.split(original)
    if len(args) == 2 and args == [PROTOCOL, 'ping']:
        return 'ping', None
    if len(args) == 3 and args[0] == PROTOCOL and args[1] in {
        'receive', 'trace', 'receipt-status', 'trace-status',
    }:
        return args[1], parse_scope(args[2])
    if re.match(r'^rsync --server --sender ', original):
        return 'download', None
    raise ValueError('Unsupported command')


def dispatch(operation, scope):
    if operation == 'ping':
        print(PROTOCOL)
        return
    if operation == 'download':
        os.execv('/usr/bin/rrsync', ['rrsync', '-ro', str(ROOT)])

    work = ROOT / scope
    if operation in ('receipt-status', 'trace-status'):
        folder = 'receipts' if operation == 'receipt-status' else 'traces'
        progress = work / folder / 'progress.json'
        payload = json.loads(progress.read_text()) if progress.exists() else {'state': 'missing'}
        print(json.dumps({key: value for key, value in payload.items() if key in PUBLIC_PROGRESS}))
        return

    if operation == 'receive':
        args = [PYTHON,SCRIPTS/'reverted/receive_receipts.py','--root',work/'receipts',
                '--source',f'{RECEIPT_SOURCE}/{scope}/receipts/']
    else:
        args = [PYTHON,SCRIPTS/'reverted/trace_received.py','--receipts',work/'receipts',
                '--output',work/'traces','--reth-rpc','http://10.246.103.160:8545','--reth-workers','32']
    log = LOG_ROOT / scope / (operation + '.log')
    log.parent.mkdir(parents=True, exist_ok=True)
    with log.open('a') as stream:
        subprocess.run(args, stdout=stream, stderr=subprocess.STDOUT, check=True)


def main():
    os.umask(0o077)
    try:
        operation, scope = parse_command(os.environ.get('SSH_ORIGINAL_COMMAND', ''))
        dispatch(operation, scope)
    except (ValueError, OSError, subprocess.SubprocessError):
        print('Revert worker request failed', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
