"""Existing SSH/rsync worker transports with coordinator heartbeats."""
import json
from contextlib import contextmanager
import os
from pathlib import Path
import shlex
import subprocess
import time

NODE='node@10.246.103.160'
REVERT='dsc-revert@10.246.103.26'
NODE_SCRIPTS='/home/node/dsc-extraction/scripts'
NODE_PYTHON='/home/node/dsc-extraction/.venv-abi/bin/python'
REVERT_SCRIPTS='/home/dsc-revert/scripts'
REVERT_PYTHON=REVERT_SCRIPTS+'/.venv-web3/bin/python'
SITE = os.environ.get('DSC_PIPELINE_SITE', 'data')
if SITE not in ('data', 'node'):
    raise ValueError(f'Unsupported pipeline site: {SITE}')
NODE_LOCAL = SITE == 'node'
DEFAULT_SCRIPTS = NODE_SCRIPTS if NODE_LOCAL else '/home/dsc-data/scripts'
SCRIPTS = Path(os.environ.get('DSC_PIPELINE_SCRIPTS') or
               os.environ.get('DSC_PIPELINE_DATA_SCRIPTS') or DEFAULT_SCRIPTS)
PYTHON = NODE_PYTHON if NODE_LOCAL else '/home/dsc-data/scripts/.venv-abi/bin/python'
REGISTRY = SCRIPTS.parent / 'contracts/ethereum/pools.csv' if NODE_LOCAL else Path('/home/dsc-data/contracts/ethereum/pools.csv')


def ssh(host, args, terminal=False):
    return ['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10','-o','ServerAliveInterval=15',
            '-o','ServerAliveCountMax=3']+(['-tt'] if terminal else [])+[host,shlex.join([str(x) for x in args])]


def node_command(args, terminal=False):
    return [str(arg) for arg in args] if NODE_LOCAL else ssh(NODE, args, terminal)


def node_path(path):
    return str(path) if NODE_LOCAL else f'{NODE}:{path}'


def remote_json(host, path):
    result=subprocess.run(ssh(host,['cat',path]),check=True,capture_output=True,text=True,timeout=30)
    return json.loads(result.stdout)


def node_json(path):
    return json.loads(Path(path).read_text()) if NODE_LOCAL else remote_json(NODE, path)


def rpc(method, params):
    code=('from web3 import Web3; import json; '
          "w=Web3(Web3.HTTPProvider('http://127.0.0.1:8545',request_kwargs={'timeout':30})); "
          'r=w.provider.make_request('+repr(method)+','+repr(params)+'); '
          "assert 'error' not in r, r.get('error'); print(json.dumps(r['result']))")
    r=subprocess.run(node_command([NODE_PYTHON,'-c',code]),check=True,capture_output=True,text=True,timeout=45)
    return json.loads(r.stdout)


@contextmanager
def running_worker(command, **kwargs):
    with subprocess.Popen(command, **kwargs) as child:
        try:
            yield child
        finally:
            if child.poll() is None:
                child.terminate()
                try: child.wait(timeout=45)
                except subprocess.TimeoutExpired: child.kill();child.wait()


def run(command, log, heartbeat, env=None, progress=None):
    log.parent.mkdir(parents=True,exist_ok=True)
    with log.open('a') as stream:
        args = [str(x) for x in command]
        with running_worker(args, stdout=stream, stderr=subprocess.STDOUT, env={**os.environ,**(env or {})}) as child:
            last_change=time.monotonic();last_value=None;checked=0
            while child.poll() is None:
                if time.monotonic()-checked>=10:
                    checked=time.monotonic()
                    try:value=progress() if progress else log.stat().st_mtime_ns
                    except (OSError,ValueError,subprocess.SubprocessError):value=last_value
                    if value!=last_value:last_value=value;last_change=time.monotonic()
                    if time.monotonic()-last_change>3600:raise TimeoutError(f'No worker progress for one hour; see {log}')
                heartbeat();time.sleep(1)
            if child.returncode:raise RuntimeError(f'Worker exited {child.returncode}; see {log}')


def transfer(source, destination, log, heartbeat):
    run(rsync_command(source, destination), log, heartbeat)


def rsync_command(source, destination, *options):
    return ['rsync', '-a', '--partial', *options, '-e',
            'ssh -o BatchMode=yes -o ConnectTimeout=10', str(source), str(destination)]
