"""Existing SSH/rsync worker transports with coordinator heartbeats."""
import json
import os
from pathlib import Path
import shlex
import subprocess
import time

NODE='node@10.246.103.160'
REVERT='dsc-revert@10.246.103.26'
SCRIPTS=Path(os.environ.get('DSC_PIPELINE_DATA_SCRIPTS','/home/dsc-data/scripts'))
PYTHON='/home/dsc-data/scripts/.venv-abi/bin/python'
NODE_SCRIPTS='/home/node/dsc-extraction/scripts'
NODE_PYTHON='/home/node/dsc-extraction/.venv-abi/bin/python'
REVERT_SCRIPTS='/home/dsc-revert/scripts'
REVERT_PYTHON=REVERT_SCRIPTS+'/.venv-web3/bin/python'


def ssh(host, args, terminal=False):
    return ['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10','-o','ServerAliveInterval=15',
            '-o','ServerAliveCountMax=3']+(['-tt'] if terminal else [])+[host,shlex.join([str(x) for x in args])]


def remote_json(host, path):
    result=subprocess.run(ssh(host,['cat',path]),check=True,capture_output=True,text=True,timeout=30)
    return json.loads(result.stdout)


def rpc(method, params):
    code=('from web3 import Web3; import json; '
          "w=Web3(Web3.HTTPProvider('http://127.0.0.1:8545',request_kwargs={'timeout':30})); "
          'r=w.provider.make_request('+repr(method)+','+repr(params)+'); '
          "assert 'error' not in r, r.get('error'); print(json.dumps(r['result']))")
    r=subprocess.run(ssh(NODE,[NODE_PYTHON,'-c',code]),check=True,capture_output=True,text=True,timeout=45)
    return json.loads(r.stdout)


def run(command, log, heartbeat, env=None, progress=None):
    log.parent.mkdir(parents=True,exist_ok=True)
    with log.open('a') as stream:
        child=subprocess.Popen([str(x) for x in command],stdout=stream,stderr=subprocess.STDOUT,
                               env={**os.environ,**(env or {})})
        last_change=time.monotonic();last_value=None;checked=0
        try:
            while child.poll() is None:
                if time.monotonic()-checked>=10:
                    checked=time.monotonic()
                    try:value=progress() if progress else log.stat().st_mtime_ns
                    except (OSError,ValueError,subprocess.SubprocessError):value=last_value
                    if value!=last_value:last_value=value;last_change=time.monotonic()
                    if time.monotonic()-last_change>3600:raise TimeoutError(f'No worker progress for one hour; see {log}')
                heartbeat();time.sleep(1)
            if child.returncode:raise RuntimeError(f'Worker exited {child.returncode}; see {log}')
        finally:
            if child.poll() is None:
                child.terminate()
                try:child.wait(timeout=45)
                except subprocess.TimeoutExpired:child.kill();child.wait()


def transfer(source, destination, log, heartbeat):
    run(['rsync','-a','--partial','-e','ssh -o BatchMode=yes -o ConnectTimeout=10',source,destination],log,heartbeat)
