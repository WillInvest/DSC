"""Install operational services using the already deployed frozen IO runtime."""
import pathlib
import subprocess

ROOT = '/home/fao/hf-flow-control-20261117'
PY = '/home/dsc-data/scripts/.venv-abi/bin/python'


def unit(name, body):
    p = pathlib.Path(ROOT)/name
    p.write_text(body)
    subprocess.run(['sudo','-n','install','-m','644',str(p),'/etc/systemd/system/'+name],check=True)


original = subprocess.check_output(['systemctl','cat','dsc-aave-v3-hf-flow-io.service'],text=True)
original = '\n'.join(l for l in original.splitlines() if not l.startswith('# /')).split('# /etc/systemd/system/dsc-aave-v3-hf-flow-io.service.d/')[0]
bindings = '\n'.join(l for l in original.splitlines() if l.startswith('BindReadOnlyPaths='))
env = next(l for l in original.splitlines() if l.startswith('Environment=PYTHONPATH='))
env = env.replace('PYTHONPATH=', 'PYTHONPATH='+ROOT+'/code:')
common = f'''[Unit]
Wants=network-online.target
After=network-online.target
[Service]
User=fao
WorkingDirectory=/home/dsc-data/scripts
{env}
Environment=PYTHONDONTWRITEBYTECODE=1
Environment=OPENBLAS_NUM_THREADS=1
Environment=OMP_NUM_THREADS=1
Environment=MALLOC_ARENA_MAX=2
{bindings}
UMask=0077
MemorySwapMax=0
Restart=on-failure
RestartSec=15
TimeoutStopSec=90
'''
tls = ' --tls-ca-file /home/dsc-data/worker-state/private/ca.pem --tls-cert /home/dsc-data/worker-state/private/host-cert.pem --tls-key /home/dsc-data/worker-state/private/host-key.pem'
unit('dsc-aave-v3-hf-flow-scheduler-compute.service', common+f'''MemoryMax=4G
ExecStart={PY} -m distributed.cli.dask_scheduler --host tls://10.246.103.143 --port 18896 --no-dashboard{tls}
[Install]
WantedBy=multi-user.target
''')
unit('dsc-aave-v3-hf-flow-control-compute.service', common+f'''MemoryMax=8G
Nice=10
ExecStart={PY} {ROOT}/code/hf_flow_control.py --config {ROOT}/flow-config.json
[Install]
WantedBy=multi-user.target
''')
unit('dsc-aave-v3-hf-flow-reconcile-compute.service', common.replace('Restart=on-failure','Restart=no')+f'''MemoryMax=8G
ExecStart={PY} {ROOT}/promote.py reconcile
''')
subprocess.run(['sudo','-n','systemctl','daemon-reload'],check=True)
subprocess.run(['sudo','-n','systemctl','enable','--now','dsc-aave-v3-hf-flow-scheduler-compute.service'],check=True)
subprocess.run(['sudo','-n','systemctl','start','--no-block','dsc-aave-v3-hf-flow-reconcile-compute.service'],check=True)
print('Scheduler started; explicit ledger reconciliation started; controller not yet enabled')
