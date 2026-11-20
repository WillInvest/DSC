"""Move only the new flow IO/cloud services; leave numeric and legacy units alone."""
import json
import pathlib
import subprocess
import time

role = __import__('sys').argv[1]
roots = {'node':'/home/node/hf-flow-20261116', 'revert':'/home/dsc-revert/hf-flow-20261116',
         'web':'/home/fao/dsc/data/incoming/aave-hf-flow-20261116'}
r = pathlib.Path(roots[role])
p = r/'role.json'
before = p.read_bytes()
backup = r/'recovery'/'before-compute-coordinator-role.json'
backup.parent.mkdir(exist_ok=True)
if not backup.exists():
    backup.write_bytes(before)
c = json.loads(before)
c['scheduler'] = 'tls://10.246.103.143:18896'
t = p.with_suffix('.json.compute-new')
t.write_text(json.dumps(c,sort_keys=True))
t.replace(p)
units = ['dsc-aave-v3-hf-flow-io.service']
if role == 'node':
    units.append('dsc-aave-v3-hf-flow-cloud.service')
for name in units:
    original = subprocess.check_output(['systemctl','cat',name],text=True)
    starts = [l for l in original.splitlines() if l.startswith('ExecStart=') and l != 'ExecStart=']
    assert starts and 'distributed.cli.dask_worker' in starts[-1]
    command = starts[-1].replace('tls://10.246.103.151:18896','tls://10.246.103.143:18896')
    d = r/(name+'.compute-coordinator.conf')
    d.write_text('[Service]\nExecStart=\n'+command+'\n')
    dest = '/etc/systemd/system/'+name+'.d'
    subprocess.run(['sudo','-n','mkdir','-p',dest],check=True)
    subprocess.run(['sudo','-n','install','-m','644',str(d),dest+'/90-compute-coordinator.conf'],check=True)
subprocess.run(['sudo','-n','systemctl','daemon-reload'],check=True)
subprocess.run(['sudo','-n','systemctl','restart',*units],check=True)
print(json.dumps(dict(role=role,moved_control_units=units,numerical_producer_restarted=False,at=time.time())))
