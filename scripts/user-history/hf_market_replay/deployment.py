"""Install separate cohort worker services using verified frozen host runtimes.

Run from the operator checkout. This command does not install SSH credentials or
change existing extraction, HF, application or tracing services.
"""
import argparse
import json
import subprocess
from pathlib import Path

HOSTS = {
    'node': dict(ssh='node@100.108.171.14', user='node',
                 base='/home/node/hf-cohort-20261120', slots=16,
                 worker='/home/node/hf-cohort-20261120/storage/worker-v2',
                 image='/home/node/aave-hf-distributed/5197dae89b140ccb/images/1d1a9be72b345989a4a8667573e929c66320535824f480517e6e440633d0e32d',
                 overlay='/home/node/hf-f64-20261116/overlay',
                 science='/home/node/hf-f64-v2-20261116/code', checkpoint_role='node'),
    'revert': dict(ssh='dsc-revert@100.91.215.76', user='dsc-revert',
                   base='/home/dsc-revert/hf-cohort-20261120', slots=20,
                   image='/home/dsc-revert/aave-hf-distributed/5197dae89b140ccb/images/1d1a9be72b345989a4a8667573e929c66320535824f480517e6e440633d0e32d',
                   overlay='/home/dsc-revert/hf-f64-20261116/overlay',
                   science='/home/dsc-revert/hf-f64-v2-20261116/code', checkpoint_role='revert'),
    'compute': dict(ssh='fao@100.119.128.31', user='fao',
                    base='/home/fao/hf-cohort-20261120', slots=20,
                    image='/home/fao/aave-hf-distributed/5197dae89b140ccb/images/1d1a9be72b345989a4a8667573e929c66320535824f480517e6e440633d0e32d',
                    overlay='/home/fao/hf-f64-20261116/overlay',
                    science='/home/fao/hf-f64-v2-20261116/code', checkpoint_role='web'),
    'data': dict(ssh='dsc-data@100.79.189.8', user='dsc-data',
                 base='/var/lib/dsc-aave-hf-cohort/20261120', slots=20,
                 image='/home/dsc-data/hf-distributed/snapshots/20261115-v2/image',
                 overlay='/var/lib/dsc-aave-hf-f64/20261116/overlay',
                 science='/var/lib/dsc-aave-hf-f64/20261116-v2/code', checkpoint_role='data'),
}
WORKER_UNIT = 'dsc-aave-hf-cohort-20261120-worker.service'
IO_UNIT = 'dsc-aave-hf-cohort-20261120-io.service'
SCHEDULER = 'tls://10.246.103.160:18920'
NETWORK = dict(node='10.246.103.160', revert='10.246.103.26',
               compute='10.246.103.143', data='10.246.103.151')


def ssh(host, command, content=None):
    return subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10',
                           host['ssh'], command], input=content, text=True,
                          capture_output=True, check=True)


def unit_text(host, role, config_path):
    base = host['base']
    code = base + '/code-v2'
    python = (host['image'] + '/scripts/.venv-abi/bin/python'
              if role != 'data' else '/home/dsc-data/scripts/.venv-abi/bin/python')
    pythonpath = ':'.join((code, host['overlay'], host['science'],
                           '/home/dsc-data/scripts/user-history', '/home/dsc-data/scripts'))
    mount = 'RequiresMountsFor=/home/node/hf-cohort-20261120/storage\n' if role == 'node' else ''
    return f'''[Unit]
Description=Aave HF cohort replay worker ({role}, 20261120)
Wants=network-online.target
After=network-online.target
{mount}
[Service]
Type=simple
User={host['user']}
WorkingDirectory=/home/dsc-data/scripts
BindReadOnlyPaths={host['image']}:/home/dsc-data
Environment=PYTHONPATH={pythonpath}
Environment=PYTHONDONTWRITEBYTECODE=1
Environment=OPENBLAS_NUM_THREADS=1
Environment=OMP_NUM_THREADS=1
Environment=MALLOC_ARENA_MAX=2
TemporaryFileSystem=/dev/shm:mode=1777,size=256M
ExecStart={python} -m hf_market_replay worker --config {config_path}
MemoryMax={'24G' if role == 'node' else '20G'}
MemorySwapMax=0
CPUQuota={host['slots'] * 100}%
Nice=10
UMask=0077
Restart=on-failure
RestartSec=30
KillMode=control-group
TimeoutStopSec=90

[Install]
WantedBy=multi-user.target
'''


def install(role, plan_path):
    host = HOSTS[role]
    base = host['base']
    worker = host.get('worker', base + '/worker-v2')
    plan_path = Path(plan_path)
    plan = json.loads(plan_path.read_text())
    if role == 'node':
        setup_storage(host)
    ssh(host, 'sudo -n mkdir -p ' + base + ' ' + worker +
        ' && sudo -n chown ' + host['user'] + ':' + host['user'] + ' ' + base + ' ' + worker)
    ssh(host, 'mkdir -p ' + base + '/code-v2')
    package = Path(__file__).parent
    shared = package.parent.parent / 'shared'
    for source in (package, shared):
        subprocess.run(['rsync', '-a', '--exclude', '__pycache__', str(source),
                        host['ssh'] + ':' + base + '/code-v2/'], check=True)
    subprocess.run(['rsync', '-a', str(plan_path), host['ssh'] + ':' + base + '/plan-v2.json'], check=True)
    checkpoints = [item for item in plan['original_checkpoints']
                   if item['role'] == host['checkpoint_role']]
    config = dict(root=worker, role=role, slots=host['slots'], plan_path=base + '/plan-v2.json',
                  checkpoints=checkpoints, part_blocks=plan['identity']['encoding']['part_blocks'],
                  reserve_bytes=50 * 1024**3, max_job_seconds=7200)
    content = 'from pathlib import Path\nimport json\np=Path(' + repr(worker + '/role.json') + ')\n'
    content += 'p.write_text(' + repr(json.dumps(config, sort_keys=True) + '\n') + ')\np.chmod(0o600)\n'
    ssh(host, 'python3 -', content)
    unit = unit_text(host, role, worker + '/role.json')
    install_script = 'from pathlib import Path\np=Path(' + repr('/etc/systemd/system/' + WORKER_UNIT) + ')\n'
    install_script += 'content=' + repr(unit) + '\n'
    install_script += 'changed=p.exists() and p.read_text()!=content\n'
    install_script += 'if changed:\n'
    install_script += ' old=p.read_text()\n'
    install_script += ' if '+repr('-m hf_market_replay worker --config ' + base + '/')+' not in old: raise RuntimeError("Existing unit is outside this cohort deployment")\n'
    install_script += ' import hashlib\n backup=p.with_name(p.name+".before-"+hashlib.sha256(old.encode()).hexdigest()[:12])\n'
    install_script += ' if not backup.exists(): backup.write_text(old)\n'
    install_script += 'p.write_text(content)\np.chmod(0o644)\nprint("changed" if changed else "same")\n'
    changed = ssh(host, 'sudo -n python3 -', install_script).stdout.strip() == 'changed'
    ssh(host, 'sudo -n systemctl daemon-reload && sudo -n systemctl enable --now ' + WORKER_UNIT)
    if changed:
        ssh(host, 'sudo -n systemctl restart ' + WORKER_UNIT)
    if role != 'node':
        install_io(role)
    print(json.dumps(dict(role=role, generation=plan['generation'], worker_root=worker,
                          unit=WORKER_UNIT, state='installed_waiting_for_assignment')), flush=True)


def install_io(role):
    """Reuse existing HF TLS identities for the separate IO-only worker."""
    host = HOSTS[role]
    base = host['base']
    private = (host['image'].split('/images/')[0] + '/state/private'
               if role != 'data' else '/var/lib/dsc-aave-hf-data/5197dae89b140ccb/state/private')
    python = host['image'] + '/scripts/.venv-abi/bin/python'
    pythonpath = ':'.join((base + '/code-v2', host['overlay'], host['science']))
    unit = f'''[Unit]
Description=Aave cohort authenticated IO worker ({role}, 20261120)
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User={host['user']}
WorkingDirectory={base}
Environment=PYTHONPATH={pythonpath}
Environment=PYTHONDONTWRITEBYTECODE=1
Environment=OPENBLAS_NUM_THREADS=1
Environment=OMP_NUM_THREADS=1
Environment=DASK_DISTRIBUTED__WORKER__MEMORY__TARGET=false
Environment=DASK_DISTRIBUTED__WORKER__MEMORY__SPILL=false
TemporaryFileSystem=/dev/shm:mode=1777,size=256M
ExecStart={python} -m distributed.cli.dask_worker {SCHEDULER} --name cohort-io-{role} --nworkers 1 --nthreads 1 --resources cohort_io=1 --memory-limit 1GiB --host tls://{NETWORK[role]} --no-dashboard --local-directory /dev/shm/cohort-io --tls-ca-file {private}/ca.pem --tls-cert {private}/host-cert.pem --tls-key {private}/host-key.pem
MemoryMax=2G
MemorySwapMax=0
CPUQuota=100%
Nice=10
UMask=0077
Restart=always
RestartSec=30
KillMode=control-group

[Install]
WantedBy=multi-user.target
'''
    script = 'from pathlib import Path\np=Path(' + repr('/etc/systemd/system/' + IO_UNIT) + ')\n'
    script += 'content=' + repr(unit) + '\n'
    script += 'changed=p.exists() and p.read_text()!=content\n'
    script += 'if changed:\n'
    script += ' old=p.read_text()\n'
    script += ' if '+repr('Aave cohort authenticated IO worker (' + role + ', 20261120)')+' not in old: raise RuntimeError("Existing IO unit is outside this cohort deployment")\n'
    script += ' import hashlib\n backup=p.with_name(p.name+".before-"+hashlib.sha256(old.encode()).hexdigest()[:12])\n'
    script += ' if not backup.exists(): backup.write_text(old)\n'
    script += 'p.write_text(content)\np.chmod(0o644)\nprint("changed" if changed else "same")\n'
    changed = ssh(host, 'sudo -n python3 -', script).stdout.strip() == 'changed'
    ssh(host, 'sudo -n systemctl daemon-reload && sudo -n systemctl enable --now ' + IO_UNIT)
    if changed:
        ssh(host, 'sudo -n systemctl restart ' + IO_UNIT)


def setup_storage(host):
    """Expose only the new private project directory through a home bind mount.

    The administrator-owned storage parent remains private. This follows the
    established HF storage layout without changing its permissions.
    """
    script = '''from pathlib import Path
import subprocess
source=Path('/mnt/storage/dsc-aave-hf-cohort-20261120')
target=Path('/home/node/hf-cohort-20261120/storage')
source.mkdir(exist_ok=True)
target.mkdir(exist_ok=True)
subprocess.run(['chown','node:node',str(source),str(target)],check=True)
name=subprocess.check_output(['systemd-escape','--path','--suffix=mount',str(target)],text=True).strip()
unit=Path('/etc/systemd/system')/name
content='[Unit]\\nDescription=Private Aave cohort storage\\nRequiresMountsFor=/mnt/storage\\n\\n[Mount]\\nWhat='+str(source)+'\\nWhere='+str(target)+'\\nType=none\\nOptions=bind\\n\\n[Install]\\nWantedBy=multi-user.target\\n'
if unit.exists() and unit.read_text()!=content:
 raise RuntimeError('Existing cohort mount unit differs')
unit.write_text(content)
subprocess.run(['systemctl','daemon-reload'],check=True)
subprocess.run(['systemctl','enable','--now',name],check=True)
'''
    ssh(host, 'sudo -n python3 -', script)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--role', choices=tuple(HOSTS), required=True)
    parser.add_argument('--plan', required=True)
    args = parser.parse_args()
    install(args.role, args.plan)


if __name__ == '__main__':
    main()
