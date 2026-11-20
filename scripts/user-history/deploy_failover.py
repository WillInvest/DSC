"""Install only new independent-host HF units; legacy services stay unchanged."""
import argparse,subprocess
from pathlib import Path
from deploy_hf_cluster import HOSTS

IMAGE_SHA='1d1a9be72b345989a4a8667573e929c66320535824f480517e6e440633d0e32d'

def main():
 p=argparse.ArgumentParser();p.add_argument('role',choices=['node','revert','web']);p.add_argument('--start',action='store_true');a=p.parse_args();h=HOSTS[a.role];base=Path(h['home'])/'hf-failover-20261115';old=Path(h['home'])/'aave-hf-distributed/5197dae89b140ccb';code=base/'code';python='/home/dsc-data/scripts/.venv-abi/bin/python'
 common='[Unit]\nAfter=network-online.target\nWants=network-online.target\nStartLimitIntervalSec=0\n[Service]\nUser='+h['user']+'\nWorkingDirectory=/home/dsc-data/scripts\nBindReadOnlyPaths='+str(old/'images'/IMAGE_SHA)+':/home/dsc-data '+str(old/'state')+':/home/dsc-data/worker-state\nEnvironment=PYTHONPATH='+str(code)+':/home/dsc-data/scripts/user-history:/home/dsc-data/scripts\nEnvironment=PATH=/home/dsc-data/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin\nEnvironment=PYTHONDONTWRITEBYTECODE=1\nEnvironment=OMP_NUM_THREADS=1\nEnvironment=MALLOC_TRIM_THRESHOLD_=0\nEnvironment=MALLOC_ARENA_MAX=2\nUMask=0077\nNice=15\nKillMode=control-group\nTimeoutStopSec=30\n'
 memory={'node':('20G','24G'),'revert':('8G','10G'),'web':('6G','8G')}[a.role]
 worker=common+'TemporaryFileSystem=/dev/shm:mode=1777,size=64M\nBindPaths='+str(old/'outputs')+':/home/dsc-data/data/aave-user-history-v3/26407881/accounting/hf-numeric\nExecStart='+python+' '+str(code/'failover_hf.py')+' run --manifest '+str(base/'plan.json')+' --state '+str(base/'state')+' --role '+a.role+' --workers '+str(h['workers'])+' --public '+str(base/'public')+'\nMemoryHigh='+memory[0]+'\nMemoryMax='+memory[1]+'\nCPUQuota='+h['cpu']+'\nRestart=on-abnormal\nRestartSec=30\n[Install]\nWantedBy=multi-user.target\n'
 status=common+'ExecStart='+python+' '+str(code/'failover_status.py')+' --file '+str(base/'public/status.json')+' --bind '+h['ip']+' --port 18879\nMemoryMax=256M\nCPUQuota=50%\nRestart=on-failure\nRestartSec=15\n[Install]\nWantedBy=multi-user.target\n'
 for name,text in [('dsc-aave-v3-hf-independent.service',worker),('dsc-aave-v3-hf-independent-status.service',status)]:
  local=base/(name+'.proposed');local.write_text(text);subprocess.run(['sudo','install','-m','644',str(local),'/etc/systemd/system/'+name],check=True)
 subprocess.run(['sudo','systemctl','daemon-reload'],check=True)
 if a.start:subprocess.run(['sudo','systemctl','enable','--now','dsc-aave-v3-hf-independent.service','dsc-aave-v3-hf-independent-status.service'],check=True)

if __name__=='__main__':main()
