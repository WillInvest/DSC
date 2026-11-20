"""Deploy isolated Dask HF workers using the existing four-server architecture.

Run on dsc-data after snapshot_hf.py prepare. SSH transports credentials privately;
the image contains input data and dependencies only. Existing services and output
receipts remain untouched, except the authorized HF producer service transition.
"""
import argparse,concurrent.futures,json,os,secrets,shlex,subprocess,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from shared.artifacts import checksum,write_json
BASE=Path('/home/dsc-data');PY=str(BASE/'scripts/.venv-abi/bin/python')
HOSTS={
 'data':dict(ssh='dsc-data@10.246.103.151',ip='10.246.103.151',user='dsc-data',home='/home/dsc-data',workers=4,memory='10G',cpu='400%'),
 'revert':dict(ssh='dsc-revert@10.246.103.26',ip='10.246.103.26',user='dsc-revert',home='/home/dsc-revert',workers=8,memory='20G',cpu='800%'),
 'web':dict(ssh='fao@10.246.103.143',ip='10.246.103.143',user='fao',home='/home/fao',workers=6,memory='16G',cpu='600%'),
 'node':dict(ssh='node@10.246.103.160',ip='10.246.103.160',user='node',home='/home/node',workers=14,memory='40G',cpu='1600%')}

def shell(role,command,**kw):
    args=['bash','-lc',command] if role=='data' else ['ssh','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=10',HOSTS[role]['ssh'],command]
    return subprocess.run(args,check=True,**kw)

def unit(text):
    return text+'\nRestart=always\nRestartSec=30\nKillMode=control-group\nTimeoutStopSec=45\nNice=15\nIOSchedulingClass=best-effort\nIOSchedulingPriority=7\nUMask=0077\n\n[Install]\nWantedBy=multi-user.target\n'

def install(role,name,text):
    path=Path('/tmp')/(name+'.'+str(os.getpid()));path.write_text(text)
    if role=='data':
        shell(role,'sudo install -m 644 '+shlex.quote(str(path))+' /etc/systemd/system/'+name)
    else:
        subprocess.run(['rsync','-a',str(path),HOSTS[role]['ssh']+':/tmp/'+name],check=True)
        shell(role,'sudo install -m 644 /tmp/'+name+' /etc/systemd/system/'+name)
    path.unlink()

def deploy(image,external_hosts=()):
    image=Path(image).resolve();manifest=image/'snapshot-manifest.json';m=json.loads(manifest.read_text());config_root=BASE/'hf-distributed';private=config_root/'private';s=json.loads((private/'security.json').read_text())
    for name,value in [('ca.pem',s['tls_ca_file']),('cert.pem',s['tls_worker_cert']),('key.pem',s['tls_worker_key'])]:
        p=private/name;p.write_text(value);p.chmod(0o600)
    suffix=m['factor_generation'][:16];identity=checksum(manifest)
    for role,h in HOSTS.items():
        h['work']=('/var/lib/dsc-aave-hf-data' if role=='data' else h['home']+'/aave-hf-distributed')+'/'+suffix
        h['image']=str(image) if role=='data' else h['work']+'/images/'+identity
    config=dict(scheduler='tls://10.246.103.151:18876',security=str(private/'security.json'),snapshot_manifest_sha256=checksum(manifest),snapshot_image=str(image),factor_generation=m['factor_generation'],hosts=HOSTS)
    write_json(config_root/'cluster.json',config)
    shell('data','sudo systemctl stop dsc-aave-v3-user-hf.service')
    for role in HOSTS:
        if role not in external_hosts:shell(role,'sudo systemctl stop dsc-aave-v3-hf-workers.service')
    public=config_root/'public';public.mkdir(exist_ok=True)
    extensions=public/'leaf-extensions.cnf';extensions.write_text('basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=clientAuth,serverAuth\n')
    def public_certificate(role):
        h=HOSTS[role];private_dir=h['work']+'/state/private';csr=public/(role+'.csr')
        if role not in external_hosts:
            command='umask 077; if ! test -f '+private_dir+'/host-key.pem; then openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out '+private_dir+'/host-key.pem 2>/dev/null; fi; openssl req -new -key '+private_dir+'/host-key.pem -out '+private_dir+'/host.csr -subj /CN=dsc-hf-'+role+'; cat '+private_dir+'/host.csr'
            response=shell(role,command,capture_output=True,text=True);csr.write_text(response.stdout)
        if not csr.exists():raise ValueError('External host must supply its public CSR: '+str(csr))
        cert=public/(role+'-cert.pem')
        subprocess.run(['openssl','x509','-req','-in',str(csr),'-CA',str(private/'ca.pem'),'-CAkey',str(private/'ca-key.pem'),'-set_serial',str(secrets.randbits(120)+1),'-days','365','-sha256','-extfile',str(extensions),'-out',str(cert)],check=True,capture_output=True)
        if role not in external_hosts:
            for source,name in [(private/'ca.pem','ca.pem'),(cert,'host-cert.pem')]:
                dest=private_dir+'/'+name
                if role=='data':subprocess.run(['install','-m','600',str(source),dest],check=True)
                else:subprocess.run(['rsync','-a',str(source),h['ssh']+':'+dest],check=True)
    def copy(role):
        if role in external_hosts:return
        h=HOSTS[role];work=h['work']
        if role=='data':shell(role,'sudo install -d -m 700 -o dsc-data -g dsc-data /var/lib/dsc-aave-hf-data')
        shell(role,'mkdir -p '+shlex.quote(work)+'/state/private '+shlex.quote(work)+'/outputs; chmod 700 '+shlex.quote(work))
        if role!='data':
            shell(role,'mkdir -p '+shlex.quote(work+'/images'))
            ready=h['image']+'.ready'
            exists=shell(role,'if test -e '+shlex.quote(ready)+'; then echo sealed; fi',capture_output=True,text=True)
            if not exists.stdout.strip():
                subprocess.run(['rsync','-a','--partial','--checksum','--link-dest='+work+'/image',str(image)+'/',h['ssh']+':'+h['image']+'/'],check=True)
                command=shlex.quote(h['image']+'/scripts/.venv-abi/bin/python')+' '+shlex.quote(h['image']+'/scripts/user-history/snapshot_hf.py')+' verify '+shlex.quote(h['image'])
                shell(role,command+' > '+shlex.quote(ready+'.tmp')+' && mv '+shlex.quote(ready+'.tmp')+' '+shlex.quote(ready))
        public_certificate(role)
        print(json.dumps(dict(role=role,stage='image_delivered')),flush=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:list(pool.map(copy,HOSTS))
    for role in external_hosts:public_certificate(role)
    common_env='Environment=PYTHONPATH=/home/dsc-data/scripts/user-history:/home/dsc-data/scripts\nEnvironment=PYTHONDONTWRITEBYTECODE=1\nEnvironment=OMP_NUM_THREADS=1\n'
    tls=' --tls-ca-file {p}/ca.pem --tls-cert {p}/cert.pem --tls-key {p}/key.pem'
    worker_tls=' --tls-ca-file {p}/ca.pem --tls-cert {p}/host-cert.pem --tls-key {p}/host-key.pem'
    scheduler=unit('[Unit]\nDescription=Private authenticated Dask scheduler for Aave HF\nAfter=network-online.target\n[Service]\nUser=dsc-data\nWorkingDirectory=/home/dsc-data/scripts\n'+common_env+'ExecStart='+PY+' -m distributed.cli.dask_scheduler --host tls://10.246.103.151 --port 18876 --no-dashboard'+tls.format(p=private)+'\nMemoryMax=2G')
    install('data','dsc-aave-v3-hf-scheduler.service',scheduler)
    validation=unit('[Unit]\nDescription=Aave HF own-node validation worker\nAfter=network-online.target dsc-aave-v3-hf-scheduler.service\n[Service]\nUser=dsc-data\nWorkingDirectory=/home/dsc-data/scripts\n'+common_env+'ExecStart='+PY+' -m distributed.cli.dask_worker '+config['scheduler']+' --name validation --nworkers 1 --nthreads 1 --resources validator=1 --memory-limit 2GiB --host tls://10.246.103.151 --no-dashboard --local-directory '+str(config_root/'validation-scratch')+tls.format(p=private)+'\nMemoryMax=3G')
    install('data','dsc-aave-v3-hf-validator.service',validation)
    for role,h in HOSTS.items():
        output='/home/dsc-data/data/aave-user-history-v3/'+m['run_id']+'/accounting/hf-numeric'
        worker=unit('[Unit]\nDescription=Aave HF computation on '+role+'\nAfter=network-online.target\n[Service]\nUser='+h['user']+'\nWorkingDirectory=/home/dsc-data/scripts\n'+common_env+'Environment=PATH=/home/dsc-data/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin\nEnvironment=NODE_OPTIONS=--max-old-space-size=256\nEnvironment=DSC_HF_ROLE='+role+'\nBindReadOnlyPaths='+h['image']+':/home/dsc-data\nBindPaths='+h['work']+'/state:/home/dsc-data/worker-state '+h['work']+'/outputs:'+output+'\nExecStartPre='+PY+' /home/dsc-data/scripts/user-history/snapshot_hf.py verify /home/dsc-data\nExecStart='+PY+' -m distributed.cli.dask_worker '+config['scheduler']+' --name '+role+' --nworkers '+str(h['workers'])+' --nthreads 1 --resources compute=1 --memory-limit 1500MiB --host tls://'+h['ip']+' --no-dashboard --local-directory /home/dsc-data/worker-state/dask'+worker_tls.format(p='/home/dsc-data/worker-state/private')+'\nMemoryHigh='+h['memory']+'\nMemoryMax='+h['memory']+'\nMemorySwapMax=0\nCPUQuota='+h['cpu'])
        if role in external_hosts:
            path=config_root/'external-units'/role/'dsc-aave-v3-hf-workers.service';path.parent.mkdir(parents=True,exist_ok=True);path.write_text(worker)
        else:install(role,'dsc-aave-v3-hf-workers.service',worker)
    for role in HOSTS:
        if role not in external_hosts:shell(role,'sudo systemctl daemon-reload')
    shell('data','sudo systemctl enable dsc-aave-v3-hf-scheduler.service dsc-aave-v3-hf-validator.service; sudo systemctl restart dsc-aave-v3-hf-scheduler.service dsc-aave-v3-hf-validator.service')
    for role in HOSTS:
        if role not in external_hosts:shell(role,'sudo systemctl enable dsc-aave-v3-hf-workers.service; sudo systemctl restart dsc-aave-v3-hf-workers.service')
    print(json.dumps(dict(stage='workers_started',hosts={k:v['workers'] for k,v in HOSTS.items()},config=str(config_root/'cluster.json'))))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--image',required=True);p.add_argument('--external-host',action='append',choices=list(HOSTS),default=[]);a=p.parse_args();deploy(a.image,a.external_host)
