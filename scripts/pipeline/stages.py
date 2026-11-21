"""Adapt the accepted fast workers to one immutable incremental interval."""
import json
from pathlib import Path
import shutil
from pipeline.runtime import (NODE,REVERT,SCRIPTS,PYTHON,NODE_SCRIPTS,NODE_PYTHON,
                              REVERT_SCRIPTS,REVERT_PYTHON,ssh,remote_json,run,transfer,rpc)
from shared.artifacts import checksum,write_json
from automation.common import paths

MODE='event-derived-unchecked-v1'


def read(path):return json.loads(Path(path).read_text())


def state_path(job):
    if job.get('checkpoint'):return Path(job['checkpoint'])
    return paths(job)[2]/MODE/'progress.json'


def validate_previous(plan, next_block):
    jobs=plan['jobs']
    if len(jobs)!=102 or len({j['address'] for j in jobs})!=102:raise ValueError('Expected the accepted 102-address registry')
    for job in jobs:
        p=state_path(job);saved=read(p)
        if (saved['state']!='complete' or saved['next_block']!=next_block or
            saved['identity']['address']!=job['address'] or saved['identity']['mode']!=MODE):
            raise ValueError(f'Incomplete predecessor state: {p}')


class Stages:
    def __init__(self, root, cycle, previous_plan, previous_profiles, heartbeat):
        self.cycle=cycle;self.root=Path(root)/'cycles'/cycle['id'];self.root.mkdir(parents=True,exist_ok=True)
        self.previous_plan=Path(previous_plan);self.previous_profiles=Path(previous_profiles);self.heartbeat=heartbeat
        self.node=f"/home/node/dsc-extraction/cycles/{cycle['id']}"
        self.revert=f"/home/dsc-revert/data/cycles/{cycle['id']}"
        self.data=Path('/home/dsc-data/data/incremental')/cycle['id']
        self.events=self.root/'events';self.auto=self.root/'offline';self.profiles=self.root/'profiles'
        self.log=self.root/'workers.log'
        self.env=dict(DSC_EVENT_ROOT=str(self.events),DSC_AUTO_ROOT=str(self.auto),DSC_PROFILES=str(self.profiles))

    def execute(self, command, env=None, progress=None):run(command,self.log,self.heartbeat,env,progress)
    def copy(self, source, destination):transfer(source,destination,self.log,self.heartbeat)

    def prepare(self):
        planfile=self.events/'plan.json'
        if planfile.exists():return read(planfile)
        previous=read(self.previous_plan);validate_previous(previous,self.cycle['start'])
        jobs=[]
        for old in previous['jobs']:
            source=Path(old['destination']);category,protocol,version=source.parts[-5:-2]
            destination=self.data/category/protocol/version/'encoded'/source.name
            for kind in ('encoded','decoded','reconstructed'):
                folder=destination.parent.parent/kind/destination.name;folder.mkdir(parents=True,exist_ok=True)
                if (source/'metadata.json').exists():shutil.copyfile(source/'metadata.json',folder/'metadata.json')
            item={**old,'start_block':self.cycle['start'],'destination':str(destination),
                  'node_dir':self.node+'/events/'+old['address'], 'previous_state':str(state_path(old))}
            item.pop('checkpoint',None);jobs.append(item)
        plan={**previous,'jobs':jobs,'end_block':self.cycle['end'],'end_hash':self.cycle['hash'],
              'node_plan':self.node+'/plan.json','poll_seconds':1}
        write_json(planfile,plan)
        return plan

    def profiles_stage(self):
        self.execute(ssh(NODE,['mkdir','-p',self.node+'/previous-profiles']))
        self.copy(str(self.previous_profiles)+'/',NODE+':'+self.node+'/previous-profiles/')
        self.copy('/home/dsc-data/contracts/ethereum/pools.csv',NODE+':'+self.node+'/pools.csv')
        self.execute(ssh(NODE,[NODE_PYTHON,NODE_SCRIPTS+'/pipeline/profiles.py',
            '--source',self.node+'/previous-profiles','--output',self.node+'/profiles',
            '--start',self.cycle['start'],'--stop',self.cycle['end']+1,
            '--contracts',self.node+'/pools.csv'],True))
        self.profiles.mkdir(exist_ok=True)
        self.copy(NODE+':'+self.node+'/profiles/',str(self.profiles)+'/')
        for v in ('v2','v3'):
            relative=f'protocols/lending/aave/{v}/history/'
            self.copy(NODE+':'+NODE_SCRIPTS+'/'+relative,str(SCRIPTS/relative)+'/')
        accepted=read(self.profiles/'accepted.json')
        for n,h in accepted['files'].items():
            if checksum(self.profiles/n)!=h:raise ValueError('Profile transfer checksum mismatch')
        return dict(receipt=str(self.profiles/'accepted.json'))

    def events_stage(self):
        plan=self.prepare()
        self.copy(str(self.events/'plan.json'),NODE+':'+self.node+'/plan.json')
        def event_progress():
            p=read(self.events/'progress.json');current=p.get('current') or {}
            return (p['complete_addresses'],current.get('address'),current.get('state'),current.get('written_chunks'))
        self.execute([PYTHON,SCRIPTS/'event-cryo-extraction/run.py','run','--root',self.events],progress=event_progress)
        jobs=read(self.events/'jobs.json')
        if any(jobs.get(j['address'],{}).get('state')!='complete' for j in plan['jobs']):raise ValueError('Event extraction/transfer incomplete')
        return dict(receipts=str(self.events/'receipts'),addresses=len(jobs))

    def offline(self, phase):
        plan=self.prepare();states=read(self.auto/'jobs.json') if (self.auto/'jobs.json').exists() else {}
        for job in plan['jobs']:
            address=job['address'];encoded,decoded,reconstructed=paths(job)
            write_json(self.auto/'progress.json',dict(state=phase,current=job))
            manifest=read(self.events/'receipts'/f'{address}.json')
            if phase=='decode':
                command=[PYTHON,SCRIPTS/'decoding/decode_event_files.py','--protocol',job['protocol'],
                         '--input',encoded,'--output',decoded,'--workers','32']
                if job['protocol'].startswith('aave'):command+=['--profiles',self.profiles]
                if manifest['files']:
                    if not states.get(address,{}).get('decoded'):
                        self.execute(command,self.env,lambda:(decoded/'decode-progress.json').stat().st_mtime_ns)
                    progress=read(decoded/'decode-progress.json')
                    if progress['state']!='complete' or progress['decoded_rows']!=sum(x['rows'] for x in manifest['files'].values()):raise ValueError('Decoded row acceptance failed')
                else:
                    write_json(decoded/'decode-progress.json',dict(state='complete',decoded_rows=0,total_rows=0,completed_files=0,total_files=0))
                states.setdefault(address,{})['decoded']=True
                states[address]['extraction_receipt']=checksum(self.events/'receipts'/f'{address}.json')
            else:
                while True:
                    self.execute([PYTHON,SCRIPTS/'automation/reconstruct.py',address],self.env,
                                 lambda:(reconstructed/'reconstruction-progress.json').stat().st_mtime_ns)
                    progress=read(reconstructed/'reconstruction-progress.json')
                    if progress['state']=='complete':break
                checkpoint=read(state_path(job))
                if checkpoint['next_block']!=self.cycle['end']+1:raise ValueError('Reconstruction coverage mismatch')
                states.setdefault(address,{})['reconstructed']=True
            write_json(self.auto/'jobs.json',states)
            write_json(self.auto/'progress.json',dict(state=phase,current=job))
        write_json(self.auto/'progress.json',dict(state='complete',current=None))
        return dict(jobs=str(self.auto/'jobs.json'),addresses=len(states))

    def receipts_stage(self):
        def progress():
            p=remote_json(NODE,self.node+'/receipts/progress.json')
            return (p.get('completed_blocks'),p.get('inflight_completed'))
        self.execute(ssh(NODE,[NODE_PYTHON,NODE_SCRIPTS+'/reverted/scan_receipts.py',
            '--start',self.cycle['start'],'--stop',self.cycle['end']+1,'--end-hash',self.cycle['hash'],
            '--workers','32','--executor','process','--output',self.node+'/receipts'],True),progress=progress)
        progress=remote_json(NODE,self.node+'/receipts/progress.json');self.check_blocks(progress,'completed_blocks')
        return progress

    def receipt_delivery_stage(self):
        self.execute(ssh(REVERT,[REVERT_PYTHON,REVERT_SCRIPTS+'/reverted/receive_receipts.py',
            '--root',self.revert+'/receipts','--source',NODE+':'+self.node+'/receipts/'],True))
        progress=remote_json(REVERT,self.revert+'/receipts/progress.json');self.check_blocks(progress,'completed_blocks')
        return progress

    def traces_stage(self):
        def progress():
            p=remote_json(REVERT,self.revert+'/traces/progress.json')
            return (p.get('completed_blocks'),p.get('archived_blocks'))
        self.execute(ssh(REVERT,[REVERT_PYTHON,REVERT_SCRIPTS+'/reverted/trace_received.py',
            '--receipts',self.revert+'/receipts','--output',self.revert+'/traces',
            '--reth-rpc','http://10.246.103.160:8545','--reth-workers','32'],True),progress=progress)
        progress=remote_json(REVERT,self.revert+'/traces/progress.json');self.check_blocks(progress,'archived_blocks')
        return {k:v for k,v in progress.items() if k not in ('accounts',)}

    def selected_delivery_stage(self):
        target=self.data/'reverted'
        self.execute([PYTHON,SCRIPTS/'reverted/deliver_selected.py','--root',target,
                      '--source',REVERT+':'+self.revert+'/traces/selected/'])
        progress=read(target/'progress.json');self.check_blocks(progress,'accepted_blocks')
        accepted=read(target/'accepted.json');cursor=self.cycle['start']
        for item in sorted(accepted.values(),key=lambda x:x['start']):
            if item['start']!=cursor:raise ValueError('Selected coverage gap or overlap')
            cursor=item['stop']
        if cursor!=self.cycle['end']+1:raise ValueError('Selected coverage ends early')
        if rpc('eth_getBlockByNumber',[hex(self.cycle['end']),False])['hash']!=self.cycle['hash']:
            raise ValueError('Cycle target hash changed')
        return dict(progress=progress,accepted=str(target/'accepted.json'))

    def check_blocks(self, progress, key):
        if progress['state']!='complete' or progress[key]!=self.cycle['end']-self.cycle['start']+1:
            raise ValueError(f'Incomplete stage: {key}')

    def decode_stage(self):
        return self.offline('decode')

    def reconstruct_stage(self):
        return self.offline('reconstruct')
