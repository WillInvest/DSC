"""Locate accepted inputs without scanning or rewriting the full historical backfill."""
import hashlib
import json
from pathlib import Path
import re
from pipeline.compaction.store import Fragment

MODE='event-derived-unchecked-v1'
RECEIPT_ANCHOR=10008355


def read(path):return json.loads(Path(path).read_text())


def tail_start(anchor,end,size):return anchor+(end+1-anchor)//size*size


def bounds(path):
    m=re.fullmatch(r'ethereum__logs__(\d+)_to_(\d+)\.parquet',path.name) or re.fullmatch(r'(\d+)-(\d+)\.(?:parquet|jsonl.gz)',path.name)
    if not m:raise ValueError(f'Unrecognized interval filename: {path}')
    return int(m[1]),int(m[2])+1


class Collector:
    def __init__(self,request,role,head):
        self.request=request;self.role=role;self.head=head;self.specs={}
        self.baseline=request['baseline'];self.origin={j['address']:j for j in self.baseline['jobs']}
        self.baseline_end=self.baseline['end_block'];self.through=self.baseline_end
        self.cycles=[]
        expected=(head if head is not None else self.baseline_end)+1
        for cycle in request['cycles']:
            if cycle['end']<expected:continue
            if cycle['start']!=expected:raise ValueError('Completed cycle coverage gap or overlap')
            self.cycles.append(cycle);expected=cycle['end']+1
        if self.cycles:self.through=self.cycles[-1]['end']
        elif head is not None:self.through=head

    def add(self,key,spec,fragment):
        old=self.specs.setdefault(key,{**spec,'fragments':[]})
        if {k:v for k,v in old.items() if k!='fragments'}!=spec:raise ValueError('Source schema or configuration changed')
        if fragment is not None:old['fragments'].append(fragment)

    def parquet(self,path,kind,job,scope=None,retire=False,abi=None,label=None,sha=None):
        import pyarrow.parquet as pq
        path=Path(path);start,stop=scope or bounds(path)
        schema=pq.read_schema(path);schema_hash=hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()
        origin=self.origin[job['address']]
        metadata=dict(kind=kind,protocol=job['protocol'],address=job['address'],name=origin['name'],
                      arrow_schema_sha256=schema_hash,historical_before=tail_start(origin['start_block'],self.baseline_end,10000))
        key=f"{kind}/{job['protocol']}/{job['address']}"
        if kind=='decoded':
            metadata.update(abi_sha256=abi,schema_version=label,event=path.stem)
            key+=f'/{label}/{abi}/{path.stem}'
        if kind=='state':
            metadata.update(mode=MODE,validation='none');key+='/'+MODE
        key+='/'+schema_hash[:16]
        spec=dict(format='parquet',anchor=origin['start_block'],size=10000,dense=kind=='state',metadata=metadata)
        self.add(key,spec,Fragment(path,start,stop,retire,sha))

    def pool(self,job,seed=False):
        import pyarrow.parquet as pq
        origin=self.origin[job['address']]
        if self.role=='node':
            encoded=Path(job['node_dir'])/'encoded';manifest=read(Path(job['node_dir'])/'manifest.json')
        else:
            encoded=Path(job['destination'])
            receipts=Path(self.request['baseline_events_root'])/'receipts' if seed else Path(self.request['pipeline_root'])/'cycles'/self.current_cycle/'events/receipts'
            manifest=read(receipts/f"{job['address']}.json")
        lower=tail_start(origin['start_block'],self.baseline_end,10000) if seed else job['start_block']
        stop=self.baseline_end+1 if seed else self.current_end+1
        if lower>=stop:return
        files=[f'ethereum__logs__{lower:08d}_to_{stop-1:08d}.parquet'] if seed else list(manifest['files'])+list(manifest['empty_chunks'])
        for name in files:
            path=encoded/name
            if name in manifest['files']:
                info=manifest['files'][name]
                if path.stat().st_size!=info['bytes'] or pq.read_metadata(path).num_rows!=info['rows']:
                    raise ValueError(f'Encoded input no longer matches acceptance receipt: {path}')
                self.parquet(path,'encoded',job,retire=not seed)
            elif name not in manifest['empty_chunks']:raise ValueError('Unaccounted encoded interval')
            if self.role=='node':continue
            decoded=encoded.parent.parent/'decoded'/encoded.name/Path(name).stem
            receipt=decoded/'receipt.json'
            if receipt.exists():
                for table in read(receipt)['tables']:
                    config=table['config'];selected=config.get('selected_profile')
                    abi=selected['abi_sha256'] if selected else config['abi_sha256'][0]
                    directory=decoded/table['directory'];metadata=read(directory/'manifest.json')
                    for item in metadata['outputs']:
                        self.parquet(directory/item['file'],'decoded',job,scope=bounds(path),retire=not seed,
                                     abi=abi,label=table['directory'],sha=item['sha256'])
            elif name in manifest['files'] and manifest['files'][name]['rows']:
                raise ValueError(f'Missing decoded acceptance receipt: {receipt}')
        if self.role=='data':
            state=encoded.parent.parent/'reconstructed'/encoded.name/MODE
            expected=lower
            paths=[state/f'{lower}-{stop-1}.parquet'] if seed else sorted(state.glob('*.parquet'))
            for path in paths:
                a,z=bounds(path)
                if z<=lower or a>=stop:continue
                if a!=expected:raise ValueError('State source interval gap or overlap')
                self.parquet(path,'state',job,retire=not seed);expected=z
            if expected!=stop:raise ValueError('State source coverage incomplete')

    def json_archive(self,folder,key,seed=False,metadata=None,hash_field='sha256'):
        folder=Path(folder)
        if seed:
            start=tail_start(RECEIPT_ANCHOR,self.baseline_end,1000)
            if start>self.baseline_end:return
            files=[folder/f'{start}-{self.baseline_end}.jsonl.gz']
        else:files=sorted(folder.glob('*.jsonl.gz'))
        cursor=None
        for path in files:
            a,z=bounds(path)
            if cursor is not None and a!=cursor:raise ValueError('Archive source files are noncontiguous')
            cursor=z;info=read(Path(metadata)/(path.name.split('.')[0]+'.json')) if metadata else {}
            identity=dict(chain_id=1,kind=key,historical_before=tail_start(RECEIPT_ANCHOR,self.baseline_end,1000))
            if 'selected' in key:
                identity['selection']=info['selection']
                stream=key+'/'+info['selection']
            else:stream=key
            spec=dict(format='jsonl',anchor=RECEIPT_ANCHOR,size=1000,dense=True,metadata=identity)
            self.add(stream,spec,Fragment(path,a,z,not seed,info.get(hash_field)))
        if not seed and (not files or bounds(files[0])[0]!=self.current_start or cursor!=self.current_end+1):
            raise ValueError('Archive does not cover the completed cycle')

    def collect(self):
        if self.head is None:
            if self.role in ('node','data'):
                for job in self.baseline['jobs']:self.pool(job,True)
            self.archives(True)
        for cycle in self.cycles:
            self.current_cycle=cycle['id'];self.current_start=cycle['start'];self.current_end=cycle['end']
            if {j['address'] for j in cycle['jobs']}!=set(self.origin):raise ValueError('Tracked address list changed')
            if self.role in ('node','data'):
                for job in cycle['jobs']:self.pool(job)
            self.archives(False)
        return self.specs

    def archives(self,seed):
        if self.role=='node':
            root=Path('/home/node/dsc-reverted/receipts/historical-v1') if seed else Path('/home/node/dsc-extraction/cycles')/self.current_cycle/'receipts'
            self.json_archive(root/'chunks','receipts',seed,root/'metadata')
        elif self.role=='data':
            root=Path('/home/dsc-data/data/reverted/drpc-v1') if seed else Path('/home/dsc-data/data/incremental')/self.current_cycle/'reverted'
            self.json_archive(root/'chunks','selected',seed,root/'metadata')
        elif self.role=='revert':
            root=Path('/home/dsc-revert/data/receipts/historical-v1') if seed else Path('/home/dsc-revert/data/cycles')/self.current_cycle/'receipts'
            self.json_archive(root/'chunks','receipts',seed,root/'metadata')
            trace=Path('/home/dsc-revert/data/archive/reverted/drpc-v1') if seed else Path('/home/dsc-revert/data/cycles')/self.current_cycle/'traces'
            self.json_archive(trace/'archive','traces',seed,trace/'selected/metadata','archive_sha256')
            self.json_archive(trace/'selected/chunks','selected',seed,trace/'selected/metadata')
        else:raise ValueError('Unknown server role')
