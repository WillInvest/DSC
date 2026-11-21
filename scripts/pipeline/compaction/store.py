"""Publish verified interval files before transactionally retiring source fragments."""
from dataclasses import dataclass
import fcntl
import json
import os
import stat
from pathlib import Path
import sqlite3
import time
from shared.artifacts import checksum,write_json


@dataclass
class Fragment:
    path: Path
    start: int
    stop: int
    retire: bool = False
    expected_sha256: str | None = None


class Store:
    def __init__(self,root,heartbeat=lambda:None):
        self.root=Path(root);self.root.mkdir(parents=True,exist_ok=True)
        self.heartbeat=heartbeat
        self.lock=(self.root/'catalog.lock').open('a')
        fcntl.flock(self.lock,fcntl.LOCK_EX)
        self.db=sqlite3.connect(self.root/'catalog.sqlite');self.db.row_factory=sqlite3.Row
        self.db.execute('pragma journal_mode=WAL');self.db.execute('pragma synchronous=FULL')
        self.db.executescript('''
          create table if not exists meta(key text primary key,value text);
          create table if not exists streams(stream text primary key,spec text);
          create table if not exists files(stream text,start integer,stop integer,path text,sha256 text,
            rows integer,sealed integer,primary key(stream,start));
          create table if not exists sources(path text primary key,sha256 text,start integer,stop integer,retire integer,identity text);
          create table if not exists retirements(path text primary key,sha256 text,proof text,state text,identity text);
          create table if not exists batches(through integer primary key,details text,committed real);
          create index if not exists retirement_status on retirements(state);
          create index if not exists open_intervals on files(stream,sealed,start);
        ''')
        for table in ('sources','retirements'):
            if 'identity' not in {r[1] for r in self.db.execute('pragma table_info('+table+')')}:
                self.db.execute('alter table '+table+' add column identity text')
        self._metrics=None

    def __enter__(self):return self
    def __exit__(self,*args):self.db.close();self.lock.close()

    @property
    def head(self):
        row=self.db.execute("select value from meta where key='through'").fetchone()
        return int(row[0]) if row else None

    def files(self,stream=None):
        sql='select * from files'+(' where stream=?' if stream is not None else '')+' order by stream,start'
        return [dict(r) for r in self.db.execute(sql,(stream,) if stream is not None else ())]

    def status(self,state='complete',**extra):
        if self._metrics is None:
            rows=self.db.execute('select count(*),coalesce(sum(sealed),0),coalesce(sum(rows),0) from files').fetchone()
            retired=self.db.execute("select count(*) from retirements where state='retired'").fetchone()[0]
            pending=self.db.execute("select count(*) from retirements where state='pending'").fetchone()[0]
            self._metrics=[*rows,retired,pending]
        rows=self._metrics[:3];retired,pending=self._metrics[3:]
        # In-flight progress is observational. The finalized report and
        # catalog commit still use their durable paths.
        write_json(self.root/'progress.json',dict(state=state,through=self.head,files=rows[0],sealed_files=rows[1],
                   open_files=rows[0]-rows[1],rows=rows[2],retired_files=retired,pending_retirements=pending,
                   updated_at=time.time(),**extra),durable=state!='compacting')

    @staticmethod
    def identity(path):
        s=Path(path).lstat()
        if not stat.S_ISREG(s.st_mode):raise ValueError('Compaction sources must be regular files')
        return dict(device=s.st_dev,inode=s.st_ino,bytes=s.st_size,mtime_ns=s.st_mtime_ns,
                    links=s.st_nlink,uid=s.st_uid,gid=s.st_gid,mode=s.st_mode)

    def cleanup(self):
        verified=set()
        retired=0
        for row in self.db.execute("select * from retirements where state='pending'").fetchall():
            for proof in json.loads(row['proof']):
                target=self.root/proof['path']
                key=(str(target),proof['sha256'])
                if key not in verified:
                    if checksum(target)!=proof['sha256']:raise ValueError(f'Canonical retirement proof changed: {target}')
                    verified.add(key)
            path=Path(row['path'])
            if path.exists():
                if not row['identity'] or self.identity(path)!=json.loads(row['identity']):
                    raise ValueError(f'Retirement source identity changed: {path}')
                if checksum(path)!=row['sha256']:raise ValueError(f'Retirement source changed: {path}')
                path.unlink()
                descriptor=os.open(path.parent,os.O_RDONLY)
                try:os.fsync(descriptor)
                finally:os.close(descriptor)
            with self.db:self.db.execute("update retirements set state='retired' where path=?",(row['path'],))
            if self._metrics is not None:self._metrics[3]+=1;self._metrics[4]-=1
            retired+=1
            if retired%100==0:self.status('retiring')
            self.heartbeat()

    def compact(self,through,specs,details=None):
        self.cleanup()
        if self.head is not None and through<=self.head:
            self.status();return
        self.status('compacting',target=through)
        proposed=[];proofs={};retired={};stream_specs={}
        all_specs={r['stream']:{**json.loads(r['spec']),'fragments':[]} for r in self.db.execute('select * from streams')}
        for key,spec in specs.items():
            if key.startswith('/') or '..' in Path(key).parts:raise ValueError('Invalid stream key')
            saved=all_specs.get(key)
            persistent={k:v for k,v in spec.items() if k!='fragments'}
            if saved and {k:v for k,v in saved.items() if k!='fragments'}!=persistent:
                raise ValueError(f'Stream identity changed: {key}')
            all_specs[key]=spec
        for key,spec in all_specs.items():
            self.heartbeat()
            stream_specs[key]={k:v for k,v in spec.items() if k!='fragments'}
            fragments=spec.get('fragments',[])
            active=[dict(f) for f in self.db.execute('select * from files where stream=? and sealed=0 order by start',(key,))]
            if not fragments and not active:continue
            for f in fragments:
                f.path=Path(f.path)
                if f.retire and f.path.resolve().is_relative_to(self.root.resolve()):
                    raise ValueError('Input fragments must be outside the canonical store')
                if not f.start<f.stop or f.stop>through+1:raise ValueError('Fragment outside accepted interval')
                sha=checksum(f.path)
                if f.expected_sha256 and sha!=f.expected_sha256:raise ValueError(f'Source checksum mismatch: {f.path}')
                proofs[str(f.path)]=dict(path=str(f.path),sha256=sha,start=f.start,stop=f.stop,retire=int(f.retire),identity=json.dumps(self.identity(f.path)))
            combined=[Fragment(self.root/f['path'],f['start'],f['stop'],expected_sha256=f['sha256']) for f in active]+fragments
            for f in active:
                path=self.root/f['path']
                if checksum(path)!=f['sha256']:raise ValueError('Canonical source changed')
                proofs[str(path)]=dict(path=str(path),sha256=f['sha256'],start=f['start'],stop=f['stop'],retire=0,identity=json.dumps(self.identity(path)))
            if not combined:continue
            anchor,size=spec['anchor'],spec['size']
            buckets=set()
            for f in combined:
                buckets.update(range(anchor+(f.start-anchor)//size*size,f.stop,size))
            for lower in sorted(buckets):
                upper=min(lower+size,through+1)
                inputs=sorted([f for f in combined if f.start<upper and f.stop>lower],key=lambda f:f.start)
                if not inputs:continue
                if any(a.stop>b.start for a,b in zip(inputs,inputs[1:])):raise ValueError('Overlapping source coverage')
                previous=self.db.execute('select * from files where stream=? and start=?',(key,lower)).fetchone()
                if previous and previous['sealed']:raise ValueError('Accepted data overlaps a sealed interval')
                extension='parquet' if spec['format']=='parquet' else 'jsonl.gz'
                relative=Path(key)/f'{lower}-{upper-1}.{extension}'
                target=self.root/relative;target.parent.mkdir(parents=True,exist_ok=True)
                temp=target.with_name(target.name+'.pending')
                if spec['format']=='parquet':
                    from pipeline.compaction.parquet import combine
                elif spec['format']=='jsonl':
                    from pipeline.compaction.jsonl import combine
                else:raise ValueError('Unsupported storage format')
                rows=combine(inputs,temp,lower,upper,spec.get('dense',False))
                with temp.open('rb') as stream:os.fsync(stream.fileno())
                try:os.link(temp,target)
                except FileExistsError:
                    if checksum(target)!=checksum(temp):raise ValueError(f'Canonical target conflict: {target}')
                    with target.open('rb') as stream:os.fsync(stream.fileno())
                temp.unlink()
                descriptor=os.open(target.parent,os.O_RDONLY)
                try:os.fsync(descriptor)
                finally:os.close(descriptor)
                entry=dict(stream=key,start=lower,stop=upper,path=str(relative),sha256=checksum(target),rows=rows,
                           sealed=int(upper==lower+size))
                proposed.append(entry)
                if previous and previous['path']!=str(relative):
                    retired[str(self.root/previous['path'])]=dict(sha256=previous['sha256'],proof=[entry],identity=proofs[str(self.root/previous['path'])]['identity'])
                self.status('compacting',target=through,current_stream=key,current_start=lower,written_files=len(proposed))
            targets=[p for p in proposed if p['stream']==key]
            for f in fragments:
                if f.retire:
                    retired[str(f.path)]=dict(sha256=proofs[str(f.path)]['sha256'],
                                             proof=[p for p in targets if p['start']<f.stop and p['stop']>f.start],identity=proofs[str(f.path)]['identity'])
        for p in proofs.values():
            if self.identity(p['path'])!=json.loads(p['identity']) or checksum(p['path'])!=p['sha256']:
                raise ValueError('Source changed during compaction')
        with self.db:
            if details and details.get('layout'):
                layout=json.dumps(details['layout'],sort_keys=True)
                existing=self.db.execute("select value from meta where key='layout'").fetchone()
                if existing and existing[0]!=layout:raise ValueError('Canonical layout identity changed')
                self.db.execute("insert or ignore into meta values('layout',?)",(layout,))
            for key,spec in stream_specs.items():
                self.db.execute('insert or ignore into streams values(?,?)',(key,json.dumps(spec,sort_keys=True)))
            for p in proposed:
                self.db.execute('insert or replace into files values(:stream,:start,:stop,:path,:sha256,:rows,:sealed)',p)
            for p in proofs.values():
                self.db.execute('insert or ignore into sources(path,sha256,start,stop,retire,identity) values(:path,:sha256,:start,:stop,:retire,:identity)',p)
            for path,p in retired.items():
                if not p['proof']:raise ValueError('Cannot retire a source without replacement evidence')
                self.db.execute('insert or replace into retirements(path,sha256,proof,state,identity) values(?,?,?,?,?)',
                                (path,p['sha256'],json.dumps(p['proof']), 'pending',p['identity']))
            self.db.execute("insert or replace into meta values('through',?)",(str(through),))
            self.db.execute('insert into batches values(?,?,?)',(through,json.dumps({k:v for k,v in (details or {}).items() if k!='layout'}),time.time()))
        self._metrics=None
        self.status('retiring',target=through);self.cleanup();self.status()
