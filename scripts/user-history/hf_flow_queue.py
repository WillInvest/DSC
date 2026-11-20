"""Durable local completion and Data-owned transfer stages; no numerical replay."""
import contextlib
import json
import sqlite3
import time
from pathlib import Path


def assignments(plan, accepted):
    roles = plan['workers']
    result = {r: [] for r in roles}
    load = {r: 0 for r in roles}
    for job in plan['jobs']:
        if job['id'] in accepted:
            continue
        role = plan['source_roles'].get(job['id'])
        if role is None:
            role = min(roles, key=lambda r: (load[r] / roles[r], r))
        if role not in roles:
            raise ValueError('Unknown frozen source role')
        result[role].append(job['id'])
        load[role] += max(job['expected_rows'], 1)
    return result


class Ledger:
    def __init__(self, path, generation):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path)
        self.db.execute('pragma journal_mode=WAL')
        self.db.execute('pragma synchronous=FULL')
        self.db.execute('create table if not exists identity(generation text primary key)')
        rows = self.db.execute('select generation from identity').fetchall()
        if rows and rows != [(generation,)]:
            self.db.close()
            raise ValueError('Ledger numerical identity changed')
        self.db.execute('insert or ignore into identity values(?)', (generation,))
        self.db.commit()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.db.close()

    @contextlib.contextmanager
    def transaction(self):
        with self.db:
            yield


class LocalLedger(Ledger):
    def __init__(self, path, generation):
        super().__init__(path, generation)
        self.db.execute('create table if not exists done(job_id text primary key,sha256 text not null,bytes integer not null,value text not null,completed_at real not null)')
        self.db.execute('create table if not exists retired(job_id text primary key,proof_sha256 text not null,retired_at real not null)')
        self.db.commit()

    def commit(self, key, value):
        old = self.db.execute('select sha256 from done where job_id=?', (key,)).fetchone()
        if old and old[0] != value['sha256']:
            raise ValueError('Local immutable completion conflicts')
        with self.transaction():
            self.db.execute('insert or ignore into done values(?,?,?,?,?)',
                            (key, value['sha256'], value['receipt']['bytes'], json.dumps(value, sort_keys=True), time.time()))

    def completed(self):
        return {r[0] for r in self.db.execute('select job_id from done')}

    def entries(self, after=0, limit=128):
        return [dict(job_id=k, bytes=b, sequence=n, value=json.loads(v))
                for n, k, b, v in self.db.execute('select rowid,job_id,bytes,value from done where rowid>? order by rowid limit ?', (after, limit))]

    def mark_retired(self, keys, proof_sha256):
        with self.transaction():
            for key in keys:
                self.db.execute('insert or ignore into retired values(?,?,?)', (key, proof_sha256, time.time()))

    def buffered_bytes(self):
        return self.db.execute('select coalesce(sum(bytes),0) from done where job_id not in (select job_id from retired)').fetchone()[0]


class FlowLedger(Ledger):
    def __init__(self, path, generation):
        super().__init__(path, generation)
        self.db.execute('create table if not exists jobs(job_id text primary key,role text not null,local_value text,node_value text,cloud_batch text,observed_at real not null)')
        self.db.execute('create table if not exists node_acceptance(sequence integer primary key autoincrement,job_id text not null unique)')
        self.db.execute('insert or ignore into node_acceptance(job_id) select job_id from jobs where node_value is not null order by rowid')
        self.db.execute('create table if not exists cloud(batch_id text primary key,report text not null)')
        self.db.execute('create table if not exists transfers(batch_id text primary key,report text not null)')
        self.db.execute('create table if not exists cursors(role text primary key,sequence integer not null)')
        self.db.execute('create table if not exists operations(id text primary key,kind text,role text,payload text,state text,report text,created_at real)')
        self.db.execute('create table if not exists metrics(job_id text primary key,bytes integer,rows integer,start integer,stop integer)')
        columns={x[1] for x in self.db.execute('pragma table_info(metrics)')}
        for name,typ in [('samples','integer default 0'),('normal_rpc','integer')]:
            if name not in columns:self.db.execute('alter table metrics add column '+name+' '+typ)
        self.db.commit()

    def _metric(self,key,value):
        r=value['receipt']
        self.db.execute('insert into metrics(job_id,bytes,rows,start,stop,samples,normal_rpc) values(?,?,?,?,?,?,?) '
                        'on conflict(job_id) do update set samples=excluded.samples,normal_rpc=coalesce(metrics.normal_rpc,excluded.normal_rpc)',
                        (key,r['bytes'],r.get('rows',0),r.get('start',0),r.get('stop',0),r.get('validated_samples',0),value.get('normal_provider_rpc')))

    def observe(self, key, role, value):
        old = self.db.execute('select role,local_value from jobs where job_id=?', (key,)).fetchone()
        if old and (old[0] != role or (old[1] is not None and json.loads(old[1])['sha256'] != value['sha256'])):
            raise ValueError('Conflicting local receipt')
        with self.transaction():
            self.db.execute('insert or ignore into jobs values(?,?,?,null,null,?)', (key, role, json.dumps(value, sort_keys=True), time.time()))
            self._metric(key,value)

    def node_commit(self, key, value, role=None):
        old = self.db.execute('select node_value from jobs where job_id=?', (key,)).fetchone()
        if old is None:
            if role is None:
                raise ValueError('Unknown node result')
            with self.transaction():
                self.db.execute('insert into jobs values(?,?,null,?,null,?)', (key, role, json.dumps(value, sort_keys=True), time.time()))
                self._metric(key,value)
                self.db.execute('insert or ignore into node_acceptance(job_id) values(?)',(key,))
        elif old[0] is not None and json.loads(old[0])['sha256'] != value['sha256']:
            raise ValueError('Conflicting node receipt')
        else:
            with self.transaction():
                self.db.execute('update jobs set node_value=? where job_id=?', (json.dumps(value, sort_keys=True), key))
                self._metric(key,value)
                self.db.execute('insert or ignore into node_acceptance(job_id) values(?)',(key,))

    def pending_transfer(self, role, target=None):
        result=[];size=0
        for k,v,b in self.db.execute('select j.job_id,local_value,m.bytes from jobs j join metrics m on j.job_id=m.job_id where role=? and node_value is null and local_value is not null order by j.job_id',(role,)):
            result.append(dict(job_id=k,value=json.loads(v)));size+=b
            if target is not None and size>=target:break
        return result

    def cloud_candidates(self, target=None):
        result=[];size=0
        for k,r,v,b in self.db.execute('select j.job_id,role,node_value,m.bytes from jobs j join metrics m on j.job_id=m.job_id where node_value is not null and cloud_batch is null order by j.job_id'):
            result.append(dict(job_id=k,role=r,value=json.loads(v)));size+=b
            if target is not None and size>=target:break
        return result

    def stage_totals(self):
        return [dict(role=r,jobs=n,rows=rows or 0,bytes=size or 0,blocks=blocks or 0,node_jobs=node_jobs or 0,
                     node_bytes=node_bytes or 0,transfer_bytes=local_bytes or 0,cloud_pending_bytes=cloud_bytes or 0)
                     |dict(validated_samples=samples or 0,normal_provider_rpc_recorded=normal or 0,normal_rpc_jobs_recorded=recorded or 0)
            for r,n,rows,size,blocks,node_jobs,node_bytes,local_bytes,cloud_bytes,samples,normal,recorded in self.db.execute(
             'select role,count(*),sum(m.rows),sum(m.bytes),sum(m.stop-m.start),sum(node_value is not null),'
             'sum(case when node_value is not null then m.bytes else 0 end),'
             'sum(case when local_value is not null and node_value is null then m.bytes else 0 end),'
             'sum(case when node_value is not null and cloud_batch is null then m.bytes else 0 end),sum(m.samples),sum(m.normal_rpc),count(m.normal_rpc) '
             'from jobs j join metrics m on j.job_id=m.job_id group by role')]

    def cloud_commit(self, keys, report):
        batch = report['id']
        raw = json.dumps(report, sort_keys=True)
        old = self.db.execute('select report from cloud where batch_id=?', (batch,)).fetchone()
        if old and old[0] != raw:
            raise ValueError('Conflicting immutable cloud report')
        with self.transaction():
            self.db.execute('insert or ignore into cloud values(?,?)', (batch, raw))
            for key in keys:
                row = self.db.execute('select node_value,cloud_batch from jobs where job_id=?', (key,)).fetchone()
                if not row or row[0] is None or row[1] not in (None, batch):
                    raise ValueError('Cloud coverage conflicts')
                self.db.execute('update jobs set cloud_batch=? where job_id=?', (batch, key))

    def prepare_operation(self, key, kind, role, payload):
        raw=json.dumps(payload,sort_keys=True)
        old=self.db.execute('select kind,role,payload from operations where id=?',(key,)).fetchone()
        if old and old!=(kind,role,raw):raise ValueError('Prepared batch identity changed')
        with self.transaction():self.db.execute('insert or ignore into operations values(?,?,?,?,?,null,?)',(key,kind,role,raw,'prepared',time.time()))

    def pending_operations(self, kind=None):
        sql='select id,kind,role,payload from operations where state=?'
        args=['prepared']
        if kind is not None:sql+=' and kind=?';args.append(kind)
        return [dict(id=k,kind=t,role=r,payload=json.loads(p)) for k,t,r,p in self.db.execute(sql+' order by created_at',args)]

    def finish_operation(self, key, report):
        with self.transaction():self.db.execute('update operations set state=?,report=? where id=?',('complete',json.dumps(report,sort_keys=True),key))


@contextlib.contextmanager
def read_snapshot(path):
    """Pin a WAL read transaction for incremental catalog reads, without copying."""
    with sqlite3.connect('file:'+str(Path(path).resolve())+'?mode=ro',uri=True) as db:
        db.execute('begin')
        try:
            yield db
        finally:
            db.rollback()


@contextlib.contextmanager
def snapshot(path):
    """Read a consistent online SQLite backup; never copy a live DB file."""
    with sqlite3.connect('file:' + str(Path(path).resolve()) + '?mode=ro', uri=True) as src:
        dst = sqlite3.connect(':memory:')
        try:
            src.backup(dst)
            yield dst
        finally:
            dst.close()
