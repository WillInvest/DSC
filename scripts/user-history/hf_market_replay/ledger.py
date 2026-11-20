"""Node-owned immutable scope, finite worker assignments and accepted coverage."""
import json

from shared.artifacts import digest
import re
import sqlite3
import time
from pathlib import Path


JOB_ID = re.compile(r'^[A-Za-z0-9_-]{1,100}$')



def load_plan(path):
    plan = json.loads(Path(path).read_text())
    if digest(plan['identity']) != plan['generation']:
        raise ValueError('Plan generation identity differs')
    if plan['identity'].get('source_pins_sha256') != digest(plan['source_pins']):
        raise ValueError('Replay source identity differs')
    jobs = sorted(plan['jobs'], key=lambda job: job['start'])
    if not jobs or len({job['id'] for job in jobs}) != len(jobs):
        raise ValueError('Invalid job scope')
    cursor = jobs[0]['start']
    for job in jobs:
        if not JOB_ID.fullmatch(job['id']) or job['start'] != cursor or job['stop'] <= cursor:
            raise ValueError('Invalid or noncontiguous job scope')
        cursor = job['stop']
    scope = plan['identity']['scope']
    if jobs[0]['start'] != scope['start'] or cursor != scope['stop']:
        raise ValueError('Plan jobs do not cover declared scope')
    if plan['identity'].get('jobs_sha256') != digest(plan['jobs']):
        raise ValueError('Job scope hash differs')
    return plan


class Ledger:
    def __init__(self, path, generation, jobs):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.execute('pragma journal_mode=WAL')
        self.db.execute('pragma synchronous=FULL')
        self.db.execute('create table if not exists identity(generation text, scope_sha text)')
        identity = (generation, digest(jobs))
        existing = self.db.execute('select generation,scope_sha from identity').fetchall()
        if existing and existing != [identity]:
            self.close()
            raise ValueError('Ledger generation identity or scope differs')
        self.db.execute('create table if not exists jobs(id text primary key,start integer,stop integer,role text,lease_until real,payload_sha text,receipt text,accepted_role text,accepted_at real)')
        self.db.execute('create table if not exists observations(role text primary key,seen_at real,status text)')
        with self.db:
            if not existing:
                self.db.execute('insert into identity values(?,?)', identity)
                self.db.executemany('insert into jobs(id,start,stop) values(?,?,?)',
                                    [(job['id'], job['start'], job['stop']) for job in jobs])

    def close(self):
        self.db.close()

    def observe(self, role, status, now=None):
        now = time.time() if now is None else now
        with self.db:
            self.db.execute('insert into observations values(?,?,?) on conflict(role) do update set seen_at=excluded.seen_at,status=excluded.status',
                            (role, now, json.dumps(status, sort_keys=True)))

    def renew(self, role, now=None, lease_seconds=600):
        now = time.time() if now is None else now
        with self.db:
            self.db.execute('update jobs set lease_until=? where role=? and payload_sha is null',
                            (now + lease_seconds, role))

    def expire(self, now=None):
        now = time.time() if now is None else now
        rows = [row[0] for row in self.db.execute('select id from jobs where role is not null and payload_sha is null and lease_until<=? order by start', (now,))]
        with self.db:
            self.db.execute('update jobs set role=null,lease_until=null where payload_sha is null and lease_until<=?', (now,))
        return rows

    def assign(self, role, count, now=None, lease_seconds=600):
        now = time.time() if now is None else now
        with self.db:
            keys = [row[0] for row in self.db.execute('select id from jobs where role is null and payload_sha is null order by start limit ?', (max(0, count),))]
            self.db.executemany('update jobs set role=?,lease_until=? where id=? and role is null and payload_sha is null',
                                [(role, now + lease_seconds, key) for key in keys])
        return keys

    def assigned(self, role):
        return [row[0] for row in self.db.execute('select id from jobs where role=? and payload_sha is null order by start', (role,))]

    def accept(self, key, payload_sha, receipt, role):
        row = self.db.execute('select payload_sha from jobs where id=?', (key,)).fetchone()
        if row is None:
            raise ValueError('Unknown completion scope')
        if row[0] is not None:
            if row[0] != payload_sha:
                raise ValueError('Conflicting immutable completion for ' + key)
            return False
        with self.db:
            self.db.execute('update jobs set payload_sha=?,receipt=?,accepted_role=?,accepted_at=? where id=?',
                            (payload_sha, str(receipt), role, time.time(), key))
        return True

    def accepted(self, key):
        row = self.db.execute('select payload_sha from jobs where id=?', (key,)).fetchone()
        return row[0] if row else None

    def totals(self):
        jobs, blocks, accepted_jobs, accepted_blocks = self.db.execute(
            'select count(*),sum(stop-start),sum(payload_sha is not null),sum(case when payload_sha is not null then stop-start else 0 end) from jobs').fetchone()
        return dict(planned_jobs=jobs, planned_blocks=blocks or 0,
                    accepted_jobs=accepted_jobs or 0, accepted_blocks=accepted_blocks or 0)


class LocalResults:
    """Append-only local publication index; output receipts remain independently checkable."""
    def __init__(self, path, generation):
        self.db = sqlite3.connect(path)
        self.db.execute('pragma journal_mode=WAL')
        self.db.execute('pragma synchronous=FULL')
        self.db.execute('create table if not exists done(sequence integer primary key autoincrement,job_id text unique,receipt_sha256 text,payload_sha256 text)')
        self.db.execute('create table if not exists identity(generation text primary key)')
        existing = self.db.execute('select generation from identity').fetchall()
        if existing != [(generation,)]:
            if existing or self.db.execute('select count(*) from done').fetchone()[0]:
                self.db.close()
                raise ValueError('Local result generation differs or is unbound')
            self.db.execute('insert into identity values(?)', (generation,))
        self.db.commit()

    def completed(self):
        return {row[0] for row in self.db.execute('select job_id from done')}

    def commit(self, key, receipt_sha, payload_sha):
        old = self.db.execute('select receipt_sha256,payload_sha256 from done where job_id=?', (key,)).fetchone()
        if old is not None and old != (receipt_sha, payload_sha):
            raise ValueError('Local completion identity changed')
        with self.db:
            self.db.execute('insert or ignore into done(job_id,receipt_sha256,payload_sha256) values(?,?,?)',
                            (key, receipt_sha, payload_sha))

    def entries(self, after=0, limit=128):
        return [dict(sequence=n, job_id=key, receipt_sha256=receipt_sha, payload_sha256=payload_sha)
                for n, key, receipt_sha, payload_sha in self.db.execute(
                    'select sequence,job_id,receipt_sha256,payload_sha256 from done where sequence>? order by sequence limit ?',
                    (after, limit))]

    def close(self):
        self.db.close()
