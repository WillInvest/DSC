"""One durable coordinator ledger; worker receipts remain the acceptance evidence."""
import json
import sqlite3
import time
from pathlib import Path
from shared.artifacts import write_json

STAGES = ('profiles', 'events', 'decode', 'reconstruct', 'receipts',
          'receipt_delivery', 'traces', 'selected_delivery')


class Tracker:
    def __init__(self, root, purpose='production'):
        self.purpose=purpose
        self.root = Path(root); self.root.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.root / 'tracker.sqlite')
        self.db.row_factory = sqlite3.Row
        self.db.execute('pragma journal_mode=WAL')
        self.db.execute('pragma synchronous=FULL')
        self.db.executescript('''
          create table if not exists position(id integer primary key check(id=1), height integer, hash text);
          create table if not exists cycles(id text primary key,start integer,end integer,hash text,state text);
          create table if not exists stages(cycle text,name text,state text,evidence text,updated real,
            primary key(cycle,name));
          create index if not exists cycle_completion on cycles(state,end);
          create table if not exists compactions(through integer primary key,roles text,committed real);
        ''')

    def close(self): self.db.close()

    def baseline(self, height, block_hash):
        if self.position() is None:
            with self.db: self.db.execute('insert into position values(1,?,?)', (height, block_hash))

    def position(self):
        r = self.db.execute('select height,hash from position where id=1').fetchone()
        return tuple(r) if r else None

    def active(self):
        r = self.db.execute("select * from cycles where state!='complete' order by start limit 1").fetchone()
        return dict(r) if r else None

    def begin(self, end, block_hash):
        if self.active(): return self.active()
        height, previous_hash = self.position()
        if end == height and previous_hash != block_hash: raise ValueError('Finalized hash changed')
        if end <= height: return None
        cycle = dict(id=f'{height+1}-{end}', start=height+1, end=end, hash=block_hash, state='running')
        with self.db:
            self.db.execute('insert into cycles values(:id,:start,:end,:hash,:state)', cycle)
        return cycle

    def done(self, cycle, name):
        r = self.db.execute('select state from stages where cycle=? and name=?', (cycle['id'], name)).fetchone()
        return bool(r and r[0] == 'complete')

    def stage(self, cycle, name, state, evidence):
        if name not in STAGES: raise ValueError(name)
        with self.db:
            self.db.execute('insert or replace into stages values(?,?,?,?,?)',
                            (cycle['id'], name, state, json.dumps(evidence), time.time()))

    def finish(self, cycle):
        if not all(self.done(cycle, s) for s in STAGES): raise ValueError('Incomplete pipeline stages')
        if self.position()[0]+1 != cycle['start']: raise ValueError('Noncontiguous cycle')
        with self.db:
            self.db.execute("update cycles set state='complete' where id=?", (cycle['id'],))
            self.db.execute('update position set height=?,hash=? where id=1', (cycle['end'], cycle['hash']))

    def publish(self, cycle=None, stage=None, state='running', **extra):
        stages = [dict(r) for r in self.db.execute('select name,state,evidence,updated from stages where cycle=?',
                                                  ((cycle or {}).get('id'),))]
        write_json(self.root/'progress.json', dict(state=state, purpose=self.purpose, cycle=cycle, stage=stage,
                   completed_through=self.position()[0], stages=stages, updated_at=time.time(), **extra))
