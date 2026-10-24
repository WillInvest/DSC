"""Local transactional coverage ledger; intervals are half-open throughout."""
import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path


class Tracker:
    def __init__(self, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, timeout=30)
        self.db.row_factory = sqlite3.Row
        from orchestration.locking import file_lock
        with file_lock(Path(str(path) + '.init.lock')):
            self._initialize()

    def _initialize(self):
        self.db.executescript('''
        PRAGMA synchronous=FULL;
        CREATE TABLE IF NOT EXISTS monitor_runs (
          id TEXT PRIMARY KEY, manifest TEXT NOT NULL, created TEXT DEFAULT CURRENT_TIMESTAMP);
        CREATE TABLE IF NOT EXISTS monitor_jobs (
          run_id TEXT, job_id TEXT, PRIMARY KEY(run_id,job_id));
        CREATE TABLE IF NOT EXISTS coverage (
          address TEXT, bit INTEGER, config TEXT, start INTEGER, stop INTEGER,
          PRIMARY KEY(address,bit,config,start), CHECK(start<stop));
        CREATE TABLE IF NOT EXISTS jobs (
          id TEXT PRIMARY KEY, spec TEXT NOT NULL, stage TEXT NOT NULL,
          attempts INTEGER NOT NULL DEFAULT 0, error TEXT, receipt TEXT,
          updated TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
        CREATE TABLE IF NOT EXISTS run_lanes (
          run_id TEXT, lane TEXT, finished INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(run_id,lane));
        CREATE TABLE IF NOT EXISTS runs (
          id TEXT PRIMARY KEY, spec TEXT NOT NULL, finished INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS chain_blocks (number INTEGER PRIMARY KEY, hash TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS invalidations (
          address TEXT, start INTEGER, stop INTEGER, reason TEXT,
          created TEXT DEFAULT CURRENT_TIMESTAMP);
        ''')
        columns = {r['name'] for r in self.db.execute('PRAGMA table_info(jobs)')}
        for column in ('start', 'stop'):
            if column not in columns:
                self.db.execute(f'ALTER TABLE jobs ADD COLUMN {column} INTEGER')
        for row in self.db.execute('SELECT id,spec FROM jobs WHERE start IS NULL').fetchall():
            spec = json.loads(row['spec'])
            self.db.execute('UPDATE jobs SET start=?,stop=? WHERE id=?', (spec.get('start', 0), spec.get('stop', 0), row['id']))
        self.db.execute('CREATE INDEX IF NOT EXISTS jobs_range ON jobs(start,stop)')
        legacy = self.db.execute('PRAGMA user_version').fetchone()[0] < 2
        for row in self.db.execute('SELECT id,receipt FROM jobs WHERE receipt IS NOT NULL').fetchall() if legacy else []:
            receipt = json.loads(row['receipt'])
            if 'headers' in receipt:
                self.record_headers(receipt)
                self.db.execute('UPDATE jobs SET receipt=? WHERE id=?', (json.dumps(receipt), row['id']))
        self.db.execute('PRAGMA user_version=2')
        self.db.commit()

    def start_lane(self, key, lane):
        with self.db:
            self.db.execute('INSERT OR REPLACE INTO run_lanes VALUES(?,?,0)', (key,lane))
            self.db.execute('UPDATE runs SET finished=0 WHERE id=?', (key,))

    def finish_lane(self, key, lane):
        with self.db:
            self.db.execute('UPDATE run_lanes SET finished=1 WHERE run_id=? AND lane=?', (key,lane))
            complete = lane == 'all' or self.db.execute(
                "SELECT count(*) FROM run_lanes WHERE run_id=? AND lane IN ('events-state','reverted') AND finished=1",
                (key,)).fetchone()[0] == 2
            if complete:
                self.db.execute('UPDATE runs SET finished=1 WHERE id=?', (key,))

    def record_headers(self, receipt):
        headers = receipt.pop('headers', {})
        if not headers:
            return
        numbers = sorted(map(int, headers))
        for row in self.db.execute('SELECT number,hash FROM chain_blocks WHERE number>=? AND number<=?', (numbers[0], numbers[-1])):
            if str(row['number']) in headers and headers[str(row['number'])] != row['hash']:
                raise ValueError('accepted block hash conflict')
        self.db.executemany('INSERT OR IGNORE INTO chain_blocks VALUES(?,?)', [(int(n),h) for n,h in headers.items()])
        receipt['header_range'] = [numbers[0], numbers[-1]+1]

    @contextmanager
    def transaction(self):
        self.db.execute('BEGIN IMMEDIATE')
        try:
            yield
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def missing(self, address, bit, config, start, stop):
        cursor = start
        for row in self.db.execute('SELECT start,stop FROM coverage WHERE address=? AND bit=? AND config=? AND stop>? AND start<? ORDER BY start', (address, bit, config, start, stop)):
            if cursor < row['start']:
                yield cursor, row['start']
            cursor = max(cursor, row['stop'])
        if cursor < stop:
            yield cursor, stop

    def status(self, address, block, configs):
        return sum(bit for bit, config in configs.items() if not list(self.missing(address, bit, config, block, block + 1)))

    def job(self, ident, spec):
        encoded = json.dumps(spec, sort_keys=True)
        with self.db:
            self.db.execute('INSERT OR IGNORE INTO jobs(id,spec,stage,start,stop) VALUES(?,?,?,?,?)', (ident, encoded, 'pending', spec.get('start', 0), spec.get('stop', 0)))
        if getattr(self, 'run_id', None):
            with self.db:
                self.db.execute('INSERT OR IGNORE INTO monitor_jobs VALUES(?,?)', (self.run_id, ident))
        row = self.db.execute('SELECT * FROM jobs WHERE id=?', (ident,)).fetchone()
        if row['spec'] != encoded:
            raise ValueError('job identity collision')
        return dict(row)

    def stage(self, ident, stage, error=None):
        with self.db:
            self.db.execute('UPDATE jobs SET stage=?,error=?, attempts=attempts+?,updated=CURRENT_TIMESTAMP WHERE id=?', (stage, error, int(stage == 'running'), ident))

    def accept(self, ident, entries, receipt):
        """Only call after destination manifests and durable files are verified."""
        receipt = dict(receipt)
        with self.transaction():
            self.record_headers(receipt)
            for address, bit, config, start, stop in entries:
                if bit not in (1, 2, 4) or start >= stop:
                    raise ValueError('invalid coverage')
                overlap = list(self.db.execute('SELECT start,stop FROM coverage WHERE address=? AND bit=? AND config=? AND stop>=? AND start<=?', (address, bit, config, start, stop)))
                for row in overlap:
                    start, stop = min(start, row['start']), max(stop, row['stop'])
                self.db.execute('DELETE FROM coverage WHERE address=? AND bit=? AND config=? AND stop>=? AND start<=?', (address, bit, config, start, stop))
                self.db.execute('INSERT INTO coverage VALUES(?,?,?,?,?)', (address, bit, config, start, stop))
            self.db.execute("UPDATE jobs SET stage='accepted',error=NULL,receipt=?,updated=CURRENT_TIMESTAMP WHERE id=?", (json.dumps(receipt, sort_keys=True), ident))

    def invalidate(self, address, start, stop, reason, bits=(1, 2, 4)):
        with self.transaction():
            rows = list(self.db.execute('SELECT * FROM coverage WHERE address=? AND stop>? AND start<?', (address, start, stop)))
            for r in rows:
                if r['bit'] not in bits:
                    continue
                self.db.execute('DELETE FROM coverage WHERE address=? AND bit=? AND config=? AND start=?', (address, r['bit'], r['config'], r['start']))
                for a, b in [(r['start'], min(start, r['stop'])), (max(stop, r['start']), r['stop'])]:
                    if a < b:
                        self.db.execute('INSERT INTO coverage VALUES(?,?,?,?,?)', (address, r['bit'], r['config'], a, b))
            self.db.execute('INSERT INTO invalidations(address,start,stop,reason) VALUES(?,?,?,?)', (address, start, stop, reason))

    def summary(self):
        return {'jobs': [dict(r) for r in self.db.execute('SELECT stage,count(*) AS count FROM jobs GROUP BY stage')],
                'coverage': [dict(r) for r in self.db.execute('SELECT address,bit,config,sum(stop-start) AS blocks,min(start) AS first_block FROM coverage GROUP BY address,bit,config')]}

    def backup(self, path):
        with sqlite3.connect(path) as target:
            self.db.backup(target)
