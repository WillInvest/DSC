"""Single coordinator on dsc-data, reusing the existing extraction programs."""
import base64
import csv
from functools import lru_cache
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import time

from orchestration.artifacts import checksum, digest, verify, write_json
from orchestration.tracker import Tracker

SCRIPTS = Path(__file__).resolve().parents[1]
SSH = ['ssh', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes', '-o', 'ConnectTimeout=10', '-o', 'ServerAliveInterval=15', '-o', 'ServerAliveCountMax=3']
DEFAULTS = {
    'node': {'host': 'node@10.246.103.160', 'scripts': '/home/node/dsc-extraction/scripts', 'python': '/home/node/dsc-extraction/.venv-abi/bin/python', 'root': '/home/node/dsc-extraction/data/runner'},
    'revert': {'host': 'dsc-revert@10.246.103.26', 'scripts': '/home/dsc-revert/scripts', 'python': '/usr/bin/python3', 'root': '/home/dsc-revert/data/runner'},
}


def shell(args):
    return shlex.join([str(a) for a in args])


def remote(server, request):
    encoded = base64.b64encode(json.dumps(request).encode()).decode()
    command = [server['python'], server['scripts'] + '/orchestration/worker.py', encoded]
    if request['action'] in ('extract', 'select', 'cleanup', 'archive_index', 'archive_reuse'):
        # flock survives an SSH client disconnect and excludes overlapping workers.
        lane = 'events-state' if request.get('spec', {}).get('kind') in ('events','state') else 'reverted'
        command = ['flock', '-s', '-w', '5', server['root'] + '.lock',
                   'flock', '-w', '5', server['root'] + f'.{lane}.lock', *command]
    result = subprocess.run([*SSH, server['host'], shell(command)], check=True, capture_output=True, text=True)
    if result.stderr:
        print(result.stderr, file=sys.stderr, end='')
    return json.loads(result.stdout)


def pull(host, source, destination):
    destination.mkdir(parents=True, exist_ok=True)
    subprocess.run(['rsync', '-rtc', '--fsync', '--checksum', '--partial-dir=.rsync-partial', '-e', shell(SSH), host + ':' + source + '/', str(destination) + '/'], check=True)


def registry(path):
    rows = list(csv.DictReader(path.open()))
    from web3 import Web3
    from protocols.protocol_config import PROTOCOLS
    for r in rows:
        if int(r['chain_id']) != 1 or r['protocol'] not in PROTOCOLS or not Web3.is_address(r['address']):
            raise ValueError('invalid registry entry')
        r['address'] = r['address'].lower()
        r['creation_block'] = int(r['creation_block'])
        if r['creation_block'] < 0:
            raise ValueError('negative deployment block')
    if not rows or len({r['address'] for r in rows}) != len(rows):
        raise ValueError('empty or duplicate registry')
    return rows


@lru_cache(maxsize=16)
def fingerprint(kind, protocol=None):
    if kind == 'archive':
        paths = [SCRIPTS / 'reverted/extract_reverted.py']
    elif kind == 'reverted':
        paths = [SCRIPTS / name for name in ('reverted/extract_reverted.py', 'reverted/select_reverted.py', 'orchestration/worker.py', 'orchestration/artifacts.py')]
    else:
        from protocols.protocol_config import load_protocol
        groups = ['decoding', 'validation']
        paths = [p for group in groups for p in (SCRIPTS/group).rglob('*') if p.suffix == '.py']
        paths += [SCRIPTS / name for name in ('orchestration/worker.py', 'orchestration/artifacts.py',
                   'protocols/protocol_config.py', 'protocols/deployment.py', 'extraction/pool_common.sh', 'extraction/check_deployment.py',
                   f'extraction/extract_pool_{kind}.sh')]
        if kind == 'state':
            paths.append(SCRIPTS/'extraction/extract_reserve_state.py')
        directory = load_protocol(protocol)['abi'].parent if protocol else SCRIPTS/'protocols'
        paths += [p for p in directory.rglob('*') if p.suffix in ('.json','.tsv')]
    return digest({'kind': kind, 'protocol': protocol, 'files': [(str(p.relative_to(SCRIPTS)), checksum(p)) for p in sorted(set(paths))]})


def accept_decoded(raw, output, spec):
    from decoding.decode_pool_data import run
    from protocols.protocol_config import load_protocol
    if list(raw.rglob('*.parquet')):
        run(spec['kind'], raw, output, load_protocol(spec['protocol'])['abi'], spec['address'], spec['protocol'])
    elif spec['kind'] == 'events':
        output.mkdir(parents=True, exist_ok=True)
        write_json(output / 'manifest.json', {'config': spec, 'outputs': [], 'total_rows': 0, 'verified_empty': True})
    else:
        raise ValueError('state cannot be accepted without results')
    manifest = json.loads((output / 'manifest.json').read_text())
    for entry in manifest['outputs']:
        if checksum(output / entry['file']) != entry['sha256']:
            raise ValueError('decoded checksum mismatch')
    return {'path': str(output), 'manifest_sha256': checksum(output / 'manifest.json')}


class Coordinator:
    def __init__(self, args):
        self.args = args
        self.lane = getattr(args, 'lane', 'all')
        self.root = args.data_root.resolve()
        self.servers = json.loads(args.servers.read_text()) if args.servers else DEFAULTS
        self.rows = registry(args.contracts)
        self.tracker = Tracker(self.root / 'tracker.sqlite')
        self.archive_config = fingerprint('archive')
        self.profiles = []
        if args.profiles:
            from protocols.protocol_config import load_protocol
            from protocols.deployment import load_deployment
            for p in sorted(args.profiles.glob('*.json')):
                data = json.loads(p.read_text())
                self.profiles.append(load_deployment(p, load_protocol(data['protocol']), data['address']))
        self.failed = False
        self.unavailable = {}

    def job(self, spec, operation, entries):
        ident = digest(spec)
        self.tracker.job(ident, spec)
        # The ledger decides whether to schedule; a stale job row never overrides it.
        self.tracker.stage(ident, 'running')
        for attempt in range(3):
            try:
                receipt = operation(ident)
                self.tracker.accept(ident, entries, receipt)
                return True
            except subprocess.CalledProcessError as exc:
                if attempt < 2:
                    time.sleep(2 ** attempt)
                    continue
                error = exc.stderr or str(exc)
            except (ValueError, OSError, KeyError) as exc:
                error = str(exc)
            self.tracker.stage(ident, 'failed', error)
            print(f'Failed {ident[:12]}: {error}', file=sys.stderr)
            self.failed = True
            return False

    def extraction(self, spec, hashes, ident):
        server = self.servers['node']
        source = server['root'] + '/' + ident
        raw = self.root / 'incoming' / ident
        if (raw / 'coverage.json').exists():
            verify(raw, spec, hashes)
        else:
            self.tracker.stage(ident, 'extracting')
            remote(server, {'action': 'extract', 'spec': spec, 'output': source})
            self.tracker.stage(ident, 'transferring')
            pull(server['host'], source, raw)
            verify(raw, spec, hashes)
        self.tracker.stage(ident, 'validating')
        accepted = accept_decoded(raw, self.root / 'accepted' / ident, spec)
        receipt = {'headers': hashes, 'coverage': str(raw / 'coverage.json'), 'decoded': accepted}
        # Receipt and decoded data are durable before tracker commit; cleanup follows it.
        write_json(self.root / 'accepted' / ident / 'acceptance.json', receipt)
        return receipt

    def cleanup(self, spec):
        ident = digest(spec)
        server = self.servers['node']
        try:
            remote(server, {'action': 'cleanup', 'root': server['root'], 'output': server['root'] + '/' + ident, 'spec': spec})
        except (subprocess.CalledProcessError, ValueError) as exc:
            self.tracker.stage(ident, 'cleanup_pending', str(exc))
        else:
            self.tracker.stage(ident, 'cleaned')

    def reverted(self, start, stop, active, hashes):
        archive_spec = {'kind': 'archive', 'start': start, 'stop': stop, 'config': self.archive_config, 'chain_id': 1}
        archive_id = digest(archive_spec)
        node, revert = self.servers['node'], self.servers['revert']
        source = node['root'] + '/' + archive_id
        archive = revert['root'] + '/archive/' + archive_id
        config = fingerprint('reverted')
        need = [r for r in active if list(self.tracker.missing(r['address'], 4, config, max(start, r['creation_block']), stop))]
        if not need:
            return
        spec = {'kind': 'reverted', 'chain_id': 1, 'start': start, 'stop': stop, 'config': config,
                'archive_config': self.archive_config, 'contracts': sorted(r['address'] for r in need)}

        if 'revert' in self.unavailable:
            ident = digest(spec)
            self.tracker.job(ident, spec)
            self.tracker.stage(ident, 'blocked', self.unavailable['revert'])
            self.failed = True
            return

        def operation(ident):
            store = revert['root'] + '/archive-store/' + self.archive_config
            reused = remote(revert, {'action': 'archive_reuse', 'spec': archive_spec, 'output': archive, 'store': store, 'headers': hashes})
            # Reuse the retained archive before considering node work.
            try:
                existing = remote(revert, {'action': 'verify', 'output': archive})
            except subprocess.CalledProcessError:
                existing = None
            if existing is None:
                self.tracker.job(archive_id, archive_spec)
                self.tracker.stage(archive_id, 'extracting')
                if reused['available']:
                    subprocess.run([*SSH, node['host'], shell(['mkdir', '-p', source])], check=True)
                    command = shell(['rsync', '-rtc', '--fsync', '--checksum', '-e', shell(SSH), archive + '/blocks', node['host'] + ':' + source + '/'])
                    subprocess.run([*SSH, revert['host'], command], check=True)
                remote(node, {'action': 'extract', 'spec': archive_spec, 'output': source})
                command = 'mkdir -p ' + shlex.quote(archive) + ' && ' + shell([
                    'rsync', '-rtc', '--fsync', '--checksum', '--partial-dir=.rsync-partial', '-e', shell(SSH), node['host'] + ':' + source + '/', archive + '/'])
                subprocess.run([*SSH, revert['host'], command], check=True)
                existing = remote(revert, {'action': 'verify', 'output': archive})
            if existing['spec'] != archive_spec or existing['headers'] != hashes:
                raise ValueError('archived chain/configuration mismatch')
            remote(revert, {'action': 'archive_index', 'spec': archive_spec, 'output': archive, 'store': store, 'headers': hashes})
            self.tracker.job(archive_id, archive_spec)
            self.tracker.accept(archive_id, [], {'archive': archive, 'headers': hashes})
            selection = revert['root'] + '/selected/' + ident
            remote(revert, {'action': 'select', 'spec': spec, 'archive': archive, 'output': selection})
            destination = self.root / 'selected' / ident
            pull(revert['host'], selection, destination)
            verify(destination, spec, hashes)
            from reverted.extract_reverted import load
            for n in range(start, stop):
                payload = load(destination / 'blocks' / f'{n//1000:09d}' / f'{n}.json.gz')
                if payload['block_hash'] != hashes[str(n)] or payload['contracts'] != spec['contracts']:
                    raise ValueError('selected reverted coverage mismatch')
            receipt = {'headers': hashes, 'selected': str(destination), 'manifest_sha256': checksum(destination / 'coverage.json')}
            write_json(destination / 'acceptance.json', receipt)
            return receipt

        entries = [(r['address'], 4, config, max(start, r['creation_block']), stop) for r in need]
        if self.job(spec, operation, entries) and self.args.cleanup:
            self.cleanup(archive_spec)

    def dataset(self, row, kind, bit, start, stop, hashes):
        from protocols.protocol_config import load_protocol
        config = load_protocol(row['protocol'])
        boundaries = {start, stop}
        matches = [p for p in self.profiles if p['address'].lower() == row['address']]
        for p in matches:
            boundaries.update(n for n in (p['from_block'], p['to_block']) if start < n < stop)
        bounds = sorted(boundaries)
        for a, b in zip(bounds, bounds[1:]):
            profiles = [p for p in matches if p['from_block'] <= a and b <= p['to_block']]
            profile = profiles[0] if len(profiles) == 1 else None
            identity = digest({'code': fingerprint(kind, row['protocol']), 'profile': profile})
            spec = {'kind': kind, 'chain_id': 1, 'protocol': row['protocol'], 'address': row['address'],
                    'start': a, 'stop': b, 'config': identity, 'profile': profile, 'code': fingerprint(kind, row['protocol'])}
            if config.get('requires_deployment') and profile is None:
                ident = digest(spec)
                self.tracker.job(ident, spec)
                self.tracker.stage(ident, 'blocked', 'Missing or overlapping reviewed Aave ABI profile')
                self.failed = True
                continue
            for lo, hi in list(self.tracker.missing(row['address'], bit, identity, a, b)):
                task = dict(spec, start=lo, stop=hi)
                subset = {str(n): hashes[str(n)] for n in range(lo, hi)}
                if self.job(task, lambda ident: self.extraction(task, subset, ident), [(row['address'], bit, identity, lo, hi)]) and self.args.cleanup:
                    self.cleanup(task)

    def check_previous(self, start, stop, hashes):
        for row in self.tracker.db.execute('SELECT number,hash FROM chain_blocks WHERE number>=? AND number<?', (start, stop)):
            if hashes[str(row['number'])] != row['hash']:
                for contract in self.rows:
                    self.tracker.invalidate(contract['address'], start, stop, 'canonical block hash changed')
                raise ValueError('chain mismatch: coverage invalidated; inspect and quarantine old artifacts before resuming')
        for job in self.tracker.db.execute('SELECT spec,receipt FROM jobs WHERE receipt IS NOT NULL AND stop>? AND start<?', (start, stop)).fetchall():
            spec, receipt = json.loads(job['spec']), json.loads(job['receipt'])
            try:
                if 'decoded' in receipt:
                    accepted = receipt['decoded']
                    path = Path(accepted['path'])
                    if checksum(path/'manifest.json') != accepted['manifest_sha256']:
                        raise ValueError('decoded manifest changed')
                    for entry in json.loads((path/'manifest.json').read_text())['outputs']:
                        if checksum(path/entry['file']) != entry['sha256']:
                            raise ValueError('decoded file changed')
                elif 'selected' in receipt:
                    path = Path(receipt['selected'])
                    if checksum(path/'coverage.json') != receipt['manifest_sha256']:
                        raise ValueError('selected manifest changed')
                    verify(path)
            except (ValueError, OSError) as exc:
                bit = {'events':1, 'state':2, 'reverted':4}.get(spec['kind'])
                for address in spec.get('contracts', [spec.get('address')]):
                    if address and bit:
                        self.tracker.invalidate(address, spec['start'], spec['stop'], str(exc), (bit,))
                raise ValueError('accepted data damaged; affected flags cleared, inspect before retrying') from exc

    def process_batch(self, start, stop, hashes):
        active = [r for r in self.rows if r['creation_block'] < stop]
        if self.lane in ('all', 'events-state'):
            for row in active:
                for kind, bit in [('events', 1), ('state', 2)]:
                    self.dataset(row, kind, bit, max(start, row['creation_block']), stop, hashes)
        if self.lane in ('all', 'reverted'):
            self.reverted(start, stop, active, hashes)

    def run(self):
        if self.args.matrix:
            if self.args.from_block is None or self.args.to_block is None or not 0 <= self.args.to_block-self.args.from_block < 1000:
                raise ValueError('--matrix requires inclusive bounds covering 1–1000 blocks')
            writer = csv.writer(sys.stdout)
            writer.writerow(['block_number'] + [r['address'] for r in self.rows])
            for n in range(self.args.from_block, self.args.to_block + 1):
                cells = []
                for r in self.rows:
                    if n < r['creation_block']:
                        cells.append('N/A')
                        continue
                    profiles = [p for p in self.profiles if p['address'].lower() == r['address'] and p['from_block'] <= n < p['to_block']]
                    profile = profiles[0] if len(profiles) == 1 else None
                    configs = {bit: digest({'code': fingerprint(kind, r['protocol']), 'profile': profile}) for kind,bit in [('events',1),('state',2)]}
                    configs[4] = fingerprint('reverted')
                    cells.append(self.tracker.status(r['address'], n, configs))
                writer.writerow([n] + cells)
            return
        if self.args.status:
            print(json.dumps(self.tracker.summary(), indent=2))
            return
        import shutil
        if shutil.disk_usage(self.root).free < 1024**3:
            raise ValueError('less than 1 GiB available on dsc-data')
        for role, server in self.servers.items():
            if self.lane == 'events-state' and role == 'revert':
                continue
            try:
                remote(server, {'action': 'preflight', 'root': server['root']})
            except subprocess.CalledProcessError as exc:
                if role == 'node':
                    raise
                self.unavailable[role] = exc.stderr or str(exc)
                print(f'{role} unavailable; its jobs will be blocked', file=sys.stderr)
        first = max(min(r['creation_block'] for r in self.rows), self.args.from_block or 0)
        run_key = digest({'contracts': checksum(self.args.contracts), 'from': first, 'to': self.args.to_block,
                          'profiles': self.profiles, 'code': fingerprint('events'), 'trace': self.archive_config})
        previous = self.tracker.db.execute('SELECT * FROM runs WHERE id=?', (run_key,)).fetchone()
        if previous and not previous['finished']:
            settings = json.loads(previous['spec'])
            end = settings['stop']
        else:
            head = remote(self.servers['node'], {'action': 'head'})['number']
            end = (self.args.to_block if self.args.to_block is not None else head) + 1
            if end - 1 > head or end <= first:
                raise ValueError('invalid or nonfinalized requested range')
            settings = {'start': first, 'stop': end, 'cutoff_hash': remote(self.servers['node'], {'action': 'headers', 'start': end-1, 'stop': end})[str(end-1)]}
        if self.args.dry_run:
            print(json.dumps({'contracts': len(self.rows), 'start': first, 'stop_exclusive': end,
                              'batches': (end-first+999)//1000, 'state_step': 1,
                              'aave_profiles': len(self.profiles), 'note': 'No extraction; missing profiles block Aave event/state jobs.'}, indent=2))
            return
        if remote(self.servers['node'], {'action': 'headers', 'start': end-1, 'stop': end})[str(end-1)] != settings['cutoff_hash']:
            raise ValueError('run cutoff hash changed')
        with self.tracker.db:
            self.tracker.db.execute('INSERT OR REPLACE INTO runs VALUES(?,?,0)', (run_key, json.dumps(settings)))
        from orchestration.reporting import record_run
        record_run(self, run_key, settings)
        self.tracker.start_lane(run_key, self.lane)
        for start in range(first, end, 1000):
            stop = min(start+1000, end)
            print(f'{self.lane} batch {start}:{stop}', flush=True)
            if self.lane != 'all':
                write_json(self.root / f'progress-{self.lane}.json',
                           {'run_id': self.tracker.run_id, 'start': start, 'stop': stop})
            hashes = remote(self.servers['node'], {'action': 'headers', 'start': start, 'stop': stop})
            self.check_previous(start, stop, hashes)
            self.process_batch(start, stop, hashes)
            if self.args.cleanup:
                owned = {'all': ('events', 'state', 'archive'),
                         'events-state': ('events', 'state'), 'reverted': ('archive',)}[self.lane]
                for job in self.tracker.db.execute("SELECT spec FROM jobs WHERE stage IN ('accepted','cleanup_pending') AND receipt IS NOT NULL AND start>=? AND stop<=?", (start, stop)).fetchall():
                    spec = json.loads(job['spec'])
                    if spec['kind'] in owned:
                        self.cleanup(spec)
            self.tracker.backup(self.root / ('tracker.backup.sqlite' if self.lane == 'all' else f'tracker.{self.lane}.backup.sqlite'))
        if not self.failed:
            self.tracker.finish_lane(run_key, self.lane)
        print(json.dumps(self.tracker.summary(), indent=2))
        if self.failed:
            raise SystemExit('Run has failed or blocked jobs; accepted coverage is retained.')
