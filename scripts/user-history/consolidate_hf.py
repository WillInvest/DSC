"""Retire explicit consumed HF replicas only after independent node readback."""
import argparse
import fcntl
import hashlib
import json
import os
import stat
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from shared.artifacts import checksum, write_json

HOMES = {'revert': Path('/home/dsc-revert'), 'web': Path('/home/fao')}
VIRTUAL = Path('/home/dsc-data/data/aave-user-history-v3/26407881/accounting/hf-numeric')
WORK = Path('aave-hf-distributed/5197dae89b140ccb/outputs')
NODE_HOME = Path('/home/node')


def identity(path):
    s = path.lstat()
    if not stat.S_ISREG(s.st_mode): raise ValueError('Nonregular retirement object')
    return dict(dev=s.st_dev, ino=s.st_ino, bytes=s.st_size, mtime_ns=s.st_mtime_ns, mode=stat.S_IMODE(s.st_mode), uid=s.st_uid, gid=s.st_gid)


def contained(path, root):
    path = Path(path); path.resolve().relative_to(root.resolve())
    if path.is_symlink(): raise ValueError('Symlink retirement object')
    return path


def aliases(path, home):
    found = [path]
    checkpoints = home / 'hf-failover-20261115/backup-checkpoints'
    if checkpoints.exists():
        for folder in checkpoints.iterdir():
            target = folder / 'publish' / path.relative_to(home)
            if target.exists() and not target.is_symlink() and target.samefile(path): found.append(target)
    return found


def prepare(role, inventory, output, reverse_backup=False):
    if role == 'web':
        with (HOMES[role] / 'dex-compaction-writer.lock').open('r+') as guard:
            fcntl.flock(guard,fcntl.LOCK_EX|fcntl.LOCK_NB)
            return _prepare(role,inventory,output,reverse_backup)
    return _prepare(role,inventory,output,reverse_backup)


def _prepare(role, inventory, output, reverse_backup=False):
    home = HOMES[role]; output = Path(output)
    if output.exists(): raise ValueError('Preserve existing retirement plan')
    source = json.loads(Path(inventory).read_text()); items = []; missing = []
    if reverse_backup:
        root = home / 'hf-backups/lan-20261116/node'
        candidates = [(root / name, Path('/home/node') / name, item['sha256'], []) for name,item in source['files'].items() if name.endswith('/accounts.parquet')]
    else:
        root = home / WORK
        candidates = []
        for item in source['files']:
            if item['role'] != role or not item['fully_consumed']: continue
            relative = Path(item['path']).relative_to(VIRTUAL)
            path = root / relative
            backup = Path('/home/node/hf-backups/lan-20261116') / role / path.relative_to(home)
            candidates.append((path, backup, item['sha256'], item['source_jobs']))
    for path,backup,sha,jobs in candidates:
        contained(path,root)
        if not path.exists(): missing.append(dict(source=str(path), backup=str(backup), sha256=sha)); continue
        ident = identity(path)
        if checksum(path) != sha: raise ValueError('Source changed from accepted evidence')
        links = aliases(path,home) if not reverse_backup else [path]
        if path.stat().st_nlink != len(links): raise ValueError('Unaccounted hard links; preserve source')
        items.append(dict(source=str(path),backup=str(backup),sha256=sha,bytes=ident['bytes'],source_jobs=jobs,aliases=[dict(path=str(p),identity=identity(p)) for p in links]))
    report = dict(role=role,kind='reverse_backup' if reverse_backup else 'consumed_source',inventory_sha256=checksum(Path(inventory)),prepared_at=time.time(),files=items,missing_sources=missing,df_before=os.statvfs(home).f_bavail*os.statvfs(home).f_frsize)
    write_json(output,report)
    return dict(plan=str(output),sha256=checksum(output),files=len(items),bytes=sum(x['bytes'] for x in items),missing_sources=len(missing))


def verify_node(plan, output):
    plan = Path(plan); raw=plan.read_bytes(); plan_sha=hashlib.sha256(raw).hexdigest(); report = json.loads(raw)
    def check(item):
        path = contained(item['backup'],NODE_HOME)
        exact = path.is_file() and path.stat().st_size == item['bytes'] and checksum(path) == item['sha256']
        return dict(source=item['source'],backup=str(path),sha256=item['sha256'],exact=bool(exact))
    with ThreadPoolExecutor(max_workers=2) as pool:checks=list(pool.map(check,report['files']))
    value = dict(plan_sha256=plan_sha,checked_at=time.time(),files=checks,all_exact=all(x['exact'] for x in checks),bytes=sum(x['bytes'] for x in report['files']))
    output = Path(output)
    if output.exists(): raise ValueError('Preserve existing node verification')
    write_json(output,value); return dict(report=str(output),sha256=checksum(output),files=len(checks),all_exact=value['all_exact'],bytes=value['bytes'])


def retire(role, plan, proof, proof_sha256):
    if os.geteuid()!=0:raise PermissionError('Retirement requires complete root process-descriptor inspection')
    plan = Path(plan); proof = Path(proof)
    raw_proof=proof.read_bytes();raw_plan=plan.read_bytes();plan_sha=hashlib.sha256(raw_plan).hexdigest()
    if hashlib.sha256(raw_proof).hexdigest() != proof_sha256: raise ValueError('Node proof identity differs')
    report = json.loads(raw_plan); validation = json.loads(raw_proof); home = HOMES[role]
    if report['role'] != role or validation['plan_sha256'] != plan_sha or not validation['all_exact']: raise ValueError('Independent backup verification is incomplete')
    expected = {(x['source'],x['backup'],x['sha256']) for x in report['files']}
    if {(x['source'],x['backup'],x['sha256']) for x in validation['files'] if x['exact']} != expected: raise ValueError('Node proof covers different files')
    guard_path = home / 'dex-compaction-writer.lock' if role == 'web' else plan.parent / 'retire.lock'
    with guard_path.open('r+' if role == 'web' else 'a') as guard:
        fcntl.flock(guard,fcntl.LOCK_EX|fcntl.LOCK_NB)
        open_objects = set()
        for proc in Path('/proc').iterdir():
            if not proc.name.isdigit(): continue
            try:
                for fd in (proc/'fd').iterdir():
                    try: s=fd.stat(); open_objects.add((s.st_dev,s.st_ino))
                    except FileNotFoundError: pass
            except FileNotFoundError: pass
        log = plan.with_suffix('.actions.jsonl')
        for item in report['files']:
            remaining = []
            for obj in item['aliases']:
                path = contained(obj['path'],home)
                if not path.exists(): continue
                if identity(path) != obj['identity']: raise ValueError('Planned object changed')
                if (obj['identity']['dev'],obj['identity']['ino']) in open_objects: raise ValueError('Planned object is open')
                remaining.append(path)
            if not remaining: continue
            if remaining[0].stat().st_nlink != len(remaining) or checksum(remaining[0]) != item['sha256']: raise ValueError('Changed content or unknown aliases')
            for path in remaining:
                obj=next(x for x in item['aliases'] if x['path']==str(path))
                if identity(path)!=obj['identity']:raise ValueError('Object changed before unlink')
                path.unlink()
                with log.open('a') as stream:
                    stream.write(json.dumps(dict(path=str(path),backup=item['backup'],sha256=item['sha256'],removed_at=time.time()))+'\n'); stream.flush(); os.fsync(stream.fileno())
        free = os.statvfs(home).f_bavail*os.statvfs(home).f_frsize
        value = dict(completed_at=time.time(),plan_sha256=plan_sha,node_proof_sha256=proof_sha256,files=len(report['files']),logical_bytes=sum(x['bytes'] for x in report['files']),df_before=report['df_before'],df_after=free,df_net_change=free-report['df_before'],node_copies_retained=True)
        target = plan.with_suffix('.completed.json')
        if target.exists(): raise ValueError('Preserve existing completion receipt')
        write_json(target,value); return value


if __name__ == '__main__':
    parser=argparse.ArgumentParser(); parser.add_argument('mode',choices=['prepare','verify-node','retire']); parser.add_argument('--role',choices=HOMES); parser.add_argument('--inventory'); parser.add_argument('--plan',required=True); parser.add_argument('--output'); parser.add_argument('--proof'); parser.add_argument('--proof-sha256'); parser.add_argument('--reverse-backup',action='store_true'); args=parser.parse_args()
    if args.mode=='prepare': result=prepare(args.role,args.inventory,args.plan,args.reverse_backup)
    elif args.mode=='verify-node': result=verify_node(args.plan,args.output)
    else: result=retire(args.role,args.plan,args.proof,args.proof_sha256)
    print(json.dumps(result),flush=True)
