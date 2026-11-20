"""Privileged, manifest-only HF result retirement; roots fixed by root config."""
import argparse
import fcntl
import stat
from contextlib import contextmanager, ExitStack
import hashlib
import json
import os
import re
import resource
from pathlib import Path
from hf_flow_artifacts import retire
from shared.artifacts import digest


@contextmanager
def job_locks(directories, exclusive=False):
    """Hold job lifetime locks while reading files or retiring their bytes."""
    directories=sorted({Path(p).resolve() for p in directories})
    previous=resource.getrlimit(resource.RLIMIT_NOFILE)
    required=len(directories)+64
    target=required if previous[1]==resource.RLIM_INFINITY else min(required,previous[1])
    raised=previous[0]!=resource.RLIM_INFINITY and target>previous[0]
    if raised:resource.setrlimit(resource.RLIMIT_NOFILE,(target,previous[1]))
    try:
        with ExitStack() as stack:
            for directory in directories:
                path = directory / 'flow.lock'
                flags = os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW
                created = False
                try:
                    fd = os.open(path, flags | os.O_CREAT | os.O_EXCL, 0o600)
                    created = True
                except FileExistsError:
                    fd = os.open(path, flags)
                stream = stack.enter_context(os.fdopen(fd, 'r+b'))
                if not stat.S_ISREG(os.fstat(fd).st_mode):
                    raise ValueError('Non-regular job lock')
                if created and os.geteuid() == 0:
                    owner = directory.stat()
                    os.fchown(fd, owner.st_uid, owner.st_gid)
                fcntl.flock(fd, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
            yield
    finally:
        if raised:resource.setrlimit(resource.RLIMIT_NOFILE,previous)


def main():
    p=argparse.ArgumentParser();p.add_argument('request');p.add_argument('request_sha256');a=p.parse_args()
    policy=json.loads(Path('/etc/dsc-hf-flow-cleanup.json').read_text())
    request=Path(a.request).resolve()
    if not any(request.is_relative_to(Path(x).resolve()) for x in policy['request_roots']):raise ValueError('Unapproved request location')
    raw=request.read_bytes()
    if hashlib.sha256(raw).hexdigest()!=a.request_sha256:raise ValueError('Retirement request changed')
    value=json.loads(raw);cert=value['certificate'];root=Path(cert['root']).resolve();directories=set()
    for name in cert['files']:
        path=(root/name).resolve()
        valid=any(path.is_relative_to(Path(x).resolve()) for x in policy['output_roots'])
        directory=path.parent
        numeric=(valid and re.fullmatch(r'[0-9]+-[0-9]+',directory.name)
                 and directory.parent.name=='jobs' and directory.parent.parent.name==policy['generation']
                 and re.fullmatch(r'(parts|aggregate_parts)-[0-9]{4}\.parquet',path.name))
        package=(any(path.is_relative_to(Path(x).resolve()) for x in policy.get('package_roots',[]))
                 and directory.name=='packs' and re.fullmatch(r'[A-Za-z0-9_-]{1,96}',directory.parent.name)
                 and re.fullmatch(r'pack-[0-9]{4}\.tar',path.name)
                 and value['proof'].get('generation')==policy['generation'])
        if not numeric and not package:raise ValueError('Only explicit approved result Parquets/TAR chunks may be retired')
        if numeric:directories.add(directory)
    log=request.with_suffix('.retirement.jsonl')
    for path in [log,log.with_name(log.name+'.lock')]:
        if path.is_symlink():raise ValueError('Symlink retirement journal')
    with job_locks(directories, exclusive=True):
        result=retire(root,cert,log,value['proof'],digest(value['proof']))
    print(json.dumps(result),flush=True)

if __name__=='__main__':main()
