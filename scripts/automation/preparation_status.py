"""Read-only node-side snapshot of active one-time Aave preparation."""
import base64
import json
from pathlib import Path
import time


def report():
    jobs=[]
    for proc in Path('/proc').glob('[0-9]*/cmdline'):
        try:
            args=proc.read_bytes().split(b'\0')
            index=next((i for i,a in enumerate(args) if a.endswith(b'/aave_inputs.py')),None)
            if index is None: continue
            request=json.loads(base64.b64decode(args[index+1]))
            root=Path(request['output']);sources=[]
            for folder in sorted(root.glob('0x*')):
                files=list(folder.glob('*.parquet'))
                stats=[p.stat() for p in files]
                completed={p.name for p in files}
                for path in (folder/'.cryo/reports').glob('*.json'):
                    if path.name.startswith('incomplete_'): continue
                    try:
                        result=json.loads(path.read_text()).get('results') or {}
                        completed.update(Path(p).name for p in result.get('completed_paths',[]))
                    except (OSError,ValueError): pass
                expected={f"ethereum__logs__{n:08d}_to_{min(n+9999,request['stop']-1):08d}.parquet"
                          for n in range(request['start'],request['stop'],10000)}
                sources.append(dict(address=folder.name,completed_chunks=len(completed & expected),
                    total_chunks=len(expected),bytes=sum(s.st_size for s in stats),
                    last_output_at=max((s.st_mtime for s in stats),default=None)))
            seeds=list(root.glob('seed-*.json'))
            jobs.append(dict(address=request['address'],protocol=request['protocol'],pid=int(proc.parent.name),
                state='preparing_inputs',seeds_completed=len(seeds),seeds_total=len(request['profiles']),
                sources=sources,updated_at=time.time()))
        except (OSError,ValueError,IndexError): continue
    return dict(jobs=jobs,updated_at=time.time())


if __name__=='__main__':
    print(json.dumps(report()))
