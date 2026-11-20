"""dsc-data pulls only selections produced on dsc-revert; accept once by checksum."""
import fcntl
import gzip
import json
from pathlib import Path
import subprocess
import sys
import time
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from shared.artifacts import checksum,write_json

ROOT=Path('/home/dsc-data/data/reverted/drpc-v1')
SOURCE='dsc-revert@10.246.103.26:/home/dsc-revert/data/archive/reverted/drpc-v1/selected/'


def receive():
    ROOT.mkdir(parents=True,exist_ok=True)
    for folder in ('chunks','metadata'):
        (ROOT/folder).mkdir(exist_ok=True)
        subprocess.run(['rsync','-a','--ignore-existing','--exclude=*.tmp','-e','ssh -o BatchMode=yes -o ConnectTimeout=10',SOURCE+folder+'/',str(ROOT/folder)+'/'],check=True,timeout=300)
    accepted_path=ROOT/'accepted.json';accepted=json.loads(accepted_path.read_text()) if accepted_path.exists() else {}
    for meta in sorted((ROOT/'metadata').glob('*.json')):
        if meta.name in accepted:continue
        info=json.loads(meta.read_text());path=ROOT/'chunks'/(meta.stem+'.jsonl.gz')
        if not path.exists():continue
        if checksum(path)!=info['sha256']:raise ValueError('Selected batch checksum mismatch')
        count=selected=0
        with gzip.open(path,'rt') as stream:
            for line in stream:
                row=json.loads(line)
                if row['block_number']!=info['start']+count:raise ValueError('Noncontiguous selected batch')
                if any(not e['matched_contracts'] or e['receipt']['status']!='0x0' for e in row['failed']):raise ValueError('Invalid selected transaction')
                count+=1;selected+=len(row['failed'])
        if count!=info['blocks'] or count!=info['stop']-info['start'] or selected!=info['selected_transactions']:raise ValueError('Selected batch count mismatch')
        if accepted:
            previous=next(iter(accepted.values()))
            if previous['selection']!=info['selection'] or previous['identity']!=info['identity']:raise ValueError('Selection identity changed')
        accepted[meta.name]=info
    write_json(accepted_path,accepted)
    total=max((x.get('total_blocks',0) for x in accepted.values()),default=0)
    completed=sum(x['blocks'] for x in accepted.values())
    write_json(ROOT/'progress.json',dict(state='complete' if total and completed==total else 'receiving',total_blocks=total,accepted_blocks=completed,selected_transactions=sum(x['selected_transactions'] for x in accepted.values()),updated_at=time.time()))


if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser();parser.add_argument('--root',type=Path,default=ROOT);parser.add_argument('--source',default=SOURCE)
    args=parser.parse_args();ROOT=args.root;SOURCE=args.source
    ROOT.mkdir(parents=True,exist_ok=True)
    with (ROOT/'.lock').open('w') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        try:receive()
        except Exception as error:
            path=ROOT/'progress.json';old=json.loads(path.read_text()) if path.exists() else {}
            write_json(path,{**old,'state':'error','error':str(error),'updated_at':time.time()});raise
