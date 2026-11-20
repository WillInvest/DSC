"""Pull immutable receipt batches directly node -> revert; accept after checksum."""
import fcntl
import gzip
import json
from pathlib import Path
import subprocess
import sys
import time
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from shared.artifacts import checksum,digest,write_json

ROOT=Path('/home/dsc-revert/data/receipts/historical-v1')
SOURCE='node@10.246.103.160:/home/node/dsc-reverted/receipts/historical-v1/'


def receive():
    ROOT.mkdir(parents=True,exist_ok=True)
    plan=ROOT/'plan.json'
    subprocess.run(['rsync','-a','-e','ssh -o BatchMode=yes -o ConnectTimeout=10',SOURCE+'plan.json',str(plan)],check=True,timeout=30)
    scope=json.loads(plan.read_text());identity=digest(scope)
    for folder in ('chunks','metadata'):
        (ROOT/folder).mkdir(exist_ok=True)
        subprocess.run(['rsync','-a','--ignore-existing','--exclude=*.tmp','-e',
                        'ssh -o BatchMode=yes -o ConnectTimeout=10',SOURCE+folder+'/',str(ROOT/folder)+'/'],check=True,timeout=300)
    accepted_path=ROOT/'accepted.json'
    accepted=json.loads(accepted_path.read_text()) if accepted_path.exists() else {}
    for meta in sorted((ROOT/'metadata').glob('*.json')):
        if meta.name in accepted:continue
        info=json.loads(meta.read_text());path=ROOT/'chunks'/(meta.stem+'.jsonl.gz')
        if not path.exists():continue  # Next pull may bring a batch created during this transfer.
        if info['identity']!=identity or checksum(path)!=info['sha256']:raise ValueError(f'Receipt batch checksum/scope mismatch: {meta.name}')
        count=failed=0
        with gzip.open(path,'rt') as stream:
            for line in stream:
                row=json.loads(line)
                if row['block_number']!=info['start']+count:raise ValueError('Noncontiguous receipt batch')
                if len(row['failed_receipts'])!=row['failed_transaction_count']:raise ValueError('Failed receipt count mismatch')
                if any(r['status']!='0x0' for r in row['failed_receipts']):raise ValueError('Nonfailed receipt in failed list')
                count+=1;failed+=row['failed_transaction_count']
        if count!=info['stop']-info['start'] or count!=info['blocks'] or failed!=info['failed_transactions']:raise ValueError('Receipt batch count mismatch')
        accepted[meta.name]=info
    write_json(accepted_path,accepted)
    total=scope['stop']-scope['start'];completed=sum(v['blocks'] for v in accepted.values())
    write_json(ROOT/'progress.json',dict(state='complete' if completed==total else 'receiving',
        completed_blocks=completed,total_blocks=total,failed_transactions=sum(v['failed_transactions'] for v in accepted.values()),
        compressed_bytes=sum(v['bytes'] for v in accepted.values()),updated_at=time.time()))


if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser();parser.add_argument('--root',type=Path,default=ROOT);parser.add_argument('--source',default=SOURCE)
    args=parser.parse_args();ROOT=args.root;SOURCE=args.source
    ROOT.mkdir(parents=True,exist_ok=True)
    with (ROOT/'.lock').open('w') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        try:receive()
        except Exception as error:
            p=ROOT/'progress.json';previous=json.loads(p.read_text()) if p.exists() else {}
            write_json(p,{**previous,'state':'error','error':str(error),'updated_at':time.time()});raise
