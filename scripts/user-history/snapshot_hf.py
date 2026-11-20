"""Freeze the explicit numerical HF inputs for read-only remote execution.

Uses SQLite's online backup API, receipt inventories and rsync. This does not
read or publish canonical catalogs and does not modify accepted input files.
"""
import argparse,json,os,shutil,subprocess,sys,time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from shared.artifacts import checksum,write_json
from distributed_hf import backup_sqlite

BASE=Path('/home/dsc-data')

def prepare(target):
    target=Path(target);image=target/'image'
    if target.exists():raise FileExistsError(target)
    image.mkdir(parents=True)
    current=json.loads((BASE/'runs/aave-user-history-v3/current.json').read_text());root=Path(current['root']);plan=json.loads(Path(current['plan']).read_text());status=json.loads((root/'hf-input-progress.json').read_text())
    if status['state']!='factorized_history_ready':raise ValueError('Factor inputs must be complete before freezing')
    generation=status['generation'];identities={}
    def copy(source):
        source=Path(source);destination=image/source.relative_to(BASE);destination.parent.mkdir(parents=True,exist_ok=True)
        if destination.exists():return
        shutil.copy2(source,destination);identities[str(source)]=checksum(destination)
    for name in ['scripts','artifacts','contracts']:
        subprocess.run(['rsync','-a','--exclude=__pycache__','--exclude=*.pyc',str(BASE/name)+'/',str(image/name)+'/'],check=True)
    # Runtime binaries are isolated too: all workers use the production node
    # release rather than depending on a host's unrelated system installation.
    (image/'bin').mkdir();shutil.copy2(shutil.which('node'),image/'bin/node')
    copy(BASE/'runs/aave-user-history-v3/current.json')
    for name in ['plan.json','position-jobs.json','hf-input-progress.json']:
        copy(root/name)
    for p in sorted((root/'token-profiles').rglob('*')):
        if p.is_file():copy(p)
    factor=Path(status['receipt']);copy(factor)
    if identities[str(factor)]!=status['receipt_sha256']:raise ValueError('Factor receipt changed during snapshot')
    for name in ['accounting.sqlite','position-owners.sqlite']:
        destination=image/(root/name).relative_to(BASE)
        backup_sqlite(root/name,destination);identities[str(root/name)]=checksum(destination)
    import sqlite3
    with sqlite3.connect(image/(root/'accounting.sqlite').relative_to(BASE)) as db:
        for (receipt,) in db.execute('select receipt from nominal_tokens'):copy(receipt)
        if db.execute('select count(*) from nominal_tokens').fetchone()[0]!=len(plan['jobs']):raise ValueError('Snapshot nominal token inventory incomplete')
    oracle=BASE/'runs/aave-oracle-v3'/plan['run_id'];copy(oracle/'plan.json');copy(oracle/'jobs.json')
    oplan=json.loads((oracle/'plan.json').read_text());states=json.loads((oracle/'jobs.json').read_text())
    for job in oplan['jobs']:
        state=states[job['id']]
        if state['state']!='accepted':raise ValueError('Oracle interval is unaccepted')
        receipt=Path(state['receipt']);copy(receipt)
        if identities[str(receipt)]!=state['receipt_sha256']:raise ValueError('Oracle receipt identity changed')
        for f in json.loads(receipt.read_text())['files']:
            path=Path(job['destination'])/'decoded'/f['name'];copy(path)
            if identities[str(path)]!=f['decoded_sha256']:raise ValueError('Decoded Oracle input differs')
    # Symlinks are recorded explicitly; their targets remain within the original
    # runtime layout or the host's same Python 3.10 ABI.
    files={};links={}
    for p in sorted(image.rglob('*')):
        if p.is_symlink():links[str(p.relative_to(image))]=os.readlink(p)
        elif p.is_file():files[str(p.relative_to(image))]=checksum(p)
    manifest=dict(created_at=time.time(),factor_generation=generation,run_id=plan['run_id'],end_block=plan['end_block'],end_hash=plan['end_hash'],files=files,symlinks=links,input_identities=identities,source_db_snapshot_method='sqlite3.Connection.backup; target journal_mode DELETE; quick_check ok',source_generation_unchanged=True)
    write_json(image/'snapshot-manifest.json',manifest)
    print(json.dumps(dict(image=str(image),manifest_sha256=checksum(image/'snapshot-manifest.json'),files=len(files),bytes=sum((image/p).stat().st_size for p in files))))

def verify(image):
    image=Path(image);manifest=json.loads((image/'snapshot-manifest.json').read_text())
    for path,identity in manifest['files'].items():
        if checksum(image/path)!=identity:raise ValueError('Worker snapshot file differs: '+path)
    for path,target in manifest['symlinks'].items():
        if os.readlink(image/path)!=target:raise ValueError('Worker snapshot symlink differs: '+path)
    print(json.dumps(dict(verified=True,files=len(manifest['files']),factor_generation=manifest['factor_generation'])))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('operation',choices=['prepare','verify']);p.add_argument('path');a=p.parse_args()
    (prepare if a.operation=='prepare' else verify)(a.path)
