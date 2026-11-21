"""Repeat an accepted interval and compare data values, independent of file layout."""
import argparse
from collections import Counter
from contextlib import closing
import fcntl
import gzip
import json
import subprocess
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import pyarrow.parquet as pq
from shared.artifacts import write_json,digest
from pipeline.runtime import NODE_LOCAL,REVERT,rpc,ssh,transfer
from pipeline.stages import read,state_path,verification_cycle_path,MODE
from pipeline.tracker import Tracker
from pipeline.main import run_cycle,DEFAULT,HISTORICAL,PROFILES


def integers(value):
    return {k:integers(v) for k,v in value.items()} if isinstance(value,dict) else int(value)


def table_rows(path,start,stop):
    return pq.read_table(path,filters=[('block_number','>=',start),('block_number','<',stop)]).to_pylist()


def source_files(folder,start,stop):
    for f in sorted(folder.glob('*.parquet')):
        import re
        m=re.fullmatch(r'ethereum__logs__(\d+)_to_(\d+)\.parquet',f.name) or re.fullmatch(r'(\d+)-(\d+)\.parquet',f.name)
        if m and int(m[1])<stop and int(m[2])>=start:yield f


def rows(folder,start,stop,decoded=False):
    result=[]
    if decoded:
        for receipt in folder.glob('*/receipt.json'):
            import re
            m=re.fullmatch(r'ethereum__logs__(\d+)_to_(\d+)',receipt.parent.name)
            if not m or int(m[1])>=stop or int(m[2])<start:continue
            for table in read(receipt)['tables']:
                for f in (receipt.parent/table['directory']).glob('*.parquet'):result.extend(table_rows(f,start,stop))
    else:
        for f in source_files(folder,start,stop):result.extend(table_rows(f,start,stop))
    return Counter(digest({k:v for k,v in row.items() if k!='source_file'}) for row in result)


def archive_records(folder,start,stop):
    records={}
    for f in source_archive_files(folder,start,stop):
        with gzip.open(f,'rt') as stream:
            for line in stream:
                row=json.loads(line);n=row.get('block_number',row.get('receipt_summary',{}).get('block_number'))
                if start<=n<stop:
                    if n in records:raise ValueError('Duplicate archive block')
                    records[n]=row
    if set(records)!=set(range(start,stop)):raise ValueError('Archive coverage mismatch')
    return records


def source_archive_files(folder,start,stop):
    for f in sorted(folder.glob('*.jsonl.gz')):
        a,z=map(int,f.name.split('.')[0].split('-'))
        if a<stop and z>=start:yield f


def compare_archives(old,new,start,stop):
    exact=0
    a=archive_records(old,start,stop);b=archive_records(new,start,stop)
    for n in range(start,stop):
        x,y=a[n],b[n]
        if x['receipt_summary']!=y['receipt_summary']:raise ValueError(f'Receipt summary differs at {n}')
        if len(x['failed'])!=len(y['failed']):raise ValueError(f'Failed transaction count differs at {n}')
        for i,j in zip(x['failed'],y['failed']):
            if i['receipt']!=j['receipt']:raise ValueError(f'Failed receipt differs at {n}')
            if i['trace']!=j['trace']:
                write_json(new.parent/'trace-mismatch.json',dict(block=n,transaction=i['receipt']['transactionHash'],old=i['trace'],new=j['trace']))
                raise ValueError(f'Trace differs at {n}: {i["receipt"]["transactionHash"]}; see trace-mismatch.json')
            exact+=1
    return dict(exact_traces=exact)


def compare(baseline_plan,new_plan,root,start,stop,revert_baseline=None):
    report=dict(start=start,stop=stop,addresses=[],state='checking')
    old=read(baseline_plan);new=read(new_plan)
    original={j['address']:j for j in old['jobs']}
    for j in new['jobs']:
        before=Path(original[j['address']]['destination']);after=Path(j['destination']);checks={}
        checks['events']=rows(before,start,stop)==rows(after,start,stop)
        checks['decoded']=rows(before.parent.parent/'decoded'/before.name,start,stop,True)==rows(after.parent.parent/'decoded'/after.name,start,stop,True)
        checks['state']=rows(before.parent.parent/'reconstructed'/before.name/MODE,start,stop)==rows(after.parent.parent/'reconstructed'/after.name/MODE,start,stop)
        report['addresses'].append(dict(address=j['address'],checks=checks))
        if not all(checks.values()):raise ValueError(f'Replay differs: {j["address"]}: {checks}')
    if revert_baseline is None:
        old_archive, new_archive, old_selected, new_selected = copy_legacy_revert_inputs(root,start,stop)
    else:
        output = root/'cycles'/f'{start}-{stop-1}'/'datasets/reverted'
        old_archive, new_archive = revert_baseline/'archive', output/'archive'
        old_selected, new_selected = revert_baseline/'selected/chunks', output/'chunks'
    report['traces']=compare_archives(old_archive,new_archive,start,stop)
    a=archive_records(old_selected,start,stop);b=archive_records(new_selected,start,stop)
    for n in a:
        def selected(r):return [(e['receipt']['transactionHash'],e['matched_contracts']) for e in r['failed']]
        if selected(a[n])!=selected(b[n]):raise ValueError(f'Selection differs at {n}')
    report.update(state='passed',receipt_comparison='exact',selection_comparison='exact')
    write_json(root/'verification.json',report)
    return report


def copy_legacy_revert_inputs(root,start,stop):
    archive=root/'comparison';archive.mkdir(exist_ok=True)
    cycle=f'{start}-{stop-1}';log=root/'comparison.log'
    new_traces = '/home/dsc-revert/data/' + verification_cycle_path(root, cycle) + '/traces'
    def copy_batches(remote,folder,local):
        local.mkdir(exist_ok=True)
        listing=subprocess.check_output(ssh(remote,['find',folder,'-maxdepth','1','-name','*.jsonl.gz','-printf','%f\n']),text=True)
        for name in listing.splitlines():
            a,z=map(int,name.split('.')[0].split('-'))
            if a<stop and z>=start:transfer(remote+':'+folder+'/'+name,str(local)+'/',log,lambda:None)
    copy_batches(REVERT,'/home/dsc-revert/data/archive/reverted/drpc-v1/archive',archive/'old')
    copy_batches(REVERT,new_traces+'/archive',archive/'new')
    copy_batches(REVERT,'/home/dsc-revert/data/archive/reverted/drpc-v1/selected/chunks',archive/'old-selected')
    copy_batches(REVERT,new_traces+'/selected/chunks',archive/'new-selected')
    return archive/'old', archive/'new', archive/'old-selected', archive/'new-selected'


def check_report(root):
    report = read(root/'verification.json')
    checks = {'events', 'decoded', 'state'}
    if (report['state'] != 'passed' or report['stop']-report['start'] != 100
            or len(report['addresses']) != 102
            or len({job['address'] for job in report['addresses']}) != 102
            or any(set(job['checks']) != checks or not all(job['checks'].values())
                   for job in report['addresses'])
            or set(report['traces']) != {'exact_traces'}
            or report.get('receipt_comparison') != 'exact'
            or report.get('selection_comparison') != 'exact'):
        raise ValueError('Exact 100-block pipeline verification has not passed')
    return report


def main():
    p=argparse.ArgumentParser();p.add_argument('--blocks',type=int,default=100);p.add_argument('--root',type=Path)
    p.add_argument('--baseline-plan',type=Path)
    p.add_argument('--profiles',type=Path)
    p.add_argument('--revert-baseline',type=Path)
    p.add_argument('--check',action='store_true',help='Check the completed exact 100-block verification gate')
    args=p.parse_args()
    if NODE_LOCAL and (args.root is None or
                       (not args.check and None in (args.baseline_plan,args.profiles,args.revert_baseline))):
        p.error('Node verification requires --root and, for replay, --baseline-plan, --profiles and --revert-baseline')
    args.root = args.root or DEFAULT/'verification'
    if args.check:
        check_report(args.root)
        print('Exact 100-block pipeline verification passed');return
    if args.blocks<1:p.error('blocks must be positive')
    baseline = args.baseline_plan or HISTORICAL
    profiles = args.profiles or PROFILES
    plan=read(baseline)
    stop=plan['end_block']+1;start=stop-args.blocks
    root = args.root.resolve() / 'isolated' / f'{start}-{stop-1}'
    root.mkdir(parents=True, exist_ok=True)
    with (root/'coordinator.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        report = replay(root, baseline, profiles, plan, start, stop, args.revert_baseline)
        if args.blocks == 100:
            write_json(args.root/'verification.json', report)
        print(json.dumps(report))


def replay(root, baseline, profiles, plan, start, stop, revert_baseline=None):
    if rpc('eth_getBlockByNumber', [hex(stop-1), False])['hash'] != plan['end_hash']:
        raise ValueError('Historical verification target hash changed')
    for job in plan['jobs']:
        folder=state_path(job).parent
        matches=[r for f in source_files(folder,start-1,start) for r in table_rows(f,start-1,start)]
        if len(matches)!=1:raise ValueError('Missing replay predecessor state row')
        checkpoint=root/'seeds'/f"{job['address']}.json"
        write_json(checkpoint,dict(state='complete',next_block=start,last_state=integers(json.loads(matches[0]['state_json'])),
                                  identity=dict(mode=MODE,address=job['address'])))
        job['checkpoint']=str(checkpoint)
    prior=root/'predecessor-plan.json';write_json(prior,plan)
    with closing(Tracker(root,purpose='verification')) as tracker:
        tracker.baseline(start-1,rpc('eth_getBlockByNumber',[hex(start-1),False])['hash'])
        cycle=tracker.active() or tracker.begin(stop-1,plan['end_hash'])
        if cycle:
            pointer=dict(cycle=cycle,root=str(root/'cycles'/cycle['id']))
            write_json(root/'current.json',pointer)
            run_cycle(tracker,cycle,prior,profiles)
        tracker.publish(cycle,state='comparing')
        try:
            report = compare(baseline,root/'cycles'/f'{start}-{stop-1}'/'events/plan.json',
                             root,start,stop,revert_baseline)
        except Exception as error:
            tracker.publish(cycle,state='mismatch',error=str(error));raise
        tracker.publish(cycle,state='passed')
        return report


if __name__=='__main__':
    import signal
    def stop(signum,frame):raise SystemExit(128+signum)
    signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
    main()
