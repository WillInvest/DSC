"""Run/resume a bounded cycle, or continuously catch up to finalized Ethereum."""
import argparse
import fcntl
import signal
import time
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from pipeline.runtime import NODE_LOCAL,REVERT,remote_json,rpc
from pipeline.stages import Stages,read,validate_previous
from pipeline.tracker import Tracker
from shared.artifacts import write_json

DEFAULT=Path('/home/dsc-data/runs/pipeline')
HISTORICAL=Path('/home/dsc-data/runs/sequential-events/plan.json')
PROFILES=Path('/home/dsc-data/runs/decode-two/profiles')


def initialize(tracker, baseline_plan):
    if tracker.position() is not None:return
    plan=read(baseline_plan);end=plan['end_block'];validate_previous(plan,end+1)
    receipts=remote_json(REVERT,'/home/dsc-revert/data/receipts/historical-v1/progress.json')
    traces=remote_json(REVERT,'/home/dsc-revert/data/archive/reverted/drpc-v1/progress.json')
    delivery=read('/home/dsc-data/data/reverted/drpc-v1/progress.json')
    expected=end-10008355+1
    for p,key in [(receipts,'completed_blocks'),(traces,'archived_blocks'),(delivery,'accepted_blocks')]:
        if p['state']!='complete' or p[key]!=expected:raise ValueError('Historical baseline incomplete')
    if rpc('eth_getBlockByNumber',[hex(end),False])['hash']!=plan['end_hash']:raise ValueError('Historical hash changed')
    tracker.baseline(end,plan['end_hash'])
    write_json(tracker.root/'baseline.json',dict(plan=str(baseline_plan),end_block=end,end_hash=plan['end_hash']))


def run_cycle(tracker, cycle, baseline_plan, profiles):
    predecessor=tracker.db.execute("select id from cycles where state='complete' order by end desc limit 1").fetchone()
    previous=tracker.root/'cycles'/predecessor['id'] if predecessor else None
    previous_plan=previous/'events/plan.json' if previous else baseline_plan
    previous_profiles=previous/'profiles' if previous else profiles
    current = 'preparing'

    def heartbeat():
        tracker.publish(cycle, current)

    def accept_stage(name, operation):
        nonlocal current
        current = name
        if tracker.done(cycle, name):
            return
        tracker.stage(cycle, name, 'running', {})
        heartbeat()
        try:
            evidence = operation()
        except Exception as error:
            tracker.stage(cycle, name, 'error', {'error': str(error)})
            tracker.publish(cycle, name, 'error', error=str(error))
            raise
        tracker.stage(cycle, name, 'complete', evidence)
        heartbeat()

    stages = Stages(tracker.root, cycle, previous_plan, previous_profiles, heartbeat,
                    verification=tracker.purpose == 'verification')
    heartbeat()
    try:
        stages.prepare()
    except Exception as error:
        tracker.publish(cycle, current, 'error', error=str(error))
        raise

    accept_stage('profiles', stages.profiles_stage)
    accept_stage('events', stages.events_stage)
    accept_stage('decode', stages.decode_stage)
    accept_stage('reconstruct', stages.reconstruct_stage)

    accept_stage('receipts', stages.receipts_stage)
    accept_stage('receipt_delivery', stages.receipt_delivery_stage)
    accept_stage('traces', stages.traces_stage)
    accept_stage('selected_delivery', stages.selected_delivery_stage)

    tracker.finish(cycle)
    tracker.publish(cycle, state='complete')


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('mode',choices=['once','continuous','status'])
    p.add_argument('--root',type=Path,default=DEFAULT);p.add_argument('--end-block',type=int)
    p.add_argument('--baseline-plan',type=Path,default=HISTORICAL);p.add_argument('--profiles',type=Path,default=PROFILES)
    p.add_argument('--poll-seconds',type=int,default=30);args=p.parse_args()
    if args.poll_seconds<5:p.error('poll-seconds must be at least 5')
    if args.mode=='continuous' and args.end_block is not None:p.error('end-block belongs to a bounded once run')
    if args.mode=='status':print((args.root/'progress.json').read_text());return
    if NODE_LOCAL:
        p.error('This entrypoint still uses the legacy Data compaction layout. Use pipeline/verify.py for Node replays.')
    args.root.mkdir(parents=True,exist_ok=True)
    with (args.root/'coordinator.lock').open('w') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        tracker=Tracker(args.root);initialize(tracker,args.baseline_plan)
        def stop(signum,frame):
            progress=read(args.root/'progress.json') if (args.root/'progress.json').exists() else {}
            tracker.publish(tracker.active(),progress.get('stage'),state='stopped')
            raise SystemExit(128+signum)
        signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
        while True:
            from pipeline.compaction.run import compact_completed
            compact_completed(tracker,args.baseline_plan)
            cycle=tracker.active()
            if not cycle:
                head=rpc('eth_getBlockByNumber',['finalized',False]);end=args.end_block if args.end_block is not None else int(head['number'],16)
                if end>int(head['number'],16):raise ValueError('Requested ending block is not finalized')
                block=head if end==int(head['number'],16) else rpc('eth_getBlockByNumber',[hex(end),False])
                previous_height,previous_hash=tracker.position()
                if rpc('eth_getBlockByNumber',[hex(previous_height),False])['hash']!=previous_hash:raise ValueError('Previous finalized anchor changed')
                cycle=tracker.begin(end,block['hash'])
            if cycle:
                write_json(args.root/'current.json',dict(cycle=cycle,root=str(args.root/'cycles'/cycle['id'])))
                run_cycle(tracker,cycle,args.baseline_plan,args.profiles)
                compact_completed(tracker,args.baseline_plan)
            else:tracker.publish(state='waiting_finalized',observed_finalized=int(head['number'],16))
            if args.mode=='once':break
            if not cycle:time.sleep(args.poll_seconds)


if __name__=='__main__':
    def stop(signum,frame):raise SystemExit(128+signum)
    signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
    main()
