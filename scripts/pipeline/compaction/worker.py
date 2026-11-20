"""Role-local compaction; only globally completed cycles are supplied by dsc-data."""
import argparse
import json
from pathlib import Path
import signal
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
from pipeline.compaction.store import Store
from pipeline.compaction.collect import Collector

ROOTS=dict(node=Path('/home/node/dsc-extraction/compacted'),data=Path('/home/dsc-data/data/compacted'),
           revert=Path('/home/dsc-revert/data/compacted'))


def main():
    p=argparse.ArgumentParser();p.add_argument('--role',required=True,choices=list(ROOTS));p.add_argument('--request',required=True,type=Path)
    args=p.parse_args();request=json.loads(args.request.read_text())
    with Store(ROOTS[args.role]) as store:
        try:
            collector=Collector(request,args.role,store.head)
            if store.head is not None and collector.through<=store.head:
                store.cleanup();store.status()
            else:
                store.status('collecting',target=collector.through)
                specs=collector.collect()
                layout=dict(version=1,role=args.role,baseline_end=collector.baseline_end,
                            event_interval=10000,receipt_interval=1000,receipt_anchor=10008355,
                            pools=[dict(address=j['address'],protocol=j['protocol'],name=j['name'],
                                        anchor=j['start_block'],historical_encoded=j['destination'],historical_node=j['node_dir'])
                                   for j in collector.baseline['jobs']])
                store.compact(collector.through,specs,dict(cycles=[c['id'] for c in collector.cycles],
                               baseline_end=collector.baseline_end,role=args.role,layout=layout))
        except Exception as error:
            store.status('error',error=str(error));raise
    print((ROOTS[args.role]/'progress.json').read_text(),flush=True)


if __name__=='__main__':
    def stop(signum,frame):raise SystemExit(128+signum)
    for sig in (signal.SIGTERM,signal.SIGHUP,signal.SIGINT):signal.signal(sig,stop)
    main()
