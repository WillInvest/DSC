#!/usr/bin/env python3
"""Coordinate resumable extraction from dsc-data; CLI block limits are inclusive."""
import argparse
from orchestration.locking import file_lock
from pathlib import Path
from orchestration.coordinator import Coordinator


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--contracts', type=Path, required=True)
    p.add_argument('--data-root', type=Path, default=Path('/home/dsc-data/data'))
    p.add_argument('--servers', type=Path, help='JSON overriding node/revert deployment paths')
    p.add_argument('--profiles', type=Path, help='Directory of reviewed Aave deployment profiles')
    p.add_argument('--from-block', type=int)
    p.add_argument('--to-block', type=int)
    p.add_argument('--dry-run', action='store_true')
    p.add_argument('--status', action='store_true')
    p.add_argument('--matrix', action='store_true', help='Export bounded block-by-address CSV; requires --from-block and --to-block')
    p.add_argument('--lane', choices=['all', 'events-state', 'reverted'], default='all',
                   help='Independent dataset or reverted lane; requires an explicit finalized --to-block')
    p.add_argument('--cleanup', action='store_true', help='Delete acknowledged node staging only')
    p.add_argument('--pool-workers', type=int, choices=[1, 2, 4], default=1,
                   help='Concurrent contracts in the events/state lane')
    p.add_argument('--rpc-requests', type=int, choices=[8, 16, 32], default=8,
                   help='Maximum concurrent Cryo RPC requests per extraction process')
    args = p.parse_args()
    if args.lane != 'all' and args.to_block is None and not (args.status or args.matrix):
        p.error('independent lanes require the same explicit --to-block cutoff')
    args.data_root.mkdir(parents=True, exist_ok=True)
    if args.status or args.matrix:
        Coordinator(args).run()
        return
    with file_lock(args.data_root / 'coordinator.lock', shared=args.lane != 'all', blocking=False), \
            file_lock(args.data_root / f'coordinator-{args.lane}.lock', blocking=False):
        from orchestration.reporting import heartbeat
        if args.dry_run:
            Coordinator(args).run()
        else:
            if args.lane == 'all':
                # The exclusive lock makes previous split-lane status obsolete.
                for lane in ('events-state', 'reverted'):
                    for name in ('heartbeat', 'progress'):
                        (args.data_root / f'{name}-{lane}.json').unlink(missing_ok=True)
            with heartbeat(args.data_root, args.lane):
                Coordinator(args).run()


if __name__ == '__main__':
    main()
