"""Commit all accepted Node cycle datasets to one append-only epoch catalog."""
from dataclasses import replace
import json
from pathlib import Path
import time

from pipeline.compaction.collect import Collector, read
from pipeline.compaction.store import Store
from pipeline.stages import node_cycle_root
from shared.artifacts import checksum, write_json


class NodeCollector(Collector):
    def __init__(self, request, head):
        # All three market layers are local in the Node cycle layout.
        super().__init__(request, 'data', head)

    def add(self, key, spec, fragment):
        start = self.baseline_end + 1
        metadata = dict(spec['metadata'], historical_before=start)
        # A new storage epoch starts at the handoff. The historical catalogs keep
        # their original physical partitions and remain separate read sources.
        spec = dict(spec, anchor=start, metadata=metadata)
        super().add(key, spec, replace(fragment, retire=False) if fragment else None)

    def archives(self, seed):
        if seed:
            raise ValueError('Node epoch must start from a verified historical waterline')
        control = Path(self.request['pipeline_root'])
        receipts = node_cycle_root(control, self.current_cycle)/'receipts'
        selected = control/'cycles'/self.current_cycle/'datasets/reverted'
        self.json_archive(receipts/'chunks', 'receipts', metadata=receipts/'metadata')
        self.json_archive(selected/'archive', 'traces', metadata=selected/'metadata', hash_field='archive_sha256')
        self.json_archive(selected/'chunks', 'selected', metadata=selected/'metadata')


def compact_node(tracker, baseline_plan):
    baseline = read(baseline_plan)
    opening = baseline['end_block']
    current = tracker.position()[0]
    root = tracker.root/'canonical'
    ledger_head = tracker.db.execute('select max(through) from compactions').fetchone()[0]
    if current == opening:
        return
    with Store(root) as store:
        if store.head is not None and store.head > current:
            raise ValueError('Node catalog is ahead of its accepted extraction ledger')
        if ledger_head == current and store.head == current and (tracker.root/'compaction.json').is_file():
            return
        if ledger_head is not None and (store.head is None or store.head < ledger_head):
            raise ValueError('Node catalog no longer contains the committed ledger interval')
        head = opening if store.head is None else store.head
        cycles = []
        for row in tracker.db.execute(
                "select * from cycles where state='complete' and end>? order by start", (head,)):
            cycle = dict(row)
            plan = read(tracker.root/'cycles'/cycle['id']/'events/plan.json')
            cycles.append(dict(cycle, jobs=plan['jobs']))
        if current != head:
            request = dict(baseline=baseline, pipeline_root=str(tracker.root),
                           baseline_events_root=str(Path(baseline_plan).parent), cycles=cycles)
            collector = NodeCollector(request, head)
            if collector.through != current:
                raise ValueError('Node catalog input does not reach the extraction ledger')
            layout = dict(version=1, role='node-unified', epoch_start=opening+1,
                          predecessor_block=opening, predecessor_hash=baseline['end_hash'],
                          predecessor_plan_sha256=checksum(baseline_plan),
                          event_interval=10000, receipt_interval=1000)
            store.compact(current, collector.collect(), dict(layout=layout, cycles=[r['id'] for r in cycles]))
        else:
            # The file catalog can commit before the surrounding progress record.
            store.cleanup()
            store.status()
        progress = read(root/'progress.json')
        if store.head != current or progress['state'] != 'complete':
            raise ValueError('Node unified catalog is not complete')
        report = dict(state='complete', through=current, role='node', canonical_root=str(root),
                      catalog_progress=progress)
        with tracker.db:
            tracker.db.execute('insert or ignore into compactions values(?,?,?)',
                               (current, json.dumps({'node': report}), time.time()))
        write_json(tracker.root/'compaction.json', report)
