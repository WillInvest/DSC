"""Export canonical jobs from the authority's shared-lock snapshot, not file globs."""
import argparse
import csv
import json
import time
from collections import Counter
from pathlib import Path

from hf_flow_queue import read_snapshot
from shared.artifacts import checksum, write_json


def export(ledger, destination):
    destination.mkdir(parents=True, exist_ok=True)
    with read_snapshot(ledger) as db:
        rows = db.execute('select j.job_id,j.role,m.start,m.stop,m.rows,j.cloud_batch,'
                          'j.node_value is not null from jobs j join metrics m using(job_id) '
                          'order by m.start,m.stop').fetchall()
        clouds = {key: json.loads(value) for key, value in db.execute('select batch_id,report from cloud')}
        generations = [row[0] for row in db.execute('select generation from identity')]
    if (len(rows) != 92294 or rows[0][2] != 16291127 or rows[-1][3] != 26407882
            or sum(row[4] for row in rows) != 910310934885
            or any(a[3] != b[2] for a, b in zip(rows, rows[1:]))
            or any(not row[6] for row in rows)):
        raise ValueError('Authority does not match the complete disjoint scientific scope')
    path = destination/'CANONICAL_JOBS.csv'
    with path.open('w', newline='') as handle:
        writer = csv.writer(handle)
        writer.writerow(['job_id', 'producer_role', 'start_block_inclusive', 'stop_block_exclusive',
                         'logical_wallet_block_values', 'canonical_cloud_batch', 'cloud_status'])
        for key, role, start, stop, values, batch, _ in rows:
            if batch is not None and batch not in clouds:
                raise ValueError('Canonical cloud batch lacks acceptance report')
            writer.writerow([key, role, start, stop, values, batch or '',
                             'accepted' if batch else 'pending'])
    write_json(destination/'job-catalog-snapshot.json', dict(
        generation=generations, exported_at=time.time(), jobs=len(rows),
        first_block=rows[0][2], last_block=rows[-1][3]-1,
        logical_wallet_block_values=sum(row[4] for row in rows),
        all_node_accepted=True, cloud_jobs=dict(Counter(row[5] or 'pending' for row in rows)),
        catalog_sha256=checksum(path), accepted_cloud_reports=clouds))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--ledger', type=Path, required=True)
    parser.add_argument('--destination', type=Path, required=True)
    args = parser.parse_args()
    export(args.ledger, args.destination)
