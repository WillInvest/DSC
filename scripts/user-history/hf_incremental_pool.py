"""Pin the HF window's Aave Pool events from Node's accepted event catalog."""
import argparse
import fcntl
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory

from hf_incremental_window import validate_window
from oracle_common import POOL
from shared.artifacts import checksum, write_json


def pool_files(entries, start, stop):
    prefix = 'encoded/aave-v3/'+POOL.lower()+'/'
    selected = sorted((r for r in entries if r['stream'].startswith(prefix)
                       and start<r['stop'] and r['start']<stop),key=lambda r:r['start'])
    cursor = start
    for row in selected:
        lower, upper = max(start,row['start']), min(stop,row['stop'])
        if lower != cursor:
            raise ValueError('Accepted Pool event catalog has a gap or overlap')
        cursor = upper
    if cursor != stop:
        raise ValueError('Accepted Pool event catalog does not cover the HF window')
    return selected


def collect(window_path, catalog, output):
    import pyarrow.dataset as ds
    import pyarrow.parquet as pq
    from web3 import Web3, HTTPProvider
    from decoding.pool_decode import cryo_event_log
    from pipeline.compaction.reader import snapshot

    window = json.loads(window_path.read_text())
    scope = validate_window(window)
    start, stop = scope['start'], scope['stop']
    headers = {h['block']:h['hash'] for h in window['headers']}
    output.mkdir(parents=True, exist_ok=True)
    with (output/'pool.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        target, receipt = output/'pool-events.parquet', output/'pool-inputs.json'
        if receipt.exists():
            evidence = json.loads(receipt.read_text())
            if evidence['window_sha256']!=window['window_sha256'] or evidence['sha256']!=checksum(target):
                raise ValueError('Existing Pool input identity differs')
            return evidence
        with snapshot(catalog) as entries:
            selected = pool_files(entries,start,stop)
            files = [r['absolute_path'] for r in selected]
            for row in selected:
                if checksum(row['absolute_path']) != row['sha256']:
                    raise ValueError('Canonical Pool event source checksum differs')
            table = ds.dataset(files,format='parquet').to_table(
                filter=(ds.field('block_number')>=start)&(ds.field('block_number')<stop))
        table=table.sort_by([('block_number','ascending'),('transaction_index','ascending'),('log_index','ascending')])
        logs=[cryo_event_log(row) for row in table.to_pylist()]
        w3=Web3(HTTPProvider('http://127.0.0.1:8545',request_kwargs={'timeout':60}))
        reference=w3.eth.get_logs({'address':POOL,'fromBlock':start,'toBlock':stop-1})

        def hex_text(value):
            return value.lower() if isinstance(value,str) else Web3.to_hex(value).lower()

        def identity(log):
            if log['address'].lower()!=POOL.lower() or headers[log['blockNumber']]!=hex_text(log['blockHash']):
                raise ValueError('Pool log does not match the pinned address and header')
            return (log['blockNumber'],log['transactionIndex'],log['logIndex'],hex_text(log['transactionHash']),
                    tuple(hex_text(t) for t in log['topics']),hex_text(log['data']))

        if sorted(map(identity,logs)) != sorted(map(identity,reference)):
            raise ValueError('Accepted Pool events differ from the direct RETH query')
        with TemporaryDirectory(dir=output) as temporary:
            staged = Path(temporary)/target.name
            pq.write_table(table,staged,compression='zstd')
            if not pq.read_table(staged).equals(table):
                raise ValueError('Staged Pool Parquet readback differs')
            with staged.open('rb') as file:
                os.fsync(file.fileno())
            staged.replace(target)
        evidence=dict(window_sha256=window['window_sha256'],start=start,stop=stop,rows=len(logs),
                      exact_reth_comparison=True,sources=selected,sha256=checksum(target))
        write_json(receipt,evidence)
        return evidence


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for flag in ('window','catalog','output'):
        parser.add_argument('--'+flag,required=True,type=Path)
    args=parser.parse_args()
    print(json.dumps(collect(args.window,args.catalog,args.output.resolve())))


if __name__ == '__main__':
    main()
