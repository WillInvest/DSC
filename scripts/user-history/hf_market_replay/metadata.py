"""Export sparse account eMode history from the preserved read-only event index."""
import argparse
import json
import sqlite3
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from shared.artifacts import checksum, write_json


def export_emodes(database_path, output, start, stop):
    """Retain ordered category changes, including history before the scope start.

    Category defaults to zero before the first event. At block end the last
    transaction/log in that block wins. Addresses are canonical lowercase.
    """
    database_path, output = Path(database_path), Path(output)
    schema = pa.schema([
        ('block', pa.uint32()), ('transaction_index', pa.uint32()),
        ('log_index', pa.uint32()), ('user', pa.string()),
        ('category', pa.uint16()),
    ]).with_metadata({
        b'format': b'aave-v3-emode-changes-v1', b'default_category': b'0',
        b'scope_start': str(start).encode(), b'scope_stop': str(stop).encode(),
        b'semantics': b'last event ordered by block,transaction_index,log_index at block end',
    })
    with sqlite3.connect(database_path.resolve().as_uri() + '?mode=ro', uri=True) as database:
        database.execute('begin')
        rows = []
        for block, tx, log, args in database.execute(
                "select block,tx,log,args from pool_events where name='UserEModeSet' "
                'and block<? order by block,tx,log', (stop,)):
            args = json.loads(args)
            rows.append(dict(block=block, transaction_index=tx, log_index=log,
                             user=args['user'].lower(), category=int(args['categoryId'])))
        database.rollback()
    table = pa.Table.from_pylist(rows, schema=schema)
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        if not pq.read_table(output).equals(table, check_metadata=True):
            raise ValueError('Retained eMode history differs from the frozen input')
    else:
        temporary = output.with_name(output.name + '.incoming')
        pq.write_table(table, temporary, compression='zstd', compression_level=9,
                       use_dictionary=['user', 'category'])
        if not pq.read_table(temporary).equals(table, check_metadata=True):
            raise ValueError('eMode export readback differs')
        temporary.replace(output)
    proof = dict(format='aave-v3-emode-changes-v1', rows=len(rows),
                 users=len({row['user'] for row in rows}), scope=dict(start=start, stop=stop),
                 bytes=output.stat().st_size, sha256=checksum(output),
                 source=str(database_path), default_category=0, end_of_block=True)
    write_json(output.with_suffix('.proof.json'), proof)
    return proof


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--start', type=int, required=True)
    parser.add_argument('--stop', type=int, required=True)
    args = parser.parse_args()
    print(json.dumps(export_emodes(args.database, args.output, args.start, args.stop)))


if __name__ == '__main__':
    main()
