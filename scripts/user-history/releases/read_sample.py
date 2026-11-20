"""Read an unpacked DSC Aave HF sample with NumPy and PyArrow."""
import argparse
import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq


def load_wallet(folder, wallet_id):
    folder = Path(folder)
    manifest = json.loads((folder / '04_validation/manifest.json').read_text())
    pieces = []
    for name in manifest['user_vector_files']:
        path = folder / name
        table = pq.read_table(path, filters=[('wallet_id', '=', wallet_id)])
        if not table.num_rows:
            continue
        meta = table.schema.metadata
        first = manifest['block_origin'] + table['first_block_index'][0].as_py()
        start = max(first, int(meta[b'chunk_start']))
        values = np.asarray(table['hf_vector'][0].as_py(), dtype=np.float64)
        pieces.append(pa.table({'block_number': np.arange(start, start + len(values), dtype=np.uint32),
                                'wallet_id': np.full(len(values), wallet_id, dtype=np.uint32),
                                'health_factor': values}))
    if not pieces:
        raise ValueError('Wallet is absent from this sample; see 03_dictionaries/users.csv')
    result = pa.concat_tables(pieces).sort_by([('block_number', 'ascending')])
    if len(np.unique(result['block_number'].to_numpy())) != result.num_rows:
        raise ValueError('Duplicate wallet/block data')
    blocks = pq.read_table(folder / '03_dictionaries/blocks.parquet')
    indices = pc.index_in(result['block_number'], value_set=blocks['block_number'])
    if indices.null_count:
        raise ValueError('Missing block timestamps')
    return result.append_column('timestamp_utc', pc.take(blocks['timestamp_utc'], indices))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--folder', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--wallet-id', type=int, default=1)
    parser.add_argument('--output', type=Path, default=Path('wallet_hf.csv'))
    args = parser.parse_args()
    frame = load_wallet(args.folder, args.wallet_id)
    pacsv.write_csv(frame, args.output)
    print(f'Saved {len(frame):,} consecutive wallet/block values to {args.output}')
