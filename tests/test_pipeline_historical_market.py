from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from pipeline.compaction.historical_market import MODE, inventory_market
from shared.artifacts import checksum, write_json


class HistoricalMarketTests(unittest.TestCase):
    def test_inventory_preserves_empty_evidence_and_requires_dense_state(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            protocol = root / 'data/dex/uniswap/v2'
            encoded = protocol / 'encoded/pool'
            state = protocol / 'reconstructed/pool' / MODE
            raw_name = 'ethereum__logs__00000010_to_00000011.parquet'
            decoded = protocol / 'decoded/pool' / Path(raw_name).stem
            payload = decoded / 'Swap/events.parquet'
            for folder in [encoded, state, payload.parent]:
                folder.mkdir(parents=True)
            for output, values in [(encoded/raw_name, [10]), (state/'10-11.parquet', [10, 11]),
                                   (state/'12-12.parquet', [12]), (payload, [10])]:
                pq.write_table(pa.table({'block_number': values}), output)
            write_json(state/'progress.json', dict(state='complete', next_block=13,
                                                  identity=dict(address='0x1', mode=MODE)))
            write_json(decoded/'receipt.json', dict(rows=1, tables=[dict(directory='Swap')]))
            write_json(payload.parent/'manifest.json', dict(outputs=[
                dict(file=payload.name, rows=1, sha256=checksum(payload)),
            ]))
            receipt = root / 'runs/receipts/0x1.json'
            empty = 'ethereum__logs__00000012_to_00000012.parquet'
            write_json(receipt, dict(identity=dict(end_block=12, end_hash='pin', address='0x1',
                                                  start_block=10, chunk_size=2),
                                     total_chunks=2, empty_chunks=[empty],
                                     files={raw_name: dict(bytes=(encoded/raw_name).stat().st_size, rows=1)}))
            job = dict(address='0x1', protocol='uniswap-v2', start_block=10, destination=str(encoded))
            plan = dict(end_block=12, end_hash='pin', chunk_size=2)

            manifest = inventory_market(root, job, plan, receipt)
            self.assertEqual(manifest['empty_event_chunks'], [empty])
            self.assertEqual(sum(row['kind']=='state' for row in manifest['files']), 2)
            self.assertEqual(next(row['sha256'] for row in manifest['files']
                                  if row['kind']=='decoded'), checksum(payload))

            (state/'12-12.parquet').unlink()
            with self.assertRaisesRegex(ValueError, 'Missing historical state chunk'):
                inventory_market(root, job, plan, receipt)
