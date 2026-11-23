"""Verify range-first names and the query directory cutover."""

from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'scripts'))
from pipeline.query_layout import FORMAT, NAMING, rename_query_files
from shared.artifacts import write_json


class QueryNamingTests(unittest.TestCase):
    def fixture(self, root):
        self.leaves = ('dex/events', 'lending/health-factor/wallets',
                       'lending/health-factor/reserves', 'lending/health-factor/protocol')
        for leaf in self.leaves:
            (root/leaf).mkdir(parents=True)
        query = root/'metadata/query-layout'
        query.mkdir(parents=True)
        self.marker = root/'metadata/query-publication.json'
        self.state = dict(format=FORMAT, state='complete', market_files={'dex/events': 1},
                          market_through=20, hf_through=25,
                          historical_wallets={'files': 1}, historical_cohorts={'files_per_product': 1},
                          incremental_hf_files={'wallets': 1, 'aggregate_tiles': 0})
        write_json(self.marker, self.state)
        self.files = {}
        self.files['dex/events'] = (('uniswap-v2__0x1__schema-v1__Swap__10-20.parquet',
                                    '10-20__uniswap-v2__0x1__Swap__schema-v1.parquet'),)
        self.files[self.leaves[1]] = (
            ('10-20__parts-0008.parquet', '10-20__wallet-ids-0-9.parquet'),
            ('21-25__wallets-22-24-10.parquet', '22-24__wallet-ids-10-19.parquet'))
        for leaf in self.leaves[2:]:
            self.files[leaf] = (('aggregates-10-20.parquet', '10-20.parquet'),)
        for leaf, names in self.files.items():
            for source, _ in names:
                path = root/leaf/source
                if leaf != self.leaves[1]:
                    path.write_bytes(b'Example query file')
                else:
                    start, stop, low, high = (10, 21, 0, 10) if '__parts-' in source else (22, 25, 10, 20)
                    table = pa.table({'wallet_id': [low]}).replace_schema_metadata(
                        dict(chunk_start=str(start), chunk_stop=str(stop),
                             wallet_start=str(low), wallet_stop=str(high)))
                    pq.write_table(table, path)

    def test_cutover_keeps_file_bytes_and_can_be_rerun(self):
        with TemporaryDirectory() as working:
            root = Path(working)
            self.fixture(root)
            rename_query_files(root, 2)
            rename_query_files(root, 2)
            backup = root/'metadata/query-layout'/NAMING/'previous'
            for leaf, files in self.files.items():
                self.assertEqual({p.name for p in (root/leaf).iterdir()}, {name for _, name in files})
                for legacy, new in files:
                    self.assertTrue((root/leaf/new).samefile(backup/leaf/legacy))

    def test_conflicting_wallet_bounds_leave_live_tree_intact(self):
        with TemporaryDirectory() as working:
            root = Path(working)
            self.fixture(root)
            wallet = root/self.leaves[1]
            legacy = wallet/'10-20__parts-0008.parquet'
            (wallet/'10-20__parts-0009.parquet').write_bytes(legacy.read_bytes())
            with self.assertRaisesRegex(ValueError, 'Conflicting published file'):
                rename_query_files(root, 2)
            self.assertTrue(legacy.is_file())
            self.assertFalse((wallet/'10-20__wallet-ids-0-9.parquet').exists())


if __name__ == '__main__':
    unittest.main()
