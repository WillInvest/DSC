import unittest
import pyarrow.parquet as pq
import test_aave
from decoding.decode_pool_data import run


class AaveCoverageBoundaryTests(unittest.TestCase):
    def test_stray_parquet_is_not_published(self):
        fixture = test_aave.AaveTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        source = fixture.extracted()
        extra = pq.read_table(source / 'reserves/100/batch.parquet').to_pylist()[:1]
        extra[0]['block_number'] = 101
        extra[0]['call_data'] = fixture.contract.encode_abi('getReserveData', args=[test_aave.ASSETS[1]])
        import pyarrow as pa
        pq.write_table(pa.Table.from_pylist(extra), source / 'stray.parquet')
        with self.assertRaisesRegex(ValueError, 'outside.*coverage'):
            run('state', source, fixture.root/'decoded', fixture.config['abi'], test_aave.ADDRESS, 'aave-v3')
        self.assertFalse((fixture.root/'decoded').exists())

    def test_nested_source_paths_preserve_provenance(self):
        fixture = test_aave.AaveTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        source = fixture.extracted()
        output = fixture.root / 'decoded'
        run('state', source, output, fixture.config['abi'], test_aave.ADDRESS, 'aave-v3')
        rows = pq.read_table(output / 'getReserveData.parquet').to_pylist()
        self.assertEqual({r['source_file'] for r in rows},
                         {'reserves/100/batch.parquet', 'reserves/101/batch.parquet'})
