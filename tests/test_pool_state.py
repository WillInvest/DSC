import importlib.util
import io
from pathlib import Path
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
if ROOT.name == 'scripts':  # Deployed tests live under scripts/tests.
    ROOT = ROOT.parent
SPEC = importlib.util.spec_from_file_location('verify_pool_state', ROOT / 'scripts/validation/verify_pool_state.py')
VERIFY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VERIFY)
ADDRESS = '0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8'
CATALOG = ROOT / 'scripts/protocols/dex/uniswap/v3/state.tsv'


class StateValidationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.folder = Path(self.temp.name)
        self.rows = []
        for line in CATALOG.read_text().splitlines():
            _, selector, length = line.split('\t')
            self.rows.append(dict(block_number=13000999, contract_address=ADDRESS,
                                  call_data=selector, output_data='0x'+'00'*int(length), chain_id=1))

    def verify(self, rows, blocks='13000999'):
        pq.write_table(pa.Table.from_pylist(rows), self.folder / 'state.parquet')
        argv = ['verify', '--blocks', blocks, '--address', ADDRESS,
                '--output', str(self.folder), '--catalog', str(CATALOG)]
        with patch('sys.argv', argv), redirect_stdout(io.StringIO()):
            VERIFY.main()

    def test_complete_zero_values_are_valid(self):
        self.verify(self.rows)
        self.verify(self.rows)

    def test_missing_getter_rejected(self):
        with self.assertRaisesRegex(ValueError, 'incomplete'):
            self.verify(self.rows[:-1])

    def test_null_call_rejected(self):
        self.rows[0]['output_data'] = None
        with self.assertRaisesRegex(ValueError, 'null'):
            self.verify(self.rows)

    def test_truncated_result_rejected(self):
        self.rows[0]['output_data'] = '0x00'
        with self.assertRaisesRegex(ValueError, 'length'):
            self.verify(self.rows)

    def test_duplicate_result_rejected(self):
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            self.verify(self.rows + [self.rows[0]])

    def test_wrong_chain_rejected(self):
        self.rows[0]['chain_id'] = 2
        with self.assertRaisesRegex(ValueError, 'chain'):
            self.verify(self.rows)

    def test_sampling_only_requires_selected_blocks(self):
        extra = [dict(row, block_number=13001001) for row in self.rows]
        self.verify(self.rows + extra, '13000999:13001002:2')


if __name__ == '__main__':
    unittest.main()
