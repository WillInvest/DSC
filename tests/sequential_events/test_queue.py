import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest

SOURCE = Path(__file__).resolve().parents[2] / 'scripts' / 'event-cryo-extraction'
sys.path.insert(0, str(SOURCE))
from common import chunks, summarize, write
from node import inventory, report_successes
from run import validate_manifest, validate_destination
import pyarrow as pa
import pyarrow.parquet as pq


class QueueTest(unittest.TestCase):
    def setUp(self):
        self.job = {'address': '0x123', 'start_block': 12}
        self.plan = {'end_block': 32, 'end_hash': '0xabc', 'chunk_size': 10, 'jobs': [self.job]}

    def manifest(self):
        return {'identity': {'end_block': 32, 'end_hash': '0xabc', 'chunk_size': 10,
                             'address': '0x123', 'start_block': 12},
                'files': {}, 'empty_chunks': list(chunks(self.job, self.plan)), 'total_chunks': 3}

    def test_inclusive_tail_and_empty_coverage(self):
        self.assertEqual(list(chunks(self.job, self.plan).values()), [10, 10, 1])
        validate_manifest(self.manifest(), self.plan, self.job)

    def test_missing_chunk_and_wrong_cutoff_rejected(self):
        m = self.manifest()
        m['empty_chunks'].pop()
        with self.assertRaises(ValueError):
            validate_manifest(m, self.plan, self.job)
        m = self.manifest()
        m['identity']['end_block'] = 33
        with self.assertRaises(ValueError):
            validate_manifest(m, self.plan, self.job)

    def test_report_errors_are_not_empty_evidence(self):
        with tempfile.TemporaryDirectory() as folder:
            names = list(chunks(self.job, self.plan))
            write(Path(folder) / 'final.json', {'results': {'completed_paths': [names[0]],
                                                          'errored_paths': names[1:]}})
            write(Path(folder) / 'incomplete_a.json', {'results': {'completed_paths': names}})
            self.assertEqual(report_successes(Path(folder), chunks(self.job, self.plan)), {names[0]})

    def test_interrupted_parquet_quarantined_and_transfer_mismatch_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / 'encoded'
            output.mkdir()
            expected = chunks(self.job, self.plan)
            first, second, _ = expected
            pq.write_table(pa.table({'block_number': [12]}), output / first)
            (output / second).write_bytes(b'PAR1broken')
            files = inventory(output, expected)
            self.assertEqual(set(files), {first})
            self.assertFalse((output / second).exists())
            validate_destination({'files': files}, output)
            files[first]['rows'] = 2
            with self.assertRaises(ValueError):
                validate_destination({'files': files}, output)

    def test_only_received_addresses_count_as_complete(self):
        for state in ['running', 'failed', 'extracted', 'transferring']:
            self.assertEqual(summarize(self.plan, {'0x123': {'state': state}})['accepted_blocks'], 0)
        self.assertEqual(summarize(self.plan, {'0x123': {'state': 'complete'}})['accepted_blocks'], 21)


if __name__ == '__main__':
    unittest.main()
