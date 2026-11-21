import gzip
import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from pipeline.revert_worker import PROTOCOL, RevertWorker, verify_full_archive
from reverted.node_worker import parse_command
from shared.artifacts import checksum

SCOPE = 'a'*24 + '/10-20'


class RevertWorkerTests(unittest.TestCase):
    def test_forced_command_allows_only_typed_operations_and_read_only_sender(self):
        self.assertEqual(parse_command(f'{PROTOCOL} receive {SCOPE}'), ('receive', SCOPE))
        self.assertEqual(parse_command('rsync --server --sender -logDtpre.iLsfxCIvu . ./'), ('download', None))
        for value in [f'{PROTOCOL} trace ../20-30', f'{PROTOCOL} trace {SCOPE} extra',
                      f'{PROTOCOL} trace {"a"*24}/20-10', 'rsync --server -logDtpre . ./',
                      'cat /private', f'{PROTOCOL} receive {SCOPE}; id']:
            with self.subTest(value=value), self.assertRaises(ValueError):
                parse_command(value)

    def test_client_pins_the_scope_in_command_and_download(self):
        client = RevertWorker('/private/config', SCOPE)
        self.assertEqual(client.command('trace')[-3:], [PROTOCOL, 'trace', SCOPE])
        with TemporaryDirectory() as folder:
            command = client.download('archive', Path(folder))
            self.assertIn('--no-links', command)
            self.assertEqual(command[-2], f'dsc-revert-pipeline:{SCOPE}/traces/archive/')
            with self.assertRaises(ValueError):
                client.download('../../private', Path(folder))

    def test_full_trace_verification_checks_identity_and_block_coverage(self):
        with TemporaryDirectory() as folder:
            root = Path(folder)
            path = root / '10-10.jsonl.gz'
            row = dict(identity='plan', receipt_summary={'block_number': 10}, failed=[{}])
            with gzip.open(path, 'wt') as out:
                out.write(json.dumps(row))
            accepted = {'10': dict(start=10, stop=11, failed_transactions=1,
                                    identity='plan', archive_sha256=checksum(path))}
            self.assertEqual(verify_full_archive(root, accepted), 1)
            accepted['10']['identity'] = 'other'
            with self.assertRaisesRegex(ValueError, 'coverage or identity'):
                verify_full_archive(root, accepted)
