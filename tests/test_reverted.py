from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from reverted.common import load
from reverted.scan_receipts import receipt_record
from reverted.trace_received import select, trace_block


class RpcFixture:
    name = 'test'
    kind = 'drpc'

    def __init__(self, failed=()):
        self.header = dict(number='0xa', hash='0xabc', transactions=[f'0x{i}' for i in range(6)])
        self.receipts = [
            dict(status=hex(int(i not in failed)), transactionIndex=hex(i), transactionHash=f'0x{i}',
                 blockHash=self.header['hash'], blockNumber=self.header['number'])
            for i in range(6)
        ]
        self.calls = []
        self.fail = None

    def trace(self, index):
        fields = dict(type='CALL', gas='0x10', gasUsed='0x5', input='0x', to='0xAA')
        fields['from'] = '0xc'
        if self.receipts[index]['status'] == '0x0':
            fields.update(error='execution reverted', calls=[dict(fields, to='0xBB')])
        return fields

    def rpc(self, method, params):
        self.calls.append((method, params[0]))
        if method == 'eth_getBlockByNumber':
            return self.header
        if method == 'eth_getBlockReceipts':
            return self.receipts
        if method == 'debug_traceTransaction':
            if self.fail == params[0]:
                raise RuntimeError('RPC failed')
            return self.trace(int(params[0], 16))
        if method == 'debug_traceBlockByHash':
            return [dict(txHash=f'0x{i}', result=self.trace(i)) for i in range(6)]
        raise AssertionError(method)

    def receipt_summary(self):
        with patch('reverted.scan_receipts.LOCAL', SimpleNamespace(rpc=self.rpc)):
            return receipt_record(10)


class ExtractionTests(unittest.TestCase):
    def run_block(self, rpc, root):
        return trace_block(rpc.receipt_summary(), rpc, Path(root), 'test')

    def test_skip_and_resume(self):
        with TemporaryDirectory() as root:
            rpc = RpcFixture()
            result = self.run_block(rpc, root)
            self.assertEqual(result['strategy'], 'skip')
            self.assertEqual(self.run_block(rpc, root), result)
            self.assertFalse(any(method.startswith('debug') for method, _ in rpc.calls))

    def test_individual_at_the_strategy_boundary(self):
        with TemporaryDirectory() as root:
            rpc = RpcFixture([5])
            self.assertEqual(self.run_block(rpc, root)['strategy'], 'individual')
            self.assertIn(('debug_traceTransaction', '0x5'), rpc.calls)

    def test_block_is_pinned_by_hash_and_discards_successful_traces(self):
        with TemporaryDirectory() as root:
            rpc = RpcFixture([3, 5])
            self.assertEqual(self.run_block(rpc, root)['strategy'], 'block')
            data = load(Path(root) / '10.json.gz')
            self.assertEqual([entry['receipt']['transactionHash'] for entry in data['failed']], ['0x3', '0x5'])
            self.assertIn(('debug_traceBlockByHash', '0xabc'), rpc.calls)

    def test_empty_receipts_for_nonempty_block_fail(self):
        rpc = RpcFixture([2])
        rpc.receipts = []
        with self.assertRaisesRegex(ValueError, 'Empty receipts for nonempty block'):
            rpc.receipt_summary()

    def test_interrupted_individual_resumes_checkpoint(self):
        with TemporaryDirectory() as root:
            rpc = RpcFixture([0, 2])
            rpc.fail = '0x2'
            with self.assertRaisesRegex(RuntimeError, 'RPC failed'):
                self.run_block(rpc, root)
            rpc.fail = None
            rpc.calls.clear()
            self.run_block(rpc, root)
            self.assertEqual([(m, h) for m, h in rpc.calls if m.startswith('debug')],
                             [('debug_traceTransaction', '0x2')])

    def test_hash_change_cannot_reuse_the_checkpoint(self):
        with TemporaryDirectory() as root:
            rpc = RpcFixture([2])
            result = self.run_block(rpc, root)
            rpc.receipts = [dict(receipt, blockHash='0xchanged') for receipt in rpc.receipts]
            with self.assertRaisesRegex(ValueError, 'Changed checkpoint identity'):
                self.run_block(rpc, root)
            self.assertEqual(load(Path(root) / '10.json.gz'), result)

    def test_corrupt_archive_fails_resume(self):
        with TemporaryDirectory() as root:
            rpc = RpcFixture([2])
            self.run_block(rpc, root)
            (Path(root) / '10.json.gz').write_bytes(b'broken')
            with self.assertRaises(OSError):
                self.run_block(rpc, root)

    def test_block_trace_failure_is_not_a_revert(self):
        self.check_invalid_block_trace(dict(txHash='0x3', error='execution timeout'))

    def test_wrong_trace_identity_is_rejected(self):
        self.check_invalid_block_trace(dict(txHash='0xwrong', result=RpcFixture([3]).trace(3)))

    def check_invalid_block_trace(self, invalid):
        with TemporaryDirectory() as root:
            rpc = RpcFixture([3, 5])
            row = rpc.receipt_summary()
            traces = rpc.rpc('debug_traceBlockByHash', ['0xabc'])
            traces[3] = invalid
            with patch.object(rpc, 'rpc', return_value=traces):
                with self.assertRaisesRegex(ValueError, 'Failed transaction trace identity/error'):
                    trace_block(row, rpc, Path(root), 'test')
            self.assertFalse((Path(root) / '10.json.gz').exists())

    def test_successful_child_matches(self):
        with TemporaryDirectory() as root:
            result = self.run_block(RpcFixture([2]), root)
            selected = select(result, {'0xaa', '0xbb', '0xcc'})
            self.assertEqual(selected['failed'][0]['matched_contracts'], ['0xaa', '0xbb'])


if __name__ == '__main__':
    unittest.main()
