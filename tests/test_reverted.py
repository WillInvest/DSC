import copy
import importlib.util
from pathlib import Path
import tempfile
import unittest
import sys
ROOT = Path(__file__).resolve().parents[1]
if ROOT.name == 'scripts':
    ROOT = ROOT.parent
sys.path.insert(0, str(ROOT / 'scripts'))
from reverted.extract_reverted import choose, extract_block, load, validate_archive
from reverted.select_reverted import called_addresses


class Node:
    def __init__(self, failed=()):
        self.header = {'number': '0xa', 'hash': '0xabc', 'transactions': [f'0x{i}' for i in range(6)]}
        self.receipts = [{'status': hex(int(i not in failed)), 'transactionIndex': hex(i),
                          'transactionHash': f'0x{i}', 'blockHash': '0xabc', 'blockNumber': '0xa'} for i in range(6)]
        self.calls = []
        self.fail = None
        self.reorg = False

    def trace(self, i):
        return {'type': 'CALL', 'from': '0xa', 'to': '0xb', 'gas': '0x10', 'gasUsed': '0x5',
                'input': '0x', **({'error': 'execution reverted'} if self.receipts[i]['status'] == '0x0' else {})}

    def __call__(self, method, params):
        self.calls.append(method)
        if method == 'eth_getBlockByNumber':
            h = copy.deepcopy(self.header)
            if self.reorg and self.calls.count(method) > 1:
                h['hash'] = '0xchanged'
            return h
        if method == 'eth_getBlockReceipts':
            return self.receipts
        if method == 'debug_traceTransaction':
            if self.fail == params[0]:
                raise RuntimeError('RPC failed')
            return self.trace(int(params[0], 16))
        if method == 'debug_traceBlockByNumber':
            return [{'txHash': f'0x{i}', 'result': self.trace(i)} for i in range(6)]
        raise AssertionError(method)


class ExtractionTests(unittest.TestCase):
    def run_block(self, node, root, multiplier=1):
        return extract_block(10, Path(root), multiplier, node, 1, 'test')

    def test_skip_and_resume(self):
        with tempfile.TemporaryDirectory() as root:
            node = Node()
            self.assertEqual(self.run_block(node, root)['strategy'], 'skip')
            self.assertTrue(self.run_block(node, root)['resumed'])
            self.assertFalse(any(m.startswith('debug') for m in node.calls))

    def test_individual_and_boundary(self):
        with tempfile.TemporaryDirectory() as root:
            node = Node([5])
            self.assertEqual(self.run_block(node, root)['strategy'], 'individual')
            self.assertEqual(node.calls.count('debug_traceTransaction'), 1)

    def test_block_discards_successful_traces(self):
        with tempfile.TemporaryDirectory() as root:
            node = Node([3, 5])
            self.assertEqual(self.run_block(node, root)['strategy'], 'block')
            payload = validate_archive(load(next(Path(root).glob('blocks/*/*.gz'))))
            self.assertEqual(len(payload['failed']), 2)
            self.assertEqual(node.calls.count('debug_traceBlockByNumber'), 1)

    def test_incomplete_receipts_fail(self):
        with tempfile.TemporaryDirectory() as root:
            node = Node([2])
            node.receipts.pop()
            with self.assertRaises(ValueError):
                self.run_block(node, root)
            self.assertFalse(list(Path(root).glob('blocks/*/*.gz')))

    def test_interrupted_individual_resumes_checkpoint(self):
        with tempfile.TemporaryDirectory() as root:
            node = Node([0, 2])
            node.fail = '0x2'
            with self.assertRaises(RuntimeError):
                self.run_block(node, root)
            node.fail = None
            node.calls.clear()
            self.run_block(node, root)
            self.assertEqual(node.calls.count('debug_traceTransaction'), 1)

    def test_hash_change_does_not_commit(self):
        with tempfile.TemporaryDirectory() as root:
            node = Node([2])
            node.reorg = True
            with self.assertRaises(ValueError):
                self.run_block(node, root)
            self.assertFalse(list(Path(root).glob('blocks/*/*.gz')))

    def test_corrupt_archive_fails_resume(self):
        with tempfile.TemporaryDirectory() as root:
            node = Node([2])
            self.run_block(node, root)
            next(Path(root).glob('blocks/*/*.gz')).write_bytes(b'broken')
            with self.assertRaises(Exception):
                self.run_block(node, root)

    def test_rpc_trace_error_is_not_a_revert(self):
        with tempfile.TemporaryDirectory() as root:
            node = Node([3, 5])
            def call(method, params):
                result = node(method, params)
                if method == 'debug_traceBlockByNumber':
                    result[3] = {'error': 'execution timeout'}
                return result
            with self.assertRaises(ValueError):
                self.run_block(call, root)
            self.assertFalse(list(Path(root).glob('blocks/*/*.gz')))

    def test_wrong_trace_identity_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            node = Node([3, 5])
            def call(method, params):
                result = node(method, params)
                if method == 'debug_traceBlockByNumber':
                    result[3]['txHash'] = '0xwrong'
                return result
            with self.assertRaises(ValueError):
                self.run_block(call, root)

    def test_successful_child_matches(self):
        trace = {'to': '0xAA', 'error': 'revert', 'calls': [{'to': '0xBB'}]}
        self.assertEqual(called_addresses(trace), {'0xaa', '0xbb'})


if __name__ == '__main__':
    unittest.main()
