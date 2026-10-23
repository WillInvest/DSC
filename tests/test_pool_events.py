import importlib.util
import tempfile
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location('events', Path(__file__).parents[1] / 'scripts/pool_events.py')
events = importlib.util.module_from_spec(spec)
spec.loader.exec_module(events)


def word(n):
    return format(n % (1 << 256), '064x')


class EventsTest(unittest.TestCase):
    def test_signed_swap_and_exact_integer(self):
        fields = events.EVENTS['Swap']
        log = {'topics': ['topic', '0x' + word(1), '0x' + word(2)],
               'data': '0x' + ''.join(map(word, [-123, 2**200, 2**96, 42, -198])) }
        decoded = events.decode(log, fields)
        self.assertEqual(decoded['amount0'], '-123')
        self.assertEqual(decoded['amount1'], str(2**200))
        self.assertEqual(decoded['tick'], '-198')
        self.assertEqual(decoded['sender'], '0x' + '0'*39 + '1')

    def test_malformed_log_rejected(self):
        with self.assertRaises(ValueError):
            events.decode({'topics': ['topic'], 'data': '0x'}, events.EVENTS['Swap'])

    def test_resume_and_failed_batch(self):
        class RPC:
            fail = False
            changed = False
            def __call__(self, method, params):
                if method == 'eth_getBlockByNumber':
                    n = int(params[0], 16)
                    return {'number': hex(n), 'hash': '0x' + word(n + (100 if self.changed else 0)), 'timestamp': hex(n)}
                if self.fail:
                    raise RuntimeError('offline')
                return []
        rpc = RPC()
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            config = {'schema_version': 1, 'chain_id': 1, 'address': '0x'+'1'*40, 'from_block': 10}
            events.extract(rpc, folder, config, {}, 11, 2)
            before = (folder / 'manifest.json').read_bytes()
            events.extract(rpc, folder, config, {}, 11, 2)
            self.assertEqual(before, (folder / 'manifest.json').read_bytes())
            rpc.fail = True
            with self.assertRaises(RuntimeError):
                events.extract(rpc, folder, config, {}, 13, 2)
            self.assertEqual(before, (folder / 'manifest.json').read_bytes())
            rpc.fail = False
            rpc.changed = True
            with self.assertRaisesRegex(ValueError, 'hash'):
                events.extract(rpc, folder, config, {}, 13, 2)


if __name__ == '__main__':
    unittest.main()
