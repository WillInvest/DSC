"""Closed-result transfer and authenticated worker selection boundaries."""
import contextlib
import json
import shutil
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from hf_market_replay.dispatch import dispatch
from hf_market_replay.transport import TLSTransport


class FakeFuture:
    def __init__(self, value):
        self.value = value
        self.released = False

    def result(self, timeout):
        return self.value

    def release(self):
        self.released = True


class FakeClient:
    def __init__(self, workers):
        self.workers = workers
        self.closed = False
        self.last_future = None
        self.last_workers = None
        self.cancelled = None

    def scheduler_info(self, **kwargs):
        return dict(workers=self.workers)

    def submit(self, function, *args, workers, allow_other_workers, pure, **kwargs):
        self.last_workers = workers
        self.last_future = FakeFuture(function(*args, **kwargs))
        return self.last_future

    def cancel(self, futures):
        self.cancelled = futures

    def close(self):
        self.closed = True


class FakeFilesystem:
    def __init__(self):
        self.sources = []

    def get_file(self, source, target):
        self.sources.append(source)
        shutil.copyfile(source, target)


class TransportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.host = dict(role='data', root=str(self.root / 'worker'),
                         worker_name='cohort-io-data', worker_host='10.246.103.151')
        Path(self.host['root']).mkdir()
        self.client = FakeClient({'tls://data:123': dict(name='cohort-io-data', host='10.246.103.151')})
        with patch('hf_market_replay.transport.connect', return_value=self.client):
            self.transport = TLSTransport(dict(hosts=[self.host]))

    def tearDown(self):
        self.transport.close()
        self.temp.cleanup()

    def test_status_uses_exact_host_worker_and_releases_future(self):
        root = Path(self.host['root'])
        (root / 'role.json').write_text(json.dumps(dict(plan_path=str(root / 'plan.json'))))
        (root / 'plan.json').write_text(json.dumps(dict(generation='generation', jobs=[])))
        result = self.transport.request(self.host, 'status')
        self.assertEqual(result['generation'], 'generation')
        self.assertEqual(self.client.last_workers, ['tls://data:123'])
        self.assertTrue(self.client.last_future.released)
        self.client.workers['tls://data:123']['host'] = 'another-host'
        with self.assertRaisesRegex(OSError, 'unavailable'):
            self.transport.request(self.host, 'status')

    def test_explicit_assignment_does_not_read_stdin(self):
        root = Path(self.host['root'])
        (root / 'role.json').write_text(json.dumps(dict(plan_path=str(root / 'plan.json'))))
        (root / 'plan.json').write_text(json.dumps(dict(generation='generation', jobs=[dict(id='100-109')])))
        assignment = dict(generation='generation', jobs=['100-109'])
        with patch('sys.stdin', None):
            dispatch(root, 'assign', assignment=assignment)
        self.assertEqual(json.loads((root / 'assignments.json').read_text()), assignment)

    def test_transfer_copies_only_closed_receipt_and_declared_parts(self):
        source = Path(self.host['root']) / 'results/100-109'
        source.mkdir(parents=True)
        (source / 'receipt.json').write_text(json.dumps(dict(parts=[dict(name='aggregates-100-109.parquet')])))
        (source / 'aggregates-100-109.parquet').write_bytes(b'closed aggregate bytes')
        (source / 'unrelated-input.json').write_text('preserve source inputs')
        filesystem = FakeFilesystem()
        self.transport.filesystem = filesystem
        modules = {'dask': types.SimpleNamespace(annotate=lambda **kwargs: contextlib.nullcontext(),
                                                config=types.SimpleNamespace(set=lambda **kwargs: contextlib.nullcontext())),
                   'fsspec': types.SimpleNamespace()}
        destination = self.root / 'received/100-109'
        with patch.dict('sys.modules', modules):
            self.transport.receive(self.host, '100-109', destination)
        self.assertEqual(sorted(path.name for path in destination.iterdir()),
                         ['aggregates-100-109.parquet', 'receipt.json'])
        self.assertEqual((destination / 'aggregates-100-109.parquet').read_bytes(), b'closed aggregate bytes')
        self.assertEqual(len(filesystem.sources), 2)
        self.assertTrue((source / 'unrelated-input.json').exists())

    def test_disappearing_worker_read_times_out_and_releases_future(self):
        future = FakeFuture(None)
        self.client.get = lambda *args, **kwargs: [future]
        self.client.gather = lambda values: self.fail('Timed-out reads must not be gathered')
        modules = {'distributed': types.SimpleNamespace(wait=lambda *args, **kwargs: (_ for _ in ()).throw(TimeoutError())),
                   'distributed.client': types.SimpleNamespace(futures_of=lambda values: values)}
        with patch.dict('sys.modules', modules):
            with self.assertRaises(TimeoutError):
                self.transport._bounded_get({}, ['read'])
        self.assertTrue(future.released)
        self.assertEqual(self.client.cancelled, [future])

    def test_receipt_traversal_is_rejected_before_fetching_other_files(self):
        source = Path(self.host['root']) / 'results/100-109'
        source.mkdir(parents=True)
        (source / 'receipt.json').write_text(json.dumps(dict(parts=[dict(name='../outside.parquet')])))
        filesystem = FakeFilesystem()
        self.transport.filesystem = filesystem
        modules = {'dask': types.SimpleNamespace(annotate=lambda **kwargs: contextlib.nullcontext(),
                                                config=types.SimpleNamespace(set=lambda **kwargs: contextlib.nullcontext())),
                   'fsspec': types.SimpleNamespace()}
        with patch.dict('sys.modules', modules):
            with self.assertRaisesRegex(ValueError, 'filename'):
                self.transport.receive(self.host, '100-109', self.root / 'received/100-109')
        self.assertEqual(len(filesystem.sources), 1)


if __name__ == '__main__':
    unittest.main()
