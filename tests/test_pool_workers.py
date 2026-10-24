"""Concurrency controls must preserve accepted coverage and job isolation."""
import shlex
import shutil
import subprocess
import tempfile
import threading
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from orchestration.artifacts import digest
from orchestration.coordinator import Coordinator, remote


class RemoteConcurrencyTests(unittest.TestCase):
    def command(self, root, action='extract', address='a', **options):
        server = dict(root=str(root), host='node', python='python', scripts='/scripts')
        request = dict(action=action, spec=dict(kind='events', address=address), **options)
        with patch('orchestration.coordinator.subprocess.run', return_value=SimpleNamespace(stdout='{}', stderr='')) as run:
            remote(server, request)
        return shlex.split(run.call_args.args[0][-1])

    def test_runtime_limit_is_forwarded_without_changing_job_identity(self):
        low = self.command('/runner', rpc_requests=8)
        high = self.command('/runner', rpc_requests=32)
        self.assertIn('MAX_CONCURRENT_REQUESTS=32', high)
        self.assertEqual(low[:low.index('env')], high[:high.index('env')])
        with self.assertRaises(ValueError):
            self.command('/runner', rpc_requests='32; echo unsafe')

    @unittest.skipUnless(shutil.which('flock'), 'Linux flock required')
    def test_remote_locks_allow_other_pools_but_exclude_duplicate_cleanup(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder) / 'runner'
            command = self.command(root)
            # Keep exactly the production lock prefix, replacing only the worker.
            prefix = command[:command.index('env')]
            holder = subprocess.Popen(prefix + ['sh', '-c', 'echo ready; cat'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
            try:
                self.assertEqual(holder.stdout.readline().strip(), 'ready')
                other = self.command(root, address='b')
                other = other[:other.index('env')] + ['true']
                self.assertEqual(subprocess.run(other).returncode, 0)
                duplicate = self.command(root, action='cleanup')
                duplicate = duplicate[:duplicate.index('python')] + ['true']
                duplicate = ['0' if value == '5' else value for value in duplicate]
                self.assertNotEqual(subprocess.run(duplicate).returncode, 0)
            finally:
                holder.communicate('')


class PoolWorkerTests(unittest.TestCase):
    def test_parallel_workers_own_connections_and_aggregate_failure(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            args = SimpleNamespace(data_root=root, servers=None, contracts=root/'contracts.csv', profiles=None,
                                   lane='events-state', pool_workers=2, rpc_requests=16, cleanup=False)
            rows = [dict(address='a', creation_block=10), dict(address='b', creation_block=10)]
            barrier = threading.Barrier(2)
            connections = set()
            def dataset(child, row, kind, bit, start, stop, hashes):
                connections.add(id(child.tracker.db))
                if bit == 1:
                    barrier.wait(timeout=10)
                if row['address'] == 'a' and bit == 2:
                    child.failed = True
                    return
                spec = dict(address=row['address'], kind=kind, start=start, stop=stop)
                ident = digest(spec)
                child.tracker.job(ident, spec)
                child.tracker.accept(ident, [(row['address'], bit, 'cfg', start, stop)], {'headers':hashes})
            with patch('orchestration.coordinator.registry', return_value=rows), \
                    patch('orchestration.coordinator.fingerprint', return_value='code'), \
                    patch.object(Coordinator, 'dataset', dataset):
                coordinator = Coordinator(args)
                try:
                    coordinator.tracker.run_id = 'production'
                    coordinator.process_batch(10, 12, {'10':'hash10', '11':'hash11'})
                    self.assertTrue(coordinator.failed)
                    self.assertEqual(len(connections), 2)
                    self.assertNotIn(id(coordinator.tracker.db), connections)
                    self.assertEqual(coordinator.tracker.status('a', 10, {1:'cfg', 2:'cfg'}), 1)
                    self.assertEqual(coordinator.tracker.status('b', 10, {1:'cfg', 2:'cfg'}), 3)
                    jobs = coordinator.tracker.db.execute('SELECT run_id FROM monitor_jobs').fetchall()
                    self.assertEqual([row[0] for row in jobs], ['production'] * 3)
                finally:
                    coordinator.tracker.db.close()


if __name__ == '__main__':
    unittest.main()
