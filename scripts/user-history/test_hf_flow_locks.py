"""Readers and verified cleanup cooperate through immutable job lock files."""
import fcntl
import multiprocessing
import os
import tempfile
import unittest
import types
from unittest.mock import patch
from pathlib import Path

from hf_flow_retire import job_locks


def hold_reader(directory, ready, release):
    with job_locks([Path(directory)], exclusive=False):
        ready.set()
        release.wait(10)


class JobLocks(unittest.TestCase):
    def test_exclusive_cleanup_waits_for_another_process_reader(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root) / 'job'
            directory.mkdir()
            with job_locks([directory], exclusive=True):
                pass
            context = multiprocessing.get_context('spawn')
            ready, release = context.Event(), context.Event()
            child = context.Process(target=hold_reader, args=(str(directory), ready, release))
            child.start()
            try:
                self.assertTrue(ready.wait(5))
                with (directory / 'flow.lock').open('a') as handle:
                    with self.assertRaises(BlockingIOError):
                        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                release.set()
                child.join(5)
                self.assertEqual(child.exitcode, 0)
                with job_locks([directory], exclusive=True):
                    self.assertTrue((directory / 'flow.lock').exists())
            finally:
                release.set()
                if child.is_alive():
                    child.terminate()
                child.join()

    def test_new_lock_matches_directory_owner_and_existing_lock_is_unchanged(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            with job_locks([directory], exclusive=True):
                lock = directory / 'flow.lock'
                before = lock.stat()
                self.assertEqual((before.st_uid, before.st_gid), (directory.stat().st_uid, directory.stat().st_gid))
            os.chmod(lock, 0o640)
            with job_locks([directory, directory], exclusive=False):
                after = lock.stat()
                self.assertEqual(after.st_ino, before.st_ino)
                self.assertEqual(after.st_mode & 0o777, 0o640)

    def test_audit_keeps_read_lock_through_callback_and_releases_on_error(self):
        from audit_hf_flow import locked_remote_audit
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            receipt = directory / 'receipt.json'
            receipt.write_text('{}')
            def audit(payload):
                with (directory / 'flow.lock').open('a') as handle:
                    with self.assertRaises(BlockingIOError):
                        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                raise ValueError('Simulated audit failure')
            module = types.SimpleNamespace(remote_audit=audit)
            with patch.dict('sys.modules', {'audit_hf_f64': module}):
                with self.assertRaisesRegex(ValueError, 'Simulated audit failure'):
                    locked_remote_audit({'receipts': [{'path': str(receipt)}]})
            with (directory / 'flow.lock').open('a') as handle:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)


    def test_late_helper_transfer_is_not_hidden_by_a_newer_node_rowid(self):
        import json
        from audit_hf_flow import select_new_rows
        rows=[(10,'helper','data',json.dumps({'sha256':'new-helper'}),None),(100,'node','node',json.dumps({'sha256':'same-node'}),None)]
        prior={'rowid':100,'receipt_sha256_by_role':{'data':'old-helper','node':'same-node'}}
        self.assertEqual([r[1] for r in select_new_rows(rows,prior)],['helper'])

    def test_cleanup_before_audit_returns_recoverable_missing_without_rpc(self):
        import json
        from audit_hf_flow import locked_remote_audit
        from shared.artifacts import checksum
        with tempfile.TemporaryDirectory() as root:
            receipt=Path(root)/'receipt.json';receipt.write_text(json.dumps({'job_id':'1-2','parts':[{'path':str(Path(root)/'retired.parquet')}],'aggregate_parts':[]}))
            def forbidden(payload):self.fail('Node RPC audit invoked before files were restored')
            with patch.dict('sys.modules',{'audit_hf_f64':types.SimpleNamespace(remote_audit=forbidden)}):
                result=locked_remote_audit({'receipts':[{'path':str(receipt),'sha256':checksum(receipt)}]})
            self.assertEqual(result['missing_jobs'],['1-2']);self.assertEqual(result['extra_node_rpc'],0)

    def test_packing_reserves_capacity_seen_by_numerical_dispatch(self):
        from hf_flow_io import node_capacity,capacity_available
        from unittest.mock import patch
        import json
        with tempfile.TemporaryDirectory() as root:
            cfg={'role':'node','generation':'g','node_flow_root':root,'reserve_bytes':100,'slots':2,'inflight_bytes_per_slot':10}
            with patch('shutil.disk_usage',return_value=types.SimpleNamespace(free=1000)):
                with node_capacity(cfg,reserve_bytes=500):
                    self.assertEqual(capacity_available(cfg,root),500)
                    with (Path(root)/'capacity.lock').open('a') as writer:
                        with self.assertRaises(BlockingIOError):fcntl.flock(writer,fcntl.LOCK_EX|fcntl.LOCK_NB)
                self.assertEqual(capacity_available(cfg,root),1000)
            self.assertFalse((Path(root)/'capacity-reservation.json').exists())

    def test_large_batch_keeps_every_lock_when_soft_descriptor_limit_is_small(self):
        import resource
        before=resource.getrlimit(resource.RLIMIT_NOFILE)
        with tempfile.TemporaryDirectory() as root:
            directories=[Path(root)/str(i) for i in range(100)]
            for directory in directories:directory.mkdir()
            try:
                resource.setrlimit(resource.RLIMIT_NOFILE,(64,before[1]))
                with job_locks(directories,exclusive=True):
                    for directory in [directories[0],directories[-1]]:
                        with (directory/'flow.lock').open('a') as handle:
                            with self.assertRaises(BlockingIOError):fcntl.flock(handle,fcntl.LOCK_EX|fcntl.LOCK_NB)
                self.assertEqual(resource.getrlimit(resource.RLIMIT_NOFILE),(64,before[1]))
            finally:resource.setrlimit(resource.RLIMIT_NOFILE,before)

    def test_symlink_lock_is_refused(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            target = directory / 'other'
            target.touch()
            (directory / 'flow.lock').symlink_to(target)
            with self.assertRaises(OSError):
                with job_locks([directory], exclusive=True):
                    self.fail('Symlink lock acquired')


if __name__ == '__main__':
    unittest.main()
