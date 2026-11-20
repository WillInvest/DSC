"""Backup acceptance rejects corruption, path escapes and existing conflicts."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import backup_hf as b


class BackupTests(unittest.TestCase):
    def test_expired_source_cleanup_removes_secret_without_stopping_other_units(self):
        with tempfile.TemporaryDirectory() as tmp:
            secret = Path(tmp) / 'hf-failover-20261115/backup-checkpoints/check/rsync-secret'
            secret.parent.mkdir(parents=True); secret.write_text('test-only')
            result = b.subprocess.CompletedProcess([], 1, 'not-found\n', '')
            with patch.dict(b.HOMES, {'node': tmp}), patch.object(b.subprocess, 'run', return_value=result) as run:
                self.assertTrue(b.cleanup('node', 'check')['secret_removed'])
                self.assertEqual(run.call_count, 1)
            self.assertFalse(secret.exists())

    def test_cleanup_still_removes_secret_if_stop_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            secret = Path(tmp) / 'hf-failover-20261115/backup-checkpoints/check/rsync-secret'
            secret.parent.mkdir(parents=True); secret.write_text('test-only')
            result = b.subprocess.CompletedProcess([], 0, 'loaded\n', '')
            with patch.dict(b.HOMES, {'node': tmp}), patch.object(b.subprocess, 'run', side_effect=[result, b.subprocess.CalledProcessError(1, ['stop'])]):
                with self.assertRaises(b.subprocess.CalledProcessError): b.cleanup('node', 'check')
            self.assertFalse(secret.exists())

    def test_active_pointer_preserves_old_plan_and_rejects_mutation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); old = root / 'plan.json'; old.write_text('{"generation":"old"}')
            self.assertEqual(b.active_manifest(root), old)
            new = root / 'new.json'; new.write_text('{"generation":"new"}')
            (root / 'active-plan.json').write_text(json.dumps(dict(path=str(new), generation='new', sha256=b.checksum(new))))
            self.assertEqual(b.active_manifest(root), new)
            new.write_text('{"generation":"changed"}')
            with self.assertRaises(ValueError): b.active_manifest(root)
            self.assertEqual(json.loads(old.read_text())['generation'], 'old')

    def test_recovered_receipt_mapping_is_explicit_and_owned(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp) / 'hf-failover-20261115'
            own = base / 'state/receipts/old/job.json'
            self.assertEqual(b.owned_receipt(base, tmp, own), own)
            self.assertEqual(b.owned_receipt(base, tmp, '/home/dsc-data/worker-state/receipts/old/job.json'), Path(tmp) / b.WORK / 'state/receipts/old/job.json')
            with self.assertRaises(ValueError): b.owned_receipt(base, tmp, Path(tmp) / 'other.json')

    def test_manifest_verification_detects_corruption(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); (root / 'accepted').write_bytes(b'original')
            manifest = dict(role='node', new_rows=2, recovered_rows=3,
                            files={'accepted': dict(sha256=b.checksum(root / 'accepted'), bytes=8)})
            (root / 'manifest.json').write_text(json.dumps(manifest))
            digest = b.checksum(root / 'manifest.json')
            self.assertTrue(b.verify(root, 'manifest.json', digest)['all_exact'])
            (root / 'accepted').write_bytes(b'changed!')
            with self.assertRaises(ValueError):
                b.verify(root, 'manifest.json', digest)

    def test_incremental_selection_preserves_conflicting_backup(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); (root / 'accepted').write_bytes(b'original')
            payload = dict(relative='manifest.json', sha256='1' * 64, manifest_bytes=10,
                           manifest=dict(files={'accepted': dict(sha256=b.checksum(root / 'accepted'), bytes=8)}))
            with patch.object(b.shutil, 'disk_usage', return_value=type('Disk', (), {'free': 100 * 2**30})()):
                self.assertEqual(b.needed(root, payload)['names'], ['manifest.json'])
                (root / 'accepted').write_bytes(b'changed!')
                with self.assertRaises(ValueError):
                    b.needed(root, payload)
                self.assertEqual((root / 'accepted').read_bytes(), b'changed!')

    def test_incremental_manifest_cannot_escape_archive(self):
        with tempfile.TemporaryDirectory() as tmp:
            payload = dict(relative='manifest.json', sha256='1' * 64, manifest_bytes=10,
                           manifest=dict(files={'../escape': dict(sha256='2' * 64, bytes=8)}))
            with self.assertRaises(ValueError):
                b.needed(tmp, payload)

    def test_receive_sends_only_missing_paths_and_preserves_existing_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp); (home / 'hf-failover-20261115').mkdir()
            payload = dict(source='node', source_ip='127.0.0.1', password='test-only',
                           relative='manifest.json', sha256='1' * 64)
            def run(command, check):
                self.assertIn('--ignore-existing', command)
                self.assertNotIn('--checksum', command)
                path = Path(next(x.split('=', 1)[1] for x in command if x.startswith('--files-from=')))
                self.assertEqual(path.read_bytes(), b'manifest.json\0nested/new.parquet\0')
                self.assertTrue((home / 'hf-failover-20261115/backup-password-node').exists())
            with patch.dict(b.HOMES, {'revert': str(home)}), \
                 patch.object(b, 'needed', return_value=dict(names=['nested/new.parquet', 'manifest.json'], transfer_bytes=50)), \
                 patch.object(b, 'verify', return_value=dict(all_exact=True)), \
                 patch.object(b.subprocess, 'run', side_effect=run):
                self.assertTrue(b.receive('revert', payload)['all_exact'])
            self.assertFalse((home / 'hf-failover-20261115/backup-password-node').exists())
            self.assertFalse((home / 'hf-failover-20261115/backup-missing-node').exists())


if __name__ == '__main__':
    unittest.main()
