import json
import fcntl
import shutil
import tempfile
import unittest
from pathlib import Path

import direct_hf_archive as archive
from shared.artifacts import checksum


class DirectArchiveTest(unittest.TestCase):
    def test_upload_preserves_originals_and_rejects_changed_remote(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder) / 'source'; root.mkdir()
            for name in ['parts-0000.parquet', 'aggregate_parts-0000.parquet', 'users.parquet']:
                (root / name).write_bytes((name * 10).encode())
            files = {n: {'bytes': p.stat().st_size, 'sha256': checksum(p)} for p in root.iterdir() for n in [p.name]}
            value = archive.describe_source(dict(id='test', files=files, generation='g', rows=3), root)
            config = Path(folder) / 'rclone.conf'; config.write_text('')
            remote = str(Path(folder) / 'remote')
            result = archive.upload_direct(value, tool=shutil.which('rclone'), config=str(config), remote=remote, drive_root_id=None)
            self.assertTrue(result['all_remote_size_md5_exact'])
            self.assertEqual(len(result['restore_probes']), 3)
            for name, item in files.items():
                self.assertEqual(checksum(root / name), item['sha256'])
                self.assertEqual(checksum(Path(remote) / 'batches/test' / name), item['sha256'])
            (Path(remote) / 'batches/test/parts-0000.parquet').write_bytes(b'conflicting remote')
            with self.assertRaises(Exception):
                archive.upload_direct(value, tool=shutil.which('rclone'), config=str(config), remote=remote, drive_root_id=None)
            self.assertEqual(checksum(root / 'parts-0000.parquet'), files['parts-0000.parquet']['sha256'])

    def test_source_change_and_escape_rejected_before_upload(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); p = root / 'data'; p.write_bytes(b'original')
            payload = dict(id='test', generation='g', rows=1, files={'data': dict(bytes=8, sha256=checksum(p))})
            p.write_bytes(b'changed!')
            with self.assertRaises(ValueError): archive.describe_source(payload, root)
            payload['files'] = {'../outside': dict(bytes=1, sha256='x')}
            with self.assertRaises(ValueError): archive.describe_source(payload, root)

    def test_concurrent_batch_upload_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            with (root / 'upload.lock').open('a') as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                with self.assertRaises(BlockingIOError):
                    archive.upload_direct(dict(source_base=folder, relative='manifest.json'))


if __name__ == '__main__': unittest.main()
