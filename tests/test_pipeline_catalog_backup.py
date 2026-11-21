import json
from pathlib import Path
import shutil
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

SCRIPTS = Path(__file__).resolve().parents[1]/'scripts'
sys.path[:0] = [str(SCRIPTS), str(SCRIPTS/'user-history')]
from pipeline.compaction.backup_catalog import backup, backup_latest, descriptor, restore
from pipeline.compaction.store import Store
from shared.artifacts import checksum, write_json


class CatalogBackupTests(unittest.TestCase):
    def test_follower_retries_the_pinned_snapshot_before_a_newer_commit(self):
        with TemporaryDirectory() as temporary:
            node = Path(temporary)/'node'
            work = Path(temporary)/'backup'
            control = node/'compaction.json'

            def advance(through):
                write_json(control, dict(state='complete', canonical_root=str(node/'canonical'), through=through))

            def succeeds(_source, revision, _remote):
                receipt = dict(through=int(revision.name), snapshot_remote=f'archive/{revision.name}')
                write_json(revision/'drive-receipt.json', receipt)
                return receipt

            advance(10)
            with patch('pipeline.compaction.backup_catalog.backup', side_effect=OSError('Drive unavailable')):
                with self.assertRaisesRegex(OSError, 'Drive unavailable'):
                    backup_latest(node, work, 'archive')
            self.assertEqual(json.loads((work/'progress.json').read_text())['pending'], 10)
            advance(12)
            with patch('pipeline.compaction.backup_catalog.backup', side_effect=succeeds):
                first = backup_latest(node, work, 'archive')
                second = backup_latest(node, work, 'archive')
            self.assertEqual((first['verified_through'], second['verified_through']), (10, 12))
            self.assertIsNone(second['pending'])

    def test_catalog_snapshot_round_trip_and_deduplicated_objects(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            primary = root/'node'
            drive = root/'drive'
            files = {'encoded/pool/9.parquet': b'event', 'decoded/pool/9.parquet': b'event',
                     'state/pool/9.parquet': b'state'}
            with Store(primary) as store:
                for relative, value in files.items():
                    path = primary/relative
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(value)
                    with store.db:
                        store.db.execute('insert into files values(?,?,?,?,?,?,?)',
                                         (str(Path(relative).parent), 9, 10, relative, checksum(path), 1, 0))
                with store.db:
                    store.db.execute("insert into meta values('through','9')")

            def copy(base, directory, names, _ops):
                for name in names:
                    target = drive/directory/name
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(base/name, target)

            def readback(name, expected, *, output=None):
                path = drive/name
                self.assertEqual(descriptor(path), expected)
                if output is not None:
                    shutil.copyfile(path, output)
                return dict(expected, exact=True)

            def fetch(source, destination, *options, command='copy'):
                if command == 'copyto':
                    shutil.copyfile(drive/source, destination)
                    return
                names = Path(options[1]).read_text().splitlines()
                destination.mkdir()
                for name in names:
                    shutil.copyfile(drive/source/name, destination/name)

            with (
                patch('hf_flow_cloud._copy_and_check', side_effect=copy),
                patch('hf_flow_cloud._readback', side_effect=readback),
                patch('pipeline.compaction.backup_catalog.fetch', side_effect=fetch),
            ):
                proof = backup(primary, root/'work', 'archive')
                self.assertEqual((proof['through'], proof['payloads'], proof['objects']), (9, 3, 2))
                receipt = root/'work/drive-receipt.json'
                self.assertEqual(json.loads(receipt.read_text()), proof)
                snapshot = proof['snapshot_remote']
                result = restore(snapshot, checksum(receipt), root/'restored')
                self.assertEqual((result['through'], result['files']), (9, 3))
                for path, value in files.items():
                    self.assertEqual((root/'restored'/path).read_bytes(), value)
                first = next((drive/'archive/objects').iterdir())
                first.write_bytes(b'corrupted object')
                with self.assertRaisesRegex(ValueError, 'Restored object differs'):
                    restore(snapshot, checksum(receipt), root/'invalid')
                self.assertFalse((root/'invalid').exists())
