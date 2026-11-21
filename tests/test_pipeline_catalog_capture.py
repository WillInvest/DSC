from contextlib import closing
from pathlib import Path
import shutil
import sqlite3
import sys
from tempfile import TemporaryDirectory
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from pipeline.compaction.capture import capture_catalog
from pipeline.compaction.reader import snapshot
from pipeline.compaction.verify_capture import verify_capture
from shared.artifacts import checksum


def source_catalog(root):
    root.mkdir()
    (root / 'catalog.lock').touch()
    with closing(sqlite3.connect(root / 'catalog.sqlite')) as db, db:
        db.execute('pragma journal_mode=WAL')
        db.executescript("""
            create table meta(key text primary key, value text);
            insert into meta values ('through', '115');
            create table files(stream text, start integer, stop integer, path text,
                               sha256 text, rows integer, sealed integer);
        """)
        for name, start, stop, sealed in [('closed', 100, 110, 1), ('tail', 110, 116, 0)]:
            payload = root / name
            payload.write_bytes(name.encode())
            db.execute('insert into files values (?, ?, ?, ?, ?, ?, ?)',
                       ('receipts', start, stop, name, checksum(payload), stop - start, sealed))


class CatalogCaptureTests(unittest.TestCase):
    def test_verify_checks_both_materialized_sealed_and_open_payloads(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            source, destination = root / 'live', root / 'capture'
            source_catalog(source)
            capture_catalog(source, destination)
            (destination / 'catalog.lock').touch()
            shutil.copyfile(source / 'closed', destination / 'closed')
            shutil.copyfile(destination / 'open/tail', destination / 'tail')

            result = verify_capture(destination)
            self.assertEqual((result['through'], result['files'], result['payload_bytes']), (115, 2, 10))

            (destination / 'closed').write_bytes(b'broken')
            with self.assertRaisesRegex(ValueError, 'payload checksum mismatch: closed'):
                verify_capture(destination)

    def test_capture_keeps_the_exact_open_tail_and_online_ledger(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            source, destination = root / 'live', root / 'capture'
            source_catalog(source)

            manifest = capture_catalog(source, destination)

            self.assertEqual(manifest['through'], 115)
            self.assertEqual(manifest['catalog_sha256'], checksum(destination / 'catalog.sqlite'))
            self.assertNotIn('captured_path', manifest['files'][0])
            self.assertEqual(manifest['files'][1]['captured_path'], 'open/tail')
            (source / 'tail').unlink()
            self.assertEqual((destination / 'open/tail').read_bytes(), b'tail')
            with snapshot(source) as live:
                self.assertEqual(len(live), 2)
            with closing(sqlite3.connect(destination / 'catalog.sqlite')) as ledger:
                self.assertEqual(ledger.execute('select count(*) from files').fetchone(), (2,))

    def test_capture_does_not_publish_an_invalid_open_payload(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            source, destination = root / 'live', root / 'capture'
            source_catalog(source)
            (source / 'tail').write_bytes(b'changed')

            with self.assertRaisesRegex(ValueError, 'Open payload checksum mismatch'):
                capture_catalog(source, destination)
            self.assertFalse(destination.exists())
