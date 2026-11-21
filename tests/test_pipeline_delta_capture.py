from pathlib import Path
import shutil
import sys
from tempfile import TemporaryDirectory
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'scripts'))
from pipeline.compaction.capture import capture_catalog
from pipeline.compaction.delta_capture import materialize, prepare, summary
from pipeline.compaction.store import Store
from shared.artifacts import checksum


class DeltaCaptureTests(unittest.TestCase):
    def publish(self, root, through, rows):
        with Store(root) as store:
            with store.db:
                store.db.execute('delete from files')
                store.db.execute("insert or replace into meta values('through',?)", (str(through),))
                for relative, start, stop, sealed, contents in rows:
                    path = root/relative
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(contents)
                    store.db.execute('insert into files values(?,?,?,?,?,?,?)',
                                     ('test', start, stop, relative, checksum(path), 1, sealed))

    def test_reuses_equal_bytes_and_transfers_pinned_open_and_changed_sealed_payloads(self):
        with TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            live, prior, capture, incoming = (root/name for name in ('live', 'prior', 'capture', 'sealed'))
            previous = [('test/1-2', 1, 3, 1, b'history'), ('test/3-3', 3, 4, 0, b'open')]
            self.publish(live, 3, previous)
            capture_catalog(live, prior)
            for relative, *_ in previous:
                (prior/relative).parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(live/relative, prior/relative)
            (prior/'catalog.lock').touch()
            self.publish(live, 5, [previous[0], ('test/3-4', 3, 5, 1, b'sealed'),
                                 ('test/5-5', 5, 6, 0, b'new tail')])
            capture_catalog(live, capture)
            plan = prepare(prior, capture, root/'work')
            self.assertEqual(summary(plan)['families'], {
                'reuse': {'files': 1, 'bytes': 7}, 'open': {'files': 1, 'bytes': 8},
                'sealed': {'files': 1, 'bytes': 6},
            })
            self.assertEqual((root/'work/open.txt').read_text(), 'open/test/5-5\n')
            self.assertEqual((root/'work/sealed.txt').read_text(), 'test/3-4\n')
            (incoming/'test').mkdir(parents=True)
            shutil.copyfile(live/'test/3-4', incoming/'test/3-4')
            output = root/'new-import'
            (live/'test/5-5').write_bytes(b'live changed after capture')
            verified = materialize(prior, capture, incoming, root/'work', output)
            self.assertEqual((verified['through'], verified['files']), (5, 3))
            self.assertEqual((output/'test/5-5').read_bytes(), b'new tail')
            self.assertEqual((output/'test/1-2').stat().st_ino, (prior/'test/1-2').stat().st_ino)
            self.assertEqual(materialize(prior, capture, incoming, root/'work', output), verified)

    def test_corrupt_transferred_payload_cannot_be_published(self):
        with TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            live, prior, capture = (root/name for name in ('live', 'prior', 'capture'))
            self.publish(live, 2, [])
            capture_catalog(live, prior)
            self.publish(live, 3, [('test/3-3', 3, 4, 0, b'tail')])
            capture_catalog(live, capture)
            prepare(prior, capture, root/'work')
            (capture/'open/test/3-3').write_bytes(b'fake')
            with self.assertRaisesRegex(ValueError, 'checksum mismatch'):
                materialize(prior, capture, root/'sealed', root/'work', root/'output')
            self.assertFalse((root/'output').exists())
