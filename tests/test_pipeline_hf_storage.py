from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'scripts'))
from pipeline.hf_storage import ARCHIVE_FORMAT, publish
from shared.artifacts import checksum, write_json


class HFStorageTests(unittest.TestCase):
    def test_places_only_the_contiguous_restore_verified_window(self):
        with TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            windows, backup, target = (root/name for name in ('source', 'backup', 'target'))
            target.mkdir()
            source, evidence = windows/'10-11', backup/'10-11'
            part = source/'result/wallets.json'
            write_json(part, [])
            write_json(source/'successor/wallets.json', [])
            write_json(source/'progress.json', dict(stage='locally_verified', start=10, stop=12,
                                                     result={'sha256': 'result'}, successor_sha256='next'))
            manifest, index = evidence/'manifest.json', evidence/'pack-index.json'
            write_json(manifest, dict(format=ARCHIVE_FORMAT, start=10, stop=12, result_sha256='result',
                                      successor_sha256='next', files={'result/wallets.json': {
                                          'bytes': part.stat().st_size, 'sha256': checksum(part)}},
                                      derived_files={'successor/wallets.json': 'result/wallets.json'}))
            write_json(index, {})
            verified = evidence/'verified.json'
            write_json(verified, dict(format=ARCHIVE_FORMAT, start=10, stop=12, full_drive_restore_exact=True,
                                      manifest_sha256=checksum(manifest), index_sha256=checksum(index)))
            write_json(backup/'progress.json', dict(identity=dict(windows=str(windows), start=10), next_block=12))
            report = publish(windows, backup, target)
            self.assertEqual(report['through'], 11)
            self.assertEqual(publish(windows, backup, target), report)
            self.assertTrue((target/'windows/10-11/result/wallets.json').samefile(part))
            self.assertTrue((target/'windows/10-11/archive/verified.json').samefile(verified))
            write_json(backup/'progress.json', dict(identity=dict(windows=str(windows), start=10), next_block=13))
            with self.assertRaisesRegex(ValueError, 'coverage has a gap'):
                publish(windows, backup, target)
