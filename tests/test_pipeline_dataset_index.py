from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'scripts'))
from pipeline.dataset_index import FORMAT, build


class DatasetIndexTests(unittest.TestCase):
    def test_index_tracks_exact_evidence_paths_and_marks_unavailable_drive_receipts(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            manifest, accepted, drive = (root/name for name in ('manifest.json', 'accepted.json', 'drive.json'))
            manifest.write_text('{}')
            accepted.write_text('{}')
            references = [dict(role='catalog', path=str(manifest)),
                          dict(role='node_acceptance', path=str(accepted)),
                          dict(role='drive_acceptance', path=str(drive), pending=True)]
            item = dict(id='accepted-market', layers=['state/ethereum'], layout='SQLite and Parquet',
                        node=str(root), drive='drive:market', evidence=references)
            configuration = dict(format=FORMAT, chain_id=1, releases=[item])
            result = build(configuration)['releases'][0]['evidence']
            self.assertEqual(result[0]['bytes'], 2)
            self.assertEqual(result[2]['availability'], 'pending')
            drive.write_text('{}')
            self.assertEqual(build(configuration)['releases'][0]['evidence'][2]['availability'], 'present')
            accepted.unlink()
            with self.assertRaises(FileNotFoundError):
                build(configuration)
