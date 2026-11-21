from pathlib import Path
import shutil
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from hf_flow_packing import pack
from hf_incremental_archive import follow, verify_download
from shared.artifacts import checksum, write_json


class IncrementalArchiveTests(unittest.TestCase):
    def test_verifies_all_restored_members_and_joins_the_restart_state(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            inputs, published = root/'inputs', root/'drive'
            wallets = inputs/'result/wallets.json'
            state = inputs/'successor/state.json.gz'
            state.parent.mkdir(parents=True)
            state.write_bytes(b'sealed state')
            write_json(wallets, [['wallet', 8]])
            write_json(inputs/'successor/boundary.json', dict(
                next_block=10, checkpoint={'sha256': checksum(state)},
                input_identity={'wallet_file_sha256': checksum(wallets)}))
            files = {str(p.relative_to(inputs)): dict(bytes=p.stat().st_size, sha256=checksum(p))
                     for p in inputs.rglob('*') if p.is_file()}
            index = pack(inputs, files, published, target_bytes=20)
            index_path, manifest = published/'pack-index.json', published/'manifest.json'
            write_json(manifest, {'stop': 10, 'files': files})

            def readback(name, expected, *, output):
                path = published/name.removeprefix('drive/')
                if checksum(path) != expected['sha256']:
                    raise ValueError('Drive payload differs')
                self.assertEqual(path.stat().st_size, expected['bytes'])
                shutil.copyfile(path, output)

            verify_download('drive', index, index_path, manifest, root, readback)
            package = published/next(iter(index['packs']))
            package.write_bytes(b'wrong')
            with self.assertRaisesRegex(ValueError, 'Drive payload differs'):
                verify_download('drive', index, index_path, manifest, root, readback)

    def test_follower_holds_position_until_the_next_window_is_accepted(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            windows, work = root/'windows', root/'backup'
            write_json(windows/'11-12/progress.json', {'stage': 'locally_verified'})
            with patch('hf_incremental_archive.backup_window') as backup:
                first = follow(windows, work, 'drive', 10, 12, once=True)
                self.assertEqual(first['state'], 'waiting_for_window')
                self.assertEqual(first['next_block'], 10)
                backup.assert_not_called()
                write_json(windows/'10-10/progress.json', {'stage': 'locally_verified'})
                backup.return_value = dict(start=10, stop=11)
                second = follow(windows, work, 'drive', 10, 12, once=True)
                self.assertEqual(second['next_block'], 11)
                backup.return_value = dict(start=11, stop=13)
                third = follow(windows, work, 'drive', 10, None, once=True)
                self.assertEqual(third['next_block'], 13)
                next_window = follow(windows, work, 'drive', 10, None, once=True)
                self.assertEqual(next_window['state'], 'waiting_for_window')
