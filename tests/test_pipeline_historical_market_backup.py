import hashlib
import json
from pathlib import Path
import shutil
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

SCRIPTS = Path(__file__).resolve().parents[1]/'scripts'
sys.path[:0] = [str(SCRIPTS), str(SCRIPTS/'user-history')]
from pipeline.compaction.backup_historical_market import accepted_market, backup_available
from shared.artifacts import checksum, write_json


class HistoricalMarketBackupTests(unittest.TestCase):
    def fixture(self, directory):
        root = directory/'imports'
        address = '0x'+'1'*40
        source = root/address
        source.mkdir(parents=True)
        files = []
        for name, kind, data in [('raw.parquet', 'encoded', b'encoded event'),
                                 ('decoded.parquet', 'decoded', b'decoded event'),
                                 ('state.parquet', 'state', b'block state')]:
            path = source/name
            path.write_bytes(data)
            files.append(dict(path=name, kind=kind, bytes=len(data), sha256=checksum(path)))
        manifest = source/'manifest.json'
        write_json(manifest, dict(plan_sha256='plan', protocol='uniswap-v2', files=files,
                                 identity=dict(address=address, start_block=5, end_block=9)))
        item = dict(address=address, sha256=checksum(manifest), files=len(files),
                    bytes=sum(row['bytes'] for row in files))
        write_json(root/'progress.json', dict(plan_sha256='plan', total_markets=1,
                                             published=[item], through=9))
        return root, item

    def test_scope_mismatch_stops_before_packaging(self):
        with TemporaryDirectory() as directory:
            root, item = self.fixture(Path(directory))
            with self.assertRaisesRegex(ValueError, 'scope differs'):
                accepted_market(root, item, 'wrong-plan', 9)
            with self.assertRaisesRegex(ValueError, 'inventory differs'):
                accepted_market(root, {**item, 'files': 99}, 'plan', 9)

    def test_publishes_after_index_and_all_artifact_types_are_restored(self):
        with TemporaryDirectory() as directory:
            folder = Path(directory)
            root, item = self.fixture(folder)
            remote = folder/'remote'
            uploaded = []

            def copy(base, destination, files, _ops):
                for name in files:
                    path = remote/destination/name
                    path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(base/name, path)
                    uploaded.append(name)

            def readback(path, expected, *, offset=None, count=None, output=None):
                payload = (remote/path).read_bytes()
                if offset is not None:
                    payload = payload[offset:offset+count]
                actual = dict(bytes=len(payload), sha256=hashlib.sha256(payload).hexdigest())
                self.assertEqual(actual, {key: expected[key] for key in actual})
                if output is not None:
                    output.write_bytes(payload)
                return dict(actual, exact=True)

            with (
                patch('hf_flow_cloud._copy_and_check', side_effect=copy),
                patch('hf_flow_cloud._readback', side_effect=readback),
            ):
                first = backup_available(root, folder/'ops', 'archive', 24)
            self.assertEqual(first['state'], 'complete')
            self.assertEqual(len(first['verified']), 1)
            self.assertEqual(uploaded[-1], 'verified.json')
            catalogue = folder/'ops'/item['address']/'verified.json'
            proof = json.loads(catalogue.read_text())
            self.assertEqual(set(proof['restore_probes']), {'pack_index', 'source_manifest', 'encoded', 'decoded', 'state'})
            with patch('hf_flow_cloud._copy_and_check', side_effect=AssertionError('Read-only retry expected')):
                resumed = backup_available(root, folder/'ops', 'archive', 24)
            self.assertEqual(resumed, first)
