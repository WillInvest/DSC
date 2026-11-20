import hashlib
import tempfile
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent/'releases'))
from read_archive import restore
from hf_flow_packing import pack


class ResearchArchiveReaderTests(unittest.TestCase):
    def test_reader_recovers_exact_bytes_and_rejects_wrong_index_and_overwrite(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)/'source'
            base.mkdir()
            data = b'original reconstructed input bytes'
            (base/'input.parquet').write_bytes(data)
            out = Path(tmp)/'archive'
            pack(base, {'input.parquet':dict(bytes=len(data),sha256=hashlib.sha256(data).hexdigest())}, out)
            pin = hashlib.sha256((out/'pack-index.json').read_bytes()).hexdigest()
            target = Path(tmp)/'readback.parquet'
            with self.assertRaisesRegex(ValueError, 'index'):
                restore(out, 'input.parquet', target, '0'*64)
            self.assertTrue(restore(out, 'input.parquet', target, pin)['exact'])
            self.assertEqual(target.read_bytes(), data)
            with self.assertRaises(FileExistsError):
                restore(out, 'input.parquet', target, pin)


if __name__ == '__main__':
    unittest.main()
