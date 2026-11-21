import gzip
import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'scripts'))
from pipeline.research_layout import ENCODED, publish_archive, publish_historical_market
from reverted.trace_received import pack
from shared.artifacts import checksum


class ResearchLayoutTests(unittest.TestCase):
    def test_original_logs_have_a_separate_replay_path_and_preserve_their_schema(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            original = root/'ethereum__logs__00000010_to_00000015.parquet'
            pq.write_table(pa.table({'block_number': [11], 'topic0': ['0x1']}), original)
            item = dict(kind='encoded', path=original.name, sha256=checksum(original),
                        bytes=original.stat().st_size)
            publish_historical_market(root, root/'dsc-data',
                                      dict(protocol='aave-v3', identity={'address': '0xa'}, files=[item]),
                                      ENCODED)
            encoded = root/'dsc-data/metadata/replay-inputs/encoded/aave-v3/0xa/10-15.parquet'
            self.assertTrue(encoded.samefile(original))
            self.assertEqual(pq.read_schema(encoded).names, ['block_number', 'topic0'])

    def test_historical_events_and_state_keep_their_rows_and_get_range_names(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            source = root/'source'
            event = source/'ethereum__logs__00000010_to_00000011/schema-v1/Swap.parquet'
            state = source/'state/10-11.parquet'
            event.parent.mkdir(parents=True)
            state.parent.mkdir()
            pq.write_table(pa.table({'block_number': [10], 'name': ['Swap']}), event)
            pq.write_table(pa.table({'block_number': [10, 11], 'state_json': ['{}', '{}']}), state)
            files = [dict(path=str(path.relative_to(source)), kind=kind, bytes=path.stat().st_size,
                          sha256=checksum(path)) for path, kind in ((event, 'decoded'), (state, 'state'))]
            target = root/'dsc-data'
            manifest = dict(protocol='uniswap-v2', identity=dict(address='0x1'), files=files)
            publish_historical_market(source, target, manifest)
            publish_historical_market(source, target, manifest)
            home = target/'dex/uniswapV2'
            saved_event = home/'events/0x1/schema-v1/Swap/10-11.parquet'
            saved_state = home/'block-state/0x1/10-11.parquet'
            self.assertTrue(saved_event.samefile(event))
            self.assertTrue(saved_state.samefile(state))
            self.assertEqual(pq.read_table(saved_event).column('name').to_pylist(), ['Swap'])
            self.assertEqual(pq.read_table(saved_state).schema.names, ['block_number', 'state_json'])

    def test_reverts_keep_full_selection_and_partition_matches_by_protocol(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            source = root/'10-11.jsonl.gz'
            row = dict(block_number=11, failed=[dict(matched_contracts=['0x1', '0x2'], trace={'calls': []})])
            pack(source, [dict(block_number=10, failed=[]), row])
            publish_archive(source, root, 'selected', 10, 12, 10, checksum(source), None)
            self.assertTrue((root/'revert/selected/10-11.jsonl.gz').samefile(source))
            self.assertFalse((root/'dex/uniswapV2/reverted-tx/10-11.jsonl.gz').exists())
            from collections import defaultdict
            addresses = defaultdict(set, {'uniswap-v2': {'0x1'}, 'aave-v3': {'0x2'}})
            publish_archive(source, root, 'selected', 10, 12, 11, checksum(source), addresses)
            def read(path):
                with gzip.open(path, 'rt') as stream:
                    return json.loads(stream.readline())
            self.assertEqual(read(root/'revert/selected/11-11.jsonl.gz'), row)
            dex = read(root/'dex/uniswapV2/reverted-tx/11-11.jsonl.gz')
            lending = read(root/'lending/aaveV3/reverted-tx/11-11.jsonl.gz')
            self.assertEqual(dex['failed'][0]['matched_contracts'], ['0x1'])
            self.assertEqual(lending['failed'][0]['matched_contracts'], ['0x2'])
