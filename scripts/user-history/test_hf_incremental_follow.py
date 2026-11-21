import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from hf_incremental_follow import follow
from shared.artifacts import checksum, write_json


class IncrementalFollowerTests(unittest.TestCase):
    def test_retry_keeps_the_interval_and_advances_only_after_successor(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            opening, windows, work = root/'opening', root/'windows', root/'work'
            write_json(opening/'boundary.json', {'next_block': 101})
            write_json(root/'plan.json', {'jobs': []})
            helpers = [{'host': 'compute'}, {'host': 'data'}]
            invocations = []

            def run(source, end, *_args, helpers):
                invocations.append((source, end, helpers[0]['host']))
                if len(invocations) == 1:
                    raise RuntimeError('worker interrupted')
                successor = _args[-1]/'successor'/'boundary.json'
                write_json(successor, {'next_block': end+1})
                return dict(next_block=end+1, successor_sha256=checksum(successor))

            def boundary(path, next_block=None):
                report = json.loads((path/'boundary.json').read_text())
                if next_block is not None:
                    self.assertEqual(report['next_block'], next_block)
                return report

            args = (opening, root/'catalog', root, root/'cryo', windows, work, 110, 2, helpers, True)
            with (
                patch.dict(sys.modules, {'hf_incremental_run': SimpleNamespace(run=run)}),
                patch('hf_incremental_follow.validate_opening', side_effect=boundary),
                patch('hf_incremental_follow.catalog_head', side_effect=[102, 110, 110]),
            ):
                with self.assertRaisesRegex(RuntimeError, 'interrupted'):
                    follow(*args)
                saved = json.loads((work/'progress.json').read_text())
                self.assertEqual(saved['pending'], dict(start=101, stop=103))
                self.assertEqual(saved['next_block'], 101)
                first = follow(*args)
                second = follow(*args)
            self.assertEqual((first['next_block'], second['next_block']), (103, 105))
            self.assertEqual(invocations, [(opening, 102, 'compute'), (opening, 102, 'compute'),
                                          (windows/'101-102/successor', 104, 'data')])
