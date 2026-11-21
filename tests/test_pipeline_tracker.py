import sys
import tempfile
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from pipeline.tracker import Tracker, STAGES


class TrackerTests(unittest.TestCase):
    def test_failed_cycle_resumes_same_target_and_advances_only_after_all_stages(self):
        with tempfile.TemporaryDirectory() as root:
            t = Tracker(Path(root))
            t.baseline(100, 'hash100')
            cycle = t.begin(110, 'hash110')
            t.stage(cycle, STAGES[0], 'complete', {'receipt': 'accepted'})
            t.stage(cycle, STAGES[1], 'error', {'error': 'interrupted'})
            t.close()
            t = Tracker(Path(root))
            self.assertEqual(t.active()['end'], 110)
            self.assertEqual(t.position(), (100, 'hash100'))
            with self.assertRaises(ValueError):
                t.finish(cycle)
            for stage in STAGES:
                t.stage(cycle, stage, 'complete', {'receipt': stage})
            t.finish(cycle)
            self.assertEqual(t.position(), (110, 'hash110'))
            self.assertIsNone(t.active())
            self.assertEqual(t.begin(115, 'hash115')['start'], 111)

    def test_no_new_finalized_blocks_do_not_create_cycle(self):
        with tempfile.TemporaryDirectory() as root:
            t = Tracker(Path(root)); t.baseline(100, 'h')
            self.assertIsNone(t.begin(100, 'h'))
            with self.assertRaises(ValueError): t.begin(100, 'different')


if __name__ == '__main__': unittest.main()
