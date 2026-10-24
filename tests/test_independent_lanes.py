import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from orchestration.coordinator import Coordinator
from orchestration.tracker import Tracker
from orchestration.locking import file_lock


class LaneTests(unittest.TestCase):
    def test_lane_locks_allow_siblings_but_exclude_duplicate_and_legacy(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)
            with file_lock(root/'coordinator.lock',shared=True,blocking=False), file_lock(root/'events-state.lock',blocking=False):
                with file_lock(root/'coordinator.lock',shared=True,blocking=False), file_lock(root/'reverted.lock',blocking=False):
                    pass
                with self.assertRaises(BlockingIOError):
                    with file_lock(root/'events-state.lock',blocking=False): pass
                with self.assertRaises(BlockingIOError):
                    with file_lock(root/'coordinator.lock',blocking=False): pass

    def test_one_finished_lane_does_not_finish_combined_run(self):
        with tempfile.TemporaryDirectory() as folder:
            a=Tracker(Path(folder)/'tracker.sqlite');b=Tracker(Path(folder)/'tracker.sqlite')
            self.addCleanup(a.db.close);self.addCleanup(b.db.close)
            with a.db:a.db.execute('INSERT INTO runs VALUES(?,?,0)',('run','{}'))
            a.start_lane('run','events-state');b.start_lane('run','reverted')
            a.finish_lane('run','events-state')
            self.assertEqual(b.db.execute('SELECT finished FROM runs').fetchone()[0],0)
            b.finish_lane('run','reverted')
            self.assertEqual(a.db.execute('SELECT finished FROM runs').fetchone()[0],1)

    def test_concurrent_acceptance_preserves_both_dataset_flags(self):
        from concurrent.futures import ThreadPoolExecutor
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'tracker.sqlite'
            def write(bit):
                tracker=Tracker(path)
                try:
                    for block in range(10,30):
                        ident=f'{bit}-{block}'
                        tracker.job(ident, {'start':block,'stop':block+1})
                        tracker.accept(ident,[('address',bit,'cfg',block,block+1)],
                                       {'headers':{str(block):f'hash-{block}'}})
                finally:
                    tracker.db.close()
            with ThreadPoolExecutor(max_workers=2) as pool:
                list(pool.map(write,[1,2]))
            tracker=Tracker(path)
            try:
                self.assertTrue(all(tracker.status('address',n,{1:'cfg',2:'cfg'})==3 for n in range(10,30)))
                self.assertEqual(tracker.db.execute('SELECT count(*) FROM chain_blocks').fetchone()[0],20)
            finally:
                tracker.db.close()

    def test_dataset_lane_never_waits_for_trace(self):
        c=Coordinator.__new__(Coordinator)
        c.lane='events-state';c.rows=[{'address':'a','creation_block':1}]
        c.dataset=Mock();c.reverted=Mock(side_effect=AssertionError('tracing must not run in this lane'))
        c.process_batch(10,20,{})
        self.assertEqual([call.args[1] for call in c.dataset.call_args_list],['events','state'])
        c.reverted.assert_not_called()

    def test_trace_lane_does_not_run_events_or_state(self):
        c=Coordinator.__new__(Coordinator)
        c.lane='reverted';c.rows=[{'address':'a','creation_block':1}]
        c.dataset=Mock(side_effect=AssertionError('dataset lane must run separately'));c.reverted=Mock()
        c.process_batch(10,20,{})
        c.dataset.assert_not_called();c.reverted.assert_called_once()

    def test_serial_run_retires_old_split_lane_monitor_files(self):
        from unittest.mock import patch
        import run
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            files = [root / f'{kind}-{lane}.json'
                     for kind in ('heartbeat', 'progress') for lane in ('events-state', 'reverted')]
            for path in files:
                path.write_text('{}')
            argv = ['run.py', '--contracts', 'contracts.csv', '--data-root', folder]
            with patch('sys.argv', argv), patch('run.Coordinator'):
                run.main()
            self.assertFalse(any(path.exists() for path in files))
            self.assertTrue((root / 'heartbeat.json').exists())
