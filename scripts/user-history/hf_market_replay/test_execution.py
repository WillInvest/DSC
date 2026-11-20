"""Durable node assignment and duplicate acceptance behavior."""
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from hf_market_replay.ledger import Ledger, LocalResults, digest, load_plan
from hf_market_replay.dispatch import dispatch


class LedgerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'flow.sqlite'
        self.jobs = [dict(id='100-109', start=100, stop=110),
                     dict(id='110-119', start=110, stop=120)]
        self.ledger = Ledger(self.path, 'generation', self.jobs)

    def tearDown(self):
        self.ledger.close()
        self.temp.cleanup()

    def test_offline_assignment_expires_and_online_work_stays_owned(self):
        self.assertEqual(self.ledger.assign('data', 1, now=10, lease_seconds=60), ['100-109'])
        self.assertEqual(self.ledger.assign('node', 1, now=10, lease_seconds=60), ['110-119'])
        self.ledger.renew('node', now=100, lease_seconds=60)
        self.assertEqual(self.ledger.expire(now=100), ['100-109'])
        self.assertEqual(self.ledger.assign('node', 1, now=100, lease_seconds=60), ['100-109'])
        self.assertEqual(len(self.ledger.assigned('node')), 2)

    def test_late_matching_result_is_counted_once_and_conflict_rejected(self):
        self.ledger.assign('data', 1, now=0, lease_seconds=1)
        self.ledger.expire(now=2)
        self.ledger.assign('node', 1, now=2, lease_seconds=60)
        self.assertTrue(self.ledger.accept('100-109', 'same-payload', '/node/receipt', 'node'))
        self.assertFalse(self.ledger.accept('100-109', 'same-payload', '/data/receipt', 'data'))
        with self.assertRaisesRegex(ValueError, 'Conflicting'):
            self.ledger.accept('100-109', 'different-payload', '/data/receipt', 'data')
        self.assertEqual(self.ledger.totals()['accepted_blocks'], 10)
        self.assertEqual(self.ledger.totals()['accepted_jobs'], 1)

    def test_restart_preserves_acceptance_and_rejects_scope_changes(self):
        self.ledger.accept('100-109', 'sha', '/receipt', 'node')
        self.ledger.close()
        self.ledger = Ledger(self.path, 'generation', self.jobs)
        self.assertEqual(self.ledger.assign('node', 2, now=10), ['110-119'])
        with self.assertRaisesRegex(ValueError, 'identity'):
            Ledger(self.path, 'another-generation', self.jobs)
        with self.assertRaisesRegex(ValueError, 'scope'):
            Ledger(self.path, 'generation', self.jobs[:1])

    def test_assignments_never_overlap_without_expiry(self):
        self.assertEqual(self.ledger.assign('node', 2, now=0), ['100-109', '110-119'])
        self.assertEqual(self.ledger.assign('data', 2, now=0), [])

    def test_plan_binds_jobs_and_original_source_pins(self):
        sources = {'native_batch.py': 'pinned-source'}
        identity = dict(scope=dict(start=100, stop=120), jobs_sha256=digest(self.jobs),
                        source_pins_sha256=digest(sources), package_sources={'runner.py': 'pinned-package'})
        plan = dict(identity=identity, generation=digest(identity), jobs=self.jobs, source_pins=sources)
        path = Path(self.temp.name) / 'plan.json'
        path.write_text(json.dumps(plan))
        self.assertEqual(load_plan(path)['generation'], plan['generation'])
        plan['source_pins']['native_batch.py'] = 'changed-source'
        path.write_text(json.dumps(plan))
        with self.assertRaisesRegex(ValueError, 'source identity'):
            load_plan(path)

    def test_local_results_reject_a_different_generation_on_restart(self):
        path = Path(self.temp.name) / 'local.sqlite'
        local = LocalResults(path, 'original-generation')
        local.commit('100-109', 'receipt-sha', 'payload-sha')
        local.close()
        with self.assertRaisesRegex(ValueError, 'generation'):
            LocalResults(path, 'different-generation')

    def test_worker_rejects_a_host_encoding_override(self):
        from hf_market_replay.runner import run_job
        plan = dict(identity=dict(encoding=dict(part_blocks=256)))
        with self.assertRaisesRegex(ValueError, 'encoding'):
            run_job(dict(part_blocks=128), plan, self.jobs[0])

    def test_dispatch_publishes_assignments_and_closed_results_only(self):
        root = Path(self.temp.name)
        plan_path = root / 'plan.json'
        plan_path.write_text(json.dumps(dict(generation='generation', jobs=self.jobs)))
        (root / 'role.json').write_text(json.dumps(dict(plan_path=str(plan_path))))
        assignment = dict(generation='generation', jobs=['100-109'])
        with patch('sys.stdin', io.StringIO(json.dumps(assignment))):
            dispatch(root, 'assign')
        self.assertEqual(json.loads((root / 'assignments.json').read_text()), assignment)
        local = LocalResults(root / 'local.sqlite', 'generation')
        local.commit('100-109', 'receipt-sha', 'payload-sha')
        local.close()
        status = dispatch(root, 'status')
        self.assertEqual(status['completions'][0]['job_id'], '100-109')
        self.assertEqual(dispatch(root, 'status', after=1)['completions'], [])


if __name__ == '__main__':
    unittest.main()
