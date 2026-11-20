import copy
import unittest
from resume_hf import resume_plan


class ResumeTests(unittest.TestCase):
    def fixture(self):
        identity = dict(all_users=True, block_step=1, cutoff=12, factor_generation='factor',
                        max_rows_per_job=10, row_group_rows=100000, snapshot_manifest_sha256='image')
        jobs = [dict(id='a', start=2, stop=7, expected_rows=6), dict(id='b', start=7, stop=13, expected_rows=10)]
        old = dict(identity=identity, generation='old', jobs=jobs,
                   assignments=dict(a='node', b='web'), recovered={}, coverage=copy.deepcopy(jobs))
        r = dict(job_id='a', start=2, stop=7, rows=6, generation='old', factor_generation='factor',
                 all_exact=True, mismatches=0, sha256='output', owner_first_seen_sha256='owners')
        inv = [dict(role='node', valid=[dict(receipt=r, path='/node/r.json', receipt_sha256='receipt')], excluded=[])]
        return old, inv

    def test_preserves_exact_population_and_canary_owner(self):
        prior, inv = self.fixture()
        result = resume_plan(prior, dict(prior['identity'], sources={'native_hf.py':'new'}), inv, {}, {'b':'web'})
        self.assertEqual(result['planned_rows'], 16)
        self.assertEqual([j['id'] for j in result['jobs']], ['b'])
        self.assertEqual(result['recovered']['a']['receipt']['generation'], 'old')
        self.assertEqual(result['assignments']['b'], 'web')
        self.assertEqual(prior['recovered'], {})

    def test_vector_format_keeps_population_and_logical_scope(self):
        prior, inv = self.fixture()
        identity={k:v for k,v in prior['identity'].items() if k not in ['max_rows_per_job','row_group_rows']}
        identity.update(format='hf-vector-v1',buffer_values=1000000)
        result=resume_plan(prior,identity,inv,{},format_change=True)
        self.assertEqual(result['planned_rows'],16)
        self.assertEqual(result['recovered']['a']['receipt']['generation'],'old')
        with self.assertRaises(ValueError):resume_plan(prior,dict(identity,block_step=2),inv,{},format_change=True)
        with self.assertRaises(ValueError):resume_plan(prior,dict(identity,format='unreviewed'),inv,{},format_change=True)

    def test_rejects_unowned_or_incomplete_receipt(self):
        for field, value in [('rows', 5), ('generation','other'), ('all_exact',False)]:
            prior, inv = self.fixture();inv[0]['valid'][0]['receipt'][field] = value
            with self.assertRaises(ValueError): resume_plan(prior, prior['identity'], inv, {})
        prior, inv = self.fixture();inv[0]['role'] = 'web'
        with self.assertRaises(ValueError): resume_plan(prior, prior['identity'], inv, {})

    def test_scope_and_coverage_changes_fail_closed(self):
        prior, inv = self.fixture()
        with self.assertRaises(ValueError): resume_plan(prior, dict(prior['identity'], block_step=2), inv, {})
        prior['coverage'][1]['start'] = 8
        with self.assertRaises(ValueError): resume_plan(prior, prior['identity'], inv, {})
        prior, inv = self.fixture(); prior['coverage'][0]['start'] = 3
        with self.assertRaises(ValueError): resume_plan(prior, prior['identity'], inv, {})


if __name__ == '__main__': unittest.main()
