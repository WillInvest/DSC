"""Scope guard tests: a format switch must not change historical population."""
import copy
import unittest
from shared.artifacts import digest
from vector_queue import validate_plan

class VectorQueueScope(unittest.TestCase):
    def plan(self):
        identity={'format':'hf-vector-v1'}
        j=dict(id='whole',start=16291127,stop=26407882,expected_rows=910310934885)
        return dict(identity=identity,generation=digest(identity),coverage=[j],jobs=[j],recovered={},planned_rows=910310934885)
    def test_valid_scope(self):validate_plan(self.plan())
    def test_duplicate_owner_rejected(self):
        p=self.plan();p['recovered']['whole']={'receipt':{'rows':910310934885}}
        with self.assertRaisesRegex(ValueError,'ownership'):validate_plan(p)
    def test_gap_rejected(self):
        p=self.plan();p['coverage']=[dict(id='a',start=16291127,stop=17000000),dict(id='b',start=17000001,stop=26407882)]
        with self.assertRaisesRegex(ValueError,'gap'):validate_plan(p)
    def test_changed_population_rejected(self):
        p=self.plan();p['jobs'][0]['expected_rows']-=1
        with self.assertRaisesRegex(ValueError,'population'):validate_plan(p)
    def test_finalized_end_rejected(self):
        p=self.plan();p['coverage'][0]['stop']+=1
        with self.assertRaisesRegex(ValueError,'finalized'):validate_plan(p)
if __name__=='__main__':unittest.main()
