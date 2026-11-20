import unittest
from oracle_common import intervals

class OracleIntervalsTest(unittest.TestCase):
    def test_new_asset_and_route_take_effect_at_end_of_change_block(self):
        snapshots=[dict(block=10,assets=[],oracle='old',unit=None),
                   dict(block=12,assets=['ETH'],oracle='old',unit=100),
                   dict(block=15,assets=['ETH','USD'],oracle='new',unit=100)]
        jobs=intervals(snapshots,20,[18])
        self.assertEqual([(j['start'],j['stop'],j['assets'],j['oracle']) for j in jobs],
                         [(10,12,[],'old'),(12,15,['ETH'],'old'),
                          (15,18,['ETH','USD'],'new'),(18,20,['ETH','USD'],'new')])
        self.assertEqual([b for j in jobs for b in range(j['start'],j['stop'])],list(range(10,20)))

    def test_invalid_stop_cannot_create_accepted_coverage(self):
        with self.assertRaises(ValueError):intervals([dict(block=10,assets=[],oracle='x',unit=None)],10)

if __name__=='__main__':unittest.main()
