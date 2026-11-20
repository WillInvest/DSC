"""The new execution path retains every logical block and owner."""
import sys,unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import native_materialize as n

class NativeMaterializeTests(unittest.TestCase):
 def test_grid_covers_all_blocks_and_both_upgrade_sides(self):
  boundaries=[16291127,17214196,21917056,26392162]
  with patch('accounting_inputs.profiles',return_value=[dict(from_block=b) for b in boundaries]):
   jobs=n.native_layout(16291127,26407882)
  ordered=sorted(jobs,key=lambda j:j['start'])
  self.assertEqual(jobs[0]['start'],16800000);self.assertEqual(jobs[0]['stop'],16800001)
  self.assertEqual(ordered[0]['start'],16291127);self.assertEqual(ordered[-1]['stop'],26407882)
  self.assertTrue(all(a['stop']==b['start'] for a,b in zip(ordered,ordered[1:])))
  self.assertTrue(all(0<j['stop']-j['start']<=10000 for j in jobs))
  self.assertEqual(sum(j['stop']-j['start'] for j in jobs),10116755)
  for b in boundaries[1:]:
   self.assertIn(b,{j['start'] for j in jobs});self.assertIn(b-1,{j['start'] for j in jobs})

if __name__=='__main__':unittest.main()
