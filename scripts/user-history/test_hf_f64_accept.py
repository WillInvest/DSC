import hashlib,unittest
from unittest.mock import patch
import numpy as np
from shared.artifacts import digest
from hf_f64_vectors import encode_users,encode_aggregates,ORIGIN
from hf_f64_accept import audit

def part(data,**kw):return dict(data=data,sha256=hashlib.sha256(data).hexdigest(),start=ORIGIN,stop=ORIGIN+2,**kw)

class Acceptance(unittest.TestCase):
    def setUp(self):
        self.owners=[('alice',ORIGIN),('bob',ORIGIN+1)]
        self.result=dict(job=dict(start=ORIGIN,stop=ORIGIN+2),registry_digest=digest(self.owners),rows=3,samples=[],reserves=['eth'],parts=[part(encode_users([[2,np.nan],[3,np.inf]],ORIGIN,[ORIGIN,ORIGIN+1]),values=3)],aggregate_parts=[part(encode_aggregates([ORIGIN,ORIGIN+1],[[2,2],[3,3]],[[4,4],[6,6]],[[2,2],[2,2]],[0,1]))])
        self.patcher=patch('hf_f64_accept.REGISTRY',digest(self.owners));self.patcher.start();self.addCleanup(self.patcher.stop)
    def test_complete(self):self.assertTrue(audit(self.result,self.owners)['structural_exact'])
    def test_duplicate_user_tile(self):
        self.result['parts']*=2
        with self.assertRaises(ValueError):audit(self.result,self.owners)
    def test_aggregate_required(self):
        self.result['aggregate_parts']=[]
        with self.assertRaises(ValueError):audit(self.result,self.owners)
    def test_protocol_must_equal_reserve_reduction(self):
        self.result['aggregate_parts']=[part(encode_aggregates([ORIGIN,ORIGIN+1],[[2,2],[3,3]],[[4,8],[6,12]],[[2,4],[2,4]],[0,1]))]
        with self.assertRaises(ValueError):audit(self.result,self.owners)
    def test_tampered_bytes(self):
        self.result['parts'][0]['data']+=b'x'
        with self.assertRaises(ValueError):audit(self.result,self.owners)
if __name__=='__main__':unittest.main()
