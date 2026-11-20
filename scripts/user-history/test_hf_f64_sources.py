import copy,tempfile,unittest
from pathlib import Path
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from shared.artifacts import checksum,digest,write_json
from hf_f64_contract import ORIGIN,STOP,FACTOR,CONTRACT,LOGICAL_VALUES,source_assignments
from hf_f64_sources import read_source,MAX
from hf_f64_queue import validate

class RetainedSources(unittest.TestCase):
    def test_table_cast_preserves_finite_and_no_debt(self):
        with tempfile.TemporaryDirectory() as folder:
            p=Path(folder)/'old.parquet';pq.write_table(pa.table({'user':['alice','alice','bob'],'block_number':pa.array([ORIGIN,ORIGIN+1,ORIGIN+1],type=pa.uint32()),'healthFactor':[str(123*10**16),str(MAX),'0']}),p)
            receipt=dict(all_exact=True,factor_generation=FACTOR,start=ORIGIN,stop=ORIGIN+2,rows=3,sha256=checksum(p));source=dict(kind='integer_table',receipt=receipt,files=[dict(path=str(p),sha256=checksum(p),values=3)])
            value=read_source(source,[('alice',ORIGIN),('bob',ORIGIN+1)],ORIGIN,ORIGIN+2)
            self.assertEqual(value[0,0],1.23);self.assertTrue(np.isposinf(value[1,0]));self.assertEqual(value[1,1],0);self.assertTrue(np.isnan(value[0,1]))
            source['files'][0]['sha256']='bad'
            with self.assertRaises(ValueError):read_source(source,[('alice',ORIGIN),('bob',ORIGIN+1)],ORIGIN,ORIGIN+2)

class SourceIdentity(unittest.TestCase):
    def test_source_map_cannot_change_under_same_or_resigned_generation(self):
        with tempfile.TemporaryDirectory() as folder:
            p=Path(folder)/'source.json';j=dict(id='whole',start=ORIGIN,stop=STOP,expected_rows=LOGICAL_VALUES,user_source='integer_table',source=dict(owner='node',receipt=dict(output='/fixture',sha256='s',rows=LOGICAL_VALUES)))
            write_json(p,dict(jobs=[j]));jobs,sources,roles=source_assignments(dict(jobs=[j]))
            identity=dict(contract=CONTRACT,source_manifest_sha256=checksum(p),**{k+'_sha256':digest(v) for k,v in [('jobs',jobs),('sources',sources),('source_roles',roles)]})
            plan=dict(identity=identity,generation=digest(identity),jobs=jobs,sources=sources,source_roles=roles,source_manifest=str(p),max_inflight=1,max_inflight_bytes=100,workers={'node':1})
            validate(plan);changed=copy.deepcopy(plan);changed['sources']={}
            with self.assertRaises(ValueError):validate(changed)
            changed['identity']['sources_sha256']=digest({});changed['generation']=digest(changed['identity'])
            with self.assertRaises(ValueError):validate(changed)
if __name__=='__main__':unittest.main()
