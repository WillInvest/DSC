import unittest
import numpy as np
from hf_f64_vectors import encode_users,decode_users,encode_aggregates,ORIGIN,STOP
import pyarrow as pa
import pyarrow.parquet as pq
class FloatVectorRoundtrip(unittest.TestCase):
    def test_first_seen_inside_tile_and_bit_pattern(self):
        first=np.array([ORIGIN,ORIGIN+1,ORIGIN+2]);x=np.array([[1.23,np.nan,np.nan],[np.inf,0,np.nan],[.0000001,2,np.inf]])
        data=encode_users(x,ORIGIN,first);start,stop,ids,y=decode_users(data,first)
        mask=~np.isnan(x);self.assertEqual(start,ORIGIN);self.assertEqual(stop,ORIGIN+3)
        self.assertTrue(np.array_equal(x[mask].view(np.uint64),y[mask].view(np.uint64)))
    def test_nonzero_wallet_range(self):
        data=encode_users([[1],[2]],ORIGIN,[ORIGIN],wallet_start=2)
        start,stop,ids,y=decode_users(data,[ORIGIN,ORIGIN,ORIGIN])
        self.assertEqual(ids.tolist(),[2]);self.assertEqual(y[:,0].tolist(),[1,2])
    def test_missing_user_value_rejected(self):
        with self.assertRaises(ValueError):encode_users([[np.nan]],ORIGIN,[ORIGIN])
    def test_dictionary_reindex_rejected(self):
        data=encode_users([[1,2]],ORIGIN,[ORIGIN,ORIGIN])
        with self.assertRaises(ValueError):decode_users(data,[ORIGIN,ORIGIN+1])
    def test_incremental_tile_pins_window_and_wallet_registry(self):
        scope=dict(start=STOP,stop=STOP+3,window_sha256='a'*64,wallet_registry_sha256='b'*64)
        first=[ORIGIN,STOP+1]
        matrix=np.array([[1.,np.nan],[np.inf,3.]])
        data=encode_users(matrix,STOP,first,scope=scope)
        start,stop,ids,values=decode_users(data,first,scope=scope)
        self.assertEqual((start,stop,ids.tolist()),(STOP,STOP+2,[0,1]))
        np.testing.assert_array_equal(values.view(np.uint64),matrix.view(np.uint64))
        with self.assertRaisesRegex(ValueError,'scope'):
            decode_users(data,first,scope={**scope,'wallet_registry_sha256':'c'*64})
        with self.assertRaisesRegex(ValueError,'scope'):
            decode_users(data,first)
        with self.assertRaisesRegex(ValueError,'scope'):
            encode_users(matrix,STOP+2,first,scope=scope)
    def test_aggregate_null_and_proof_retained(self):
        data=encode_aggregates([ORIGIN],[[0,2]],[[0,6]],[[0,3]],[0,1]);t=pq.read_table(pa.BufferReader(data))
        self.assertEqual(t['hf_vector'].to_pylist(),[[None],[2.]])
        self.assertEqual(t['denominator_vector'].to_pylist(),[[0.],[3.]])
if __name__=='__main__':unittest.main()
