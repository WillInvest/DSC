import unittest,asyncio
from unittest.mock import patch
import numpy as np
from test_hf_f64 import fixture

class FloatDebtBoundary(unittest.TestCase):
    def test_integer_positive_debt_rounded_to_float_zero_is_not_no_debt(self):
        model,ev=fixture();scaled=15384615384615385
        ev.positions['bob']={'veth':str(scaled)};ev.asset_prices[0]='65'
        self.assertEqual(scaled*65//10**18,1)
        self.assertEqual(np.floor((float(scaled)/1e18)*65),0)
        ev.account=lambda owner:{'account':['0','1','0','0','0','0']}
        with patch('hf_batch_f64.exact_debts',return_value=[(0,1.)]):value=model.evaluate(ev)
        self.assertEqual(value['debt'][1],1.)
        self.assertEqual(value['hf'][1],0.)

class BoundedResourceWait(unittest.TestCase):
    def test_python310_dask_timeout_is_a_poll_not_a_task_failure(self):
        from hf_f64_queue import wait_result
        class Future:
            calls=0
            def result(self,timeout):
                self.calls+=1
                if self.calls==1:raise asyncio.TimeoutError()
                return 'finished'
        pulses=[];self.assertEqual(wait_result(Future(),lambda:False,lambda:pulses.append(1)),'finished');self.assertEqual(pulses,[1])
    def test_pause_observed_between_resource_waits(self):
        from hf_f64_queue import wait_result
        class Future:
            def result(self,timeout):raise TimeoutError()
        calls=iter([False,True]);pulses=[]
        with self.assertRaises(InterruptedError):wait_result(Future(),lambda:next(calls),lambda:pulses.append(1),timeout=30)
        self.assertEqual(len(pulses),1)
    def test_missing_resource_has_finite_deadline(self):
        from hf_f64_queue import wait_result
        class Future:
            def result(self,timeout):raise TimeoutError()
        ticks=iter([0,0,2])
        with self.assertRaises(TimeoutError):wait_result(Future(),lambda:False,lambda:None,timeout=1,clock=lambda:next(ticks))

if __name__=='__main__':unittest.main()
