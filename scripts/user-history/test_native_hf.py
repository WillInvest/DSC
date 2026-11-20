"""Native outputs must equal retained original-Solidity/node fixtures."""
import unittest
from test_account_kernel import AccountKernelTests, ARTIFACTS
from native_hf import AccountKernel

class NativeAccountTests(AccountKernelTests):
 @classmethod
 def setUpClass(cls):cls.worker=AccountKernel(ARTIFACTS)
 @classmethod
 def tearDownClass(cls):pass
 def call(self,p):
  try:return self.worker.call(p)
  except ValueError as e:return {'error':str(e)}

if __name__=='__main__':unittest.main()
