import gzip
import json
import sys
import tempfile
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from pipeline.verify import compare_archives


class VerificationTests(unittest.TestCase):
    def records(self,root,reason):
        root.mkdir()
        row=dict(receipt_summary=dict(block_number=10),failed=[dict(receipt=dict(transactionHash='tx'),
                 trace=dict(type='CALL',output='0x08c379a0',error='execution reverted',revertReason=reason))])
        with gzip.open(root/'10-10.jsonl.gz','wt') as stream:stream.write(json.dumps(row)+'\n')

    def test_different_decoded_reason_cannot_pass_exact_replay_gate(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);self.records(root/'old','Too little received');self.records(root/'new','WRONG REASON')
            with self.assertRaisesRegex(ValueError,'Trace differs'):compare_archives(root/'old',root/'new',10,11)
            self.assertTrue((root/'trace-mismatch.json').exists())

    def test_identical_receipt_and_trace_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);self.records(root/'old','same');self.records(root/'new','same')
            self.assertEqual(compare_archives(root/'old',root/'new',10,11),dict(exact_traces=1))


if __name__=='__main__':unittest.main()
