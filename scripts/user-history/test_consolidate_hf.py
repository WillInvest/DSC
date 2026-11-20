import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import consolidate_hf as c
from shared.artifacts import checksum,write_json


class RetirementTests(unittest.TestCase):
    def test_independent_node_copy_is_read_and_corruption_is_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);copy=root/'copy';copy.write_bytes(b'accepted');plan=root/'node-plan.json'
            write_json(plan,dict(files=[dict(source='original',backup=str(copy),bytes=8,sha256=checksum(copy))]))
            with patch.object(c,'NODE_HOME',root):self.assertTrue(c.verify_node(plan,root/'good.json')['all_exact'])
            copy.write_bytes(b'changed!')
            with patch.object(c,'NODE_HOME',root):self.assertFalse(c.verify_node(plan,root/'bad.json')['all_exact'])
    def setup_plan(self,root):
        home=root/'revert';home.mkdir(); source=home/c.WORK/'g/jobs/1/accounts.parquet'; source.parent.mkdir(parents=True);source.write_bytes(b'accepted-data')
        inventory=root/'inventory.json';write_json(inventory,dict(files=[dict(role='revert',fully_consumed=True,path=str(c.VIRTUAL/'g/jobs/1/accounts.parquet'),sha256=checksum(source),source_jobs=['1'])]))
        plan=root/'plan.json'
        with patch.dict(c.HOMES,{'revert':home}):c.prepare('revert',inventory,plan)
        record=json.loads(plan.read_text());proof=root/'proof.json';write_json(proof,dict(plan_sha256=checksum(plan),all_exact=True,files=[dict(source=x['source'],backup=x['backup'],sha256=x['sha256'],exact=True) for x in record['files']]))
        return home,source,plan,proof

    def test_retirement_records_recovery_and_preserves_other_files(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);home,source,plan,proof=self.setup_plan(root);other=home/'keep';other.write_bytes(b'untouched')
            with patch.dict(c.HOMES,{'revert':home}):result=c.retire('revert',plan,proof,checksum(proof))
            self.assertFalse(source.exists());self.assertEqual(other.read_bytes(),b'untouched');self.assertTrue(result['node_copies_retained']);self.assertTrue(plan.with_suffix('.actions.jsonl').exists())

    def test_changed_source_or_unverified_copy_refuses_retirement(self):
        with tempfile.TemporaryDirectory() as folder:
            home,source,plan,proof=self.setup_plan(Path(folder));source.write_bytes(b'changed-value')
            with patch.dict(c.HOMES,{'revert':home}), self.assertRaises(ValueError):c.retire('revert',plan,proof,checksum(proof))
            self.assertTrue(source.exists())
        with tempfile.TemporaryDirectory() as folder:
            home,source,plan,proof=self.setup_plan(Path(folder));v=json.loads(proof.read_text());v['all_exact']=False;write_json(proof,v)
            with patch.dict(c.HOMES,{'revert':home}), self.assertRaises(ValueError):c.retire('revert',plan,proof,checksum(proof))
            self.assertTrue(source.exists())

    def test_open_or_new_hardlink_refuses_retirement(self):
        with tempfile.TemporaryDirectory() as folder:
            home,source,plan,proof=self.setup_plan(Path(folder))
            with source.open('rb'), patch.dict(c.HOMES,{'revert':home}), self.assertRaises(ValueError):c.retire('revert',plan,proof,checksum(proof))
            self.assertTrue(source.exists());os.link(source,home/'new-link')
            with patch.dict(c.HOMES,{'revert':home}), self.assertRaises(ValueError):c.retire('revert',plan,proof,checksum(proof))
            self.assertTrue(source.exists())

    def test_uninspectable_owned_process_refuses_retirement(self):
        original=Path.iterdir
        def denied(path):
            if path==Path('/proc')/str(os.getpid())/'fd': raise PermissionError('fixture denied descriptors')
            return original(path)
        with tempfile.TemporaryDirectory() as folder:
            home,source,plan,proof=self.setup_plan(Path(folder))
            with patch.object(Path,'iterdir',denied), patch.dict(c.HOMES,{'revert':home}), self.assertRaises(PermissionError):c.retire('revert',plan,proof,checksum(proof))
            self.assertTrue(source.exists())

    def test_interrupted_alias_removal_can_resume(self):
        with tempfile.TemporaryDirectory() as folder:
            home,source,plan,proof=self.setup_plan(Path(folder));alias=source.with_name('alias.parquet');os.link(source,alias)
            v=json.loads(plan.read_text());v['files'][0]['aliases'].append(dict(path=str(alias),identity=c.identity(alias)));write_json(plan,v)
            p=json.loads(proof.read_text());p['plan_sha256']=checksum(plan);write_json(proof,p);alias.unlink()
            with patch.dict(c.HOMES,{'revert':home}):c.retire('revert',plan,proof,checksum(proof))
            self.assertFalse(source.exists())


if __name__=='__main__':unittest.main()
