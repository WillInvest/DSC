import copy
import tempfile
import json
import types
from unittest.mock import patch
import unittest
from pathlib import Path

from shared.artifacts import digest, checksum
import hf_flow_control as control
from hf_flow_queue import FlowLedger


class ControlTests(unittest.TestCase):
    def value(self, source=None):
        identity = {'numeric': 'frozen'}
        generation = digest(identity)
        job = {'id': '1-2', 'start': 1, 'stop': 2, 'expected_rows': 10}
        receipt = dict(generation=generation, job=job, identity=identity,
                       within_error_budget=True, structural_exact=True, rows=10, bytes=10, start=1, stop=2,
                       source_kind='integer_cast' if source else 'float64_batch',
                       retained_source_sha256=digest(source) if source else None)
        return dict(receipt=receipt, sha256=digest(receipt)), job, generation

    def test_large_payload_is_scattered_directly_and_graph_contains_only_reference(self):
        payload = {'source_certificate': {'files': {'large-proof': 'unchanged'}}}
        reference = types.SimpleNamespace(release=lambda: None)
        client = types.SimpleNamespace()
        seen = {}
        def scatter(values, **kwargs):
            seen['scatter'] = (values, kwargs)
            return [reference]
        def submit(fn, *args, **kwargs):
            seen['graph_args'] = args
            return 'future'
        client.scatter = scatter;client.submit = submit
        future, retained = control.submit_bulk(client, lambda: None, {'config': 1}, payload,
                                               payload_index=1, workers=['target'], pure=False)
        self.assertEqual(future, 'future')
        self.assertIs(retained, reference)
        self.assertIs(seen['graph_args'][1], reference)
        self.assertEqual(seen['scatter'][1], dict(workers=['target'], direct=True, hash=False))
        self.assertEqual(seen['scatter'][0], [payload])

    def test_receipt_cannot_claim_another_retained_source(self):
        source = {'receipt': 'accepted-source', 'sha256': 'original'}
        value, job, generation = self.value(source)
        control.validate_value(value, job, generation, source)
        other = copy.deepcopy(value)
        other['receipt']['retained_source_sha256'] = digest({'different': 'source'})
        other['sha256'] = digest(other['receipt'])
        with self.assertRaisesRegex(ValueError, 'provenance'):
            control.validate_value(other, job, generation, source)
        with self.assertRaisesRegex(ValueError, 'provenance'):
            control.validate_value(value, job, generation, None)

    def test_malformed_receipt_is_an_integrity_error(self):
        value, job, generation = self.value()
        del value['receipt']['identity']
        value['sha256'] = digest(value['receipt'])
        with self.assertRaisesRegex(ValueError, 'receipt'):
            control.validate_value(value, job, generation, None)

    def test_direct_batch_cannot_be_substituted_for_retained_cast(self):
        value, job, generation = self.value()
        control.validate_value(value, job, generation, None)
        with self.assertRaisesRegex(ValueError, 'provenance'):
            control.validate_value(value, job, generation, {'receipt': 'retained'})

    def test_completion_requires_exact_cloud_scope_and_closed_operations(self):
        jobs = {'a': {'expected_rows': 910310934884}, 'b': {'expected_rows': 1}}
        with tempfile.TemporaryDirectory() as directory:
            with FlowLedger(Path(directory) / 'flow.sqlite', 'g') as db:
                for key, job in jobs.items():
                    value = dict(sha256=key, receipt=dict(bytes=1, rows=job['expected_rows']))
                    db.node_commit(key, value, role='node')
                self.assertFalse(control.completed_scope(db, jobs))
                db.cloud_commit(['a', 'b'], {'id': 'cloud', 'verified_bytes': 2})
                db.prepare_operation('transfer', 'transfer', 'data', {'keys': []})
                self.assertFalse(control.completed_scope(db, jobs))
                db.finish_operation('transfer', {'all_exact': True})
                self.assertTrue(control.completed_scope(db, jobs))
                db.db.execute('update metrics set rows=rows-1 where job_id=?', ('a',))
                db.db.commit()
                with self.assertRaisesRegex(ValueError, 'population'):
                    control.completed_scope(db, jobs)

    def test_archive_fill_counts_only_uncommitted_transfer_bytes(self):
        cfg=dict(transfer_target_bytes=100,cloud_target_bytes=400,archive_fill_transfers=True)
        with tempfile.TemporaryDirectory() as directory:
            with FlowLedger(Path(directory)/'flow.sqlite','g') as db:
                node=dict(sha256='node',receipt=dict(bytes=240,rows=1))
                queued=dict(sha256='queued',receipt=dict(bytes=80,rows=1))
                db.node_commit('node',node,role='node');db.observe('queued','data',queued)
                db.prepare_operation('copy','transfer','data',dict(keys=['queued']))
                self.assertEqual(control.transfer_target(db,db.stage_totals(),db.pending_operations(),cfg,90),80)
                # Committing the queued copy increases node coverage; do not add it twice.
                db.node_commit('queued',queued)
                self.assertEqual(control.transfer_target(db,db.stage_totals(),db.pending_operations(),cfg,90),80)
                # Do not create tiny trickles, or fills during an existing archive.
                self.assertIsNone(control.transfer_target(db,db.stage_totals(),db.pending_operations(),cfg,12))
                db.prepare_operation('archive','cloud','cloud',dict(keys=['node']))
                self.assertIsNone(control.transfer_target(db,db.stage_totals(),db.pending_operations(),cfg,90))
                self.assertEqual(control.transfer_target(db,db.stage_totals(),db.pending_operations(),cfg,150),100)

    def test_offline_pending_transfer_does_not_block_online_archive_fill(self):
        cfg=dict(transfer_target_bytes=100,cloud_target_bytes=400,archive_fill_transfers=True)
        with tempfile.TemporaryDirectory() as directory:
            with FlowLedger(Path(directory)/'flow.sqlite','g') as db:
                db.node_commit('node',dict(sha256='node',receipt=dict(bytes=320,rows=1)),role='node')
                db.observe('queued','data',dict(sha256='queued',receipt=dict(bytes=125,rows=1)))
                db.prepare_operation('copy','transfer','data',dict(keys=['queued']))
                self.assertEqual(control.transfer_target(db,db.stage_totals(),db.pending_operations(),cfg,90,available_roles={'node','web'}),80)
                self.assertEqual([op['id'] for op in db.pending_operations()],['copy'])

    def test_archive_fill_does_not_change_ordinary_or_disabled_thresholds(self):
        cfg=dict(transfer_target_bytes=100,cloud_target_bytes=400,archive_fill_transfers=False)
        with tempfile.TemporaryDirectory() as directory:
            with FlowLedger(Path(directory)/'flow.sqlite','g') as db:
                self.assertIsNone(control.transfer_target(db,[dict(cloud_pending_bytes=320)],[],cfg,90))
                self.assertEqual(control.transfer_target(db,[],[],cfg,100),100)
                cfg['archive_fill_transfers']=True
                self.assertIsNone(control.transfer_target(db,[dict(cloud_pending_bytes=50)],[],cfg,90))
                self.assertIsNone(control.transfer_target(db,[dict(cloud_pending_bytes=400)],[],cfg,90))

    def controller_fixture(self, directory, entries=None, read_error=None):
        root = Path(directory)
        value, job, generation = self.value()
        numeric = root / 'numeric.json'
        numeric.write_text(json.dumps(dict(jobs=[job], sources={})))
        (root / 'users.parquet').write_bytes(b'dictionary')
        (root / 'dataset.json').write_text('{}')
        cfg = dict(control_root=str(root), generation=generation,
                   numeric_plan=str(numeric), numeric_plan_sha256=checksum(numeric),
                   old_base=str(root), scheduler='unused', private='unused',
                   node_bulk=str(root / 'bulk'), node_home=str(root),
                   production_ready=True, transfer_target_bytes=125000000000,
                   cloud_target_bytes=1,
                   roles={'node': dict(jobs=[job['id']], reserve_bytes=50, slots=1)})
        config = root / 'config.json'
        config.write_text(json.dumps(cfg))
        client = types.SimpleNamespace(holds=[], submissions=[], reads=0)
        client.scatter = lambda values, **kwargs: [types.SimpleNamespace(release=lambda: None)]
        client.scheduler_info = lambda **kwargs: {'workers': {'io': {'name': 'flow-io-node-0'}, 'cloud': {'name': 'flow-cloud-0'}}}
        def run(function, *args, **kwargs):
            if function is control.hold:
                client.holds.append(args)
                return {'io': {}}
            client.reads += 1
            if read_error:
                raise read_error
            return {'io': dict(entries=entries or [], sequence=0, progress={'state': 'running'})}
        client.run = run
        future = types.SimpleNamespace(done=lambda: False, release=lambda: None)
        def forbidden_result(*args, **kwargs):
            self.fail('Pending metadata must not block the controller')
        future.result = forbidden_result
        def submit(function, *args, **kwargs):
            client.submissions.append(kwargs)
            return future
        client.submit = submit
        client.close = lambda: None
        modules = {'distributed': types.SimpleNamespace(Client=lambda *args, **kwargs: client),
                   'vector_coordinator': types.SimpleNamespace(security=lambda _: None),
                   'vector_archive': types.SimpleNamespace(registry_source=lambda: None)}
        return config, cfg, client, modules, value, job

    def test_pending_metadata_does_not_block_catalog_polling(self):
        with tempfile.TemporaryDirectory() as directory:
            config, cfg, client, modules, value, job = self.controller_fixture(directory)
            with FlowLedger(Path(directory) / 'flow.sqlite', cfg['generation']) as db:
                db.observe(job['id'], 'node', value)
                db.node_commit(job['id'], value)
            class EndLoop(Exception):
                pass
            def tick(_):
                if client.reads >= 3:
                    raise EndLoop()
            with patch.dict('sys.modules', modules), patch.object(control.time, 'sleep', tick):
                with self.assertRaises(EndLoop):
                    control.run(config)
            self.assertEqual(client.reads, 3)
            self.assertEqual(len(client.submissions), 1)
            self.assertEqual(client.submissions[0]['priority'], 100)
            self.assertEqual(client.holds, [])
            self.assertTrue((Path(directory) / 'public' / 'node.json').exists())

    def test_invalid_provenance_holds_producer_without_accepting_result(self):
        with tempfile.TemporaryDirectory() as directory:
            value, job, _ = self.value({'unexpected': 'source'})
            entry = dict(job_id=job['id'], value=value, sequence=1)
            config, cfg, client, modules, _, _ = self.controller_fixture(directory, entries=[entry])
            with patch.dict('sys.modules', modules):
                with self.assertRaisesRegex(ValueError, 'integrity failure'):
                    control.run(config)
            self.assertEqual(len(client.holds), 1)
            with FlowLedger(Path(directory) / 'flow.sqlite', cfg['generation']) as db:
                self.assertEqual(db.db.execute('select count(*) from jobs').fetchone()[0], 0)
            self.assertEqual(json.loads((Path(directory) / 'progress.json').read_text())['state'], 'failed')

    def test_transport_timeout_does_not_hold_numerical_producers(self):
        with tempfile.TemporaryDirectory() as directory:
            config, _, client, modules, _, _ = self.controller_fixture(directory, read_error=TimeoutError('network'))
            class EndLoop(Exception):
                pass
            with patch.dict('sys.modules', modules), patch.object(control.time, 'sleep', side_effect=EndLoop):
                with self.assertRaises(EndLoop):
                    control.run(config)
            self.assertEqual(client.holds, [])
            report = json.loads((Path(directory) / 'progress.json').read_text())
            self.assertEqual(report['state'], 'running')
            self.assertIn('TimeoutError', report['errors']['node'])


if __name__ == '__main__':
    unittest.main()
