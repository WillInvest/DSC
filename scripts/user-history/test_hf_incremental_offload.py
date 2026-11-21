from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest

import numpy as np

from hf_f64_vectors import STOP
from hf_incremental_compute import write_tiles
from hf_incremental_offload import CANDIDATE, accept_candidate, validate_wallets
from shared.artifacts import write_json


class IncrementalOffloadTests(unittest.TestCase):
    def test_acceptance_couples_samples_to_the_returned_wallet_tiles(self):
        with TemporaryDirectory() as temporary:
            stage = Path(temporary)
            scope = dict(start=STOP, stop=STOP+3, window_sha256='0'*64,
                         wallet_registry_sha256='1'*64)
            owners = [('first', STOP-5), ('second', STOP+2)]
            model = SimpleNamespace(owners=owners, firsts=np.array([item[1] for item in owners]))
            write_json(stage/'wallets.json', owners)
            matrix = np.array([[np.inf, np.nan], [1.0, np.nan], [2.0, 3.0]])
            parts = list(write_tiles(stage, STOP, matrix, model.firsts, scope))
            record = dict(scope=scope, wallet_values=4, wallet_parts=parts)
            samples = [dict(user='first', entity_id=0, block=STOP+i, hf=value)
                       for i, value in enumerate(('+inf', 1.0, 2.0))]
            validate_wallets(stage, record, model, samples)
            with self.assertRaisesRegex(ValueError, 'sample differs from returned Parquet'):
                validate_wallets(stage, record, model, [*samples[:2], dict(samples[2], hf=2.1)])
            with self.assertRaisesRegex(ValueError, 'lacks complete HF sample blocks'):
                validate_wallets(stage, record, model, samples[:2])
            with self.assertRaisesRegex(ValueError, 'do not cover the entire window'):
                validate_wallets(stage, dict(record, wallet_parts=[]), model, samples)

    def test_rejects_unpinned_candidate_before_creating_an_accepted_directory(self):
        with TemporaryDirectory() as temporary:
            job = Path(temporary)
            write_json(job/CANDIDATE, {'state': 'computed'})
            product = job/'accepted'
            with self.assertRaisesRegex(ValueError, 'receipt checksum differs'):
                accept_candidate(job, 'wrong', product, {}, None, {})
            self.assertFalse(product.exists())
