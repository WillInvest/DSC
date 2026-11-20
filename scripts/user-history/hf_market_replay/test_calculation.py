"""Behavioral checks for end-of-block cohort membership and archive validation."""
import unittest

import numpy as np

from hf_market_replay.cohorts import summarize
from hf_market_replay.validation import encode_part, validate_result


class CohortTests(unittest.TestCase):
    def summarize(self, categories):
        # Alice owes reserve 0 and 1. Bob owes reserve 0.
        return summarize(
            np.array([1.2, 2.0]),
            np.array([0, 0, 1]),
            np.array([0, 1, 0]),
            np.array([900.0, 100.0, 100.0]),
            np.array(categories),
            reserve_count=2,
        )

    def test_filter_removes_all_reserve_debt_of_emode_wallet(self):
        result = self.summarize([1, 1, 0])
        self.assertAlmostEqual(result['non_emode']['numerator'][-1], 200.0)
        self.assertAlmostEqual(result['non_emode']['debt_base'][-1], 100.0)
        self.assertEqual(result['non_emode']['borrower_count'].tolist(), [1, 0, 1])
        self.assertTrue(np.isnan(result['non_emode']['hf'][1]))
        self.assertAlmostEqual(result['emode']['debt_base'][-1], 1000.0)
        self.assertAlmostEqual(result['all']['debt_base'][-1], 1100.0)

    def test_no_emode_matches_unfiltered_and_input_hf_stays_unchanged(self):
        result = self.summarize([0, 0, 0])
        np.testing.assert_array_equal(result['all']['numerator'], result['non_emode']['numerator'])
        np.testing.assert_array_equal(result['all']['debt_base'], result['non_emode']['debt_base'])
        self.assertTrue(np.all(np.isnan(result['emode']['hf'])))

    def test_category_transition_changes_membership_at_that_block(self):
        before = self.summarize([0, 0, 0])
        entered = self.summarize([1, 1, 0])
        switched = self.summarize([2, 2, 0])
        exited = self.summarize([0, 0, 0])
        self.assertEqual(before['non_emode']['debt_base'][-1], 1100.0)
        self.assertEqual(entered['non_emode']['debt_base'][-1], 100.0)
        np.testing.assert_array_equal(entered['non_emode']['hf'], switched['non_emode']['hf'])
        np.testing.assert_array_equal(before['non_emode']['hf'], exited['non_emode']['hf'])

    def test_no_debt_and_all_emode_have_null_filtered_hf(self):
        result = summarize([np.inf], [], [], [], [], reserve_count=2)
        self.assertEqual(result['non_emode']['debt_base'].tolist(), [0.0, 0.0, 0.0])
        self.assertTrue(np.all(np.isnan(result['non_emode']['hf'])))
        result = self.summarize([1, 1, 1])
        self.assertTrue(np.all(np.isnan(result['non_emode']['hf'])))

    def test_multiple_edges_for_wallet_have_consistent_category(self):
        with self.assertRaisesRegex(ValueError, 'category'):
            self.summarize([0, 1, 0])

    def test_hf_input_is_not_mutated_and_infinity_has_no_weight(self):
        hf = np.array([1.2, np.inf])
        result = summarize(hf, [0], [0], [1_000_000.0], [0], 1)
        np.testing.assert_array_equal(hf, [1.2, np.inf])
        self.assertEqual(result['non_emode']['debt_base'][-1], 1_000_000.0)
        self.assertAlmostEqual(result['non_emode']['hf'][-1], 1.2)

    def test_parquet_scope_and_partition_roundtrip(self):
        block = 16_800_000
        summary = self.summarize([1, 1, 0])
        part = encode_part([dict(block=block, cohorts=summary, excluded_dust_users=0,
                                 excluded_dust_debt_base=0.0)], block, block + 1, 2)
        result = dict(job=dict(id=str(block), start=block, stop=block + 1),
                      format='hf-market-cohort-v1', reserves=['asset0', 'asset1'],
                      parts=[part], samples=[], cohort_proofs=[])
        report = validate_result(result)
        self.assertEqual(report['aggregate_blocks'], 1)
        self.assertEqual(report['rows'], 9)
        damaged = dict(result, parts=[dict(part, stop=block + 2)])
        with self.assertRaises(ValueError):
            validate_result(damaged)


if __name__ == '__main__':
    unittest.main()
