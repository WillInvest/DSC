"""Historical deployment boundaries must be usable without accepting unknown upgrades."""
import hashlib
from unittest.mock import Mock
import unittest
from web3 import Web3
from protocols.deployment import verify_deployment

ADDRESS = '0x' + '12' * 20
IMPL = '0x' + '34' * 20
OLD = '0x' + '56' * 20
TX = '0x' + 'ab' * 32


class HistoricalProfileTests(unittest.TestCase):
    def setup_boundary(self):
        event = {'block': 100, 'log_index': 7, 'transaction_hash': TX, 'implementation': IMPL}
        profile = dict(chain_id=1, address=ADDRESS, from_block=100, to_block=200,
                       implementation=IMPL, previous_implementation=OLD,
                       boundary_upgrades=[event],
                       implementation_code_sha256=hashlib.sha256(b'code').hexdigest())
        w3 = Mock()
        w3.eth.chain_id = 1
        w3.eth.get_storage_at.side_effect = lambda address, slot, block_identifier: bytes.fromhex(
            '00'*12 + (OLD if block_identifier < 100 else IMPL)[2:])
        w3.eth.get_code.return_value = b'code'
        w3.eth.get_logs.return_value = [dict(blockNumber=100, logIndex=7,
            transactionHash=bytes.fromhex(TX[2:]), topics=[Web3.keccak(text='Upgraded(address)'), bytes.fromhex('00'*12+IMPL[2:])])]
        return profile, w3

    def test_reviewed_upgrade_block_is_included(self):
        profile, w3 = self.setup_boundary()
        verify_deployment(w3, profile, range(100,102))

    def test_unrecorded_upgrade_is_rejected(self):
        profile, w3 = self.setup_boundary()
        w3.eth.get_logs.return_value[0]['logIndex'] = 8
        with self.assertRaisesRegex(ValueError, 'upgrade'):
            verify_deployment(w3, profile, range(100,102))

    def test_wrong_previous_implementation_is_rejected(self):
        profile, w3 = self.setup_boundary()
        profile['previous_implementation'] = ADDRESS
        with self.assertRaisesRegex(ValueError, 'implementation'):
            verify_deployment(w3, profile, range(100,102))
