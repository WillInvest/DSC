"""Decode Cryo rows with Web3.py/eth-abi and describe their output tables."""
from dataclasses import dataclass
from functools import lru_cache
import json
import re

from eth_abi.exceptions import DecodingError
import pyarrow as pa
from web3 import Web3
from web3.exceptions import Web3Exception
from protocols.deployment import check_block
from decoding.abi_fields import argument_name, arrow_type, arrow_value, decode_exact

COMMON_COLUMNS = [
    ('protocol', pa.string()), ('chain_id', pa.uint64()), ('address', pa.string()),
    ('block_number', pa.uint64()), ('source_file', pa.string()), ('name', pa.string()),
]
DATASET_COLUMNS = {
    'events': [('block_hash', pa.string()), ('transaction_hash', pa.string()),
               ('transaction_index', pa.uint64()), ('log_index', pa.uint64()), ('topic0', pa.string())],
    'state': [('call_data', pa.string())],
}


@dataclass
class DecodedRow:
    name: str
    values: dict
    key: tuple
    schema: pa.Schema


def payload(value):
    if not isinstance(value, str) or not re.fullmatch(r'0x(?:[0-9a-fA-F]{2})*', value):
        raise ValueError('null or invalid encoded payload')
    return bytes.fromhex(value[2:])


def cryo_event_log(row):
    topics = [row.get('topic' + str(i)) for i in range(4)]
    present = [topic for topic in topics if topic is not None]
    if topics[:len(present)] != present:
        raise ValueError('noncontiguous event topics')
    return dict(address=row['address'].lower(), topics=[payload(t) for t in present],
                data=payload(row['data']), blockNumber=row['block_number'],
                blockHash=row['block_hash'], transactionHash=row['transaction_hash'],
                transactionIndex=row['transaction_index'], logIndex=row['log_index'])


class EventDecoder:
    def __init__(self, abi):
        contract = Web3().eth.contract(abi=abi)
        self.event_by_topic = lru_cache(maxsize=128)(contract.get_event_by_topic)

    def decode(self, row):
        log = cryo_event_log(row)
        try:
            event = self.event_by_topic(row['topic0'])
            fields = event.abi['inputs']
            decode_exact([f for f in fields if not f['indexed']], log['data'])
            args = event().process_log(log)['args']
        except (Web3Exception, DecodingError) as exc:
            raise ValueError('invalid or unknown event log') from exc
        fields = [dict(f, type='bytes32') if f['indexed'] and
                  (f['type'] in ('string', 'bytes', 'tuple') or '[' in f['type']) else f for f in fields]
        return event.abi['name'], fields, [args[f['name']] for f in fields], (row['block_hash'], row['log_index'])


class StateDecoder:
    def __init__(self, abi, getters):
        self.contract = Web3().eth.contract(abi=abi)
        self.getters = set(getters)
        names = {entry.get('name') for entry in abi if entry['type'] == 'function'}
        if not self.getters <= names:
            raise ValueError('ABI is missing configured state getters')

    @lru_cache(maxsize=512)
    def resolve_call(self, data):
        try:
            function, arguments = self.contract.decode_function_input(payload(data))
        except (ValueError, Web3Exception, DecodingError) as exc:
            raise ValueError(f'unknown or malformed state selector: {data}') from exc
        entry = function.abi
        if entry['name'] not in self.getters:
            raise ValueError(f'unknown state selector: {data}')
        if self.contract.encode_abi(function.signature, args=list(arguments.values())).lower() != data.lower():
            raise ValueError('noncanonical state call data')
        fields = [dict(field, name=argument_name(field, i)) for i, field in enumerate(entry['outputs'])]
        inputs = [dict(f, name='input_' + f['name']) for f in entry['inputs']]
        return entry['name'], inputs, fields, list(arguments.values())

    def decode(self, row):
        data = row['call_data']
        name, inputs, fields, arguments = self.resolve_call(data)
        values = decode_exact(fields, payload(row['output_data']))
        return name, inputs + fields, arguments + list(values), (row['block_number'], data.lower())


class PoolDecoder:
    def __init__(self, kind, abi_path, address, getters, schema_version=None, deployment=None):
        abi = json.loads(abi_path.read_text())
        self.deployment = deployment
        self.schema_version = schema_version
        self.kind = kind
        self.address = address
        self.decoder = EventDecoder(abi) if kind == 'events' else StateDecoder(abi, getters)
        self.metadata = DATASET_COLUMNS[kind]
        self.schemas = {}

    def decode_row(self, row, source_file):
        address_column = 'address' if self.kind == 'events' else 'contract_address'
        source_address = row[address_column].lower()
        if row['chain_id'] != 1 or source_address != self.address:
            raise ValueError('input has wrong chain or pool address')
        if self.deployment:
            check_block(self.deployment, row['block_number'])
        name, fields, decoded, key = self.decoder.decode(row)
        result = dict(chain_id=1, address=self.address, block_number=row['block_number'],
                      source_file=source_file, name=name, **{k: row[k] for k, _ in self.metadata})
        if self.schema_version is not None:
            result['schema_version'] = self.schema_version
        for index, (field, value) in enumerate(zip(fields, decoded)):
            result['arg__' + argument_name(field, index)] = arrow_value(field, value)
        if name not in self.schemas:
            columns = [('arg__' + argument_name(field, i), arrow_type(field))
                       for i, field in enumerate(fields)]
            version = [('schema_version', pa.uint32())] if self.schema_version is not None else []
            self.schemas[name] = pa.schema(COMMON_COLUMNS + self.metadata + version + columns)
        return DecodedRow(name, result, key, self.schemas[name])
