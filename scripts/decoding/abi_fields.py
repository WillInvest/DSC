from collections.abc import Mapping
import pyarrow as pa
from eth_abi import decode, encode
from eth_utils.abi import collapse_if_tuple


def argument_name(field, index):
    return field['name'] or 'output_' + str(index)


def decode_exact(fields, raw):
    types = [collapse_if_tuple(field) for field in fields]
    values = decode(types, raw, strict=True)
    if encode(types, values) != raw:
        raise ValueError('encoded data length or layout does not match ABI')
    return values


def array_item(field):
    return dict(field, type=field['type'].rsplit('[', 1)[0])


def arrow_type(field):
    kind = field['type']
    if kind.endswith(']'):
        return pa.list_(arrow_type(array_item(field)))
    if kind == 'tuple':
        return pa.struct([(argument_name(f, i), arrow_type(f))
                          for i, f in enumerate(field['components'])])
    return pa.bool_() if kind == 'bool' else pa.string()


def arrow_value(field, value):
    kind = field['type']
    if kind.endswith(']'):
        return [arrow_value(array_item(field), item) for item in value]
    if kind == 'tuple':
        return {argument_name(f, i): arrow_value(f, value[f['name']] if isinstance(value, Mapping) else value[i])
                for i, f in enumerate(field['components'])}
    if kind == 'bool':
        return value
    if kind == 'address':
        return value.lower()
    if kind.startswith('bytes'):
        return '0x' + bytes(value).hex()
    return str(value)
