"""Read the tracked contract CSV using the standard csv library."""
import csv


def registry(path):
    rows = list(csv.DictReader(path.open()))
    from web3 import Web3
    from protocols.protocol_config import PROTOCOLS
    for r in rows:
        if int(r['chain_id']) != 1 or r['protocol'] not in PROTOCOLS or not Web3.is_address(r['address']):
            raise ValueError('invalid registry entry')
        r['address'] = r['address'].lower()
        r['creation_block'] = int(r['creation_block'])
        if r['creation_block'] < 0:
            raise ValueError('negative deployment block')
    if not rows or len({r['address'] for r in rows}) != len(rows):
        raise ValueError('empty or duplicate registry')
    return rows
