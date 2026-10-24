import hashlib
import json
from pathlib import Path

IMPLEMENTATION_SLOT = '0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc'


def block_range(value):
    parts = value.split(':')
    if not 1 <= len(parts) <= 3 or any(not p.isdigit() for p in parts):
        raise ValueError('use BLOCK or START:END_EXCLUSIVE[:STEP]')
    start = int(parts[0])
    stop = int(parts[1]) if len(parts) > 1 else start + 1
    step = int(parts[2]) if len(parts) > 2 else 1
    if stop <= start or step <= 0:
        raise ValueError('range must be nonempty and step positive')
    return range(start, stop, step)


def load_deployment(path, config, address):
    from web3 import Web3
    if path is None:
        raise ValueError('this protocol requires a reviewed deployment profile')
    profile = json.loads(Path(path).read_text())
    if profile['protocol'] != config['protocol'] or profile['chain_id'] != 1:
        raise ValueError('deployment protocol or chain mismatch')
    if not Web3.is_address(address) or profile['address'].lower() != address.lower():
        raise ValueError('deployment address mismatch')
    if not Web3.is_address(profile['implementation']):
        raise ValueError('invalid deployment implementation')
    if not isinstance(profile['from_block'], int) or not isinstance(profile['to_block'], int) or not 0 <= profile['from_block'] < profile['to_block']:
        raise ValueError('deployment requires a bounded historical block interval')
    if profile['abi_sha256'] != hashlib.sha256(config['abi'].read_bytes()).hexdigest():
        raise ValueError('deployment ABI pin mismatch')
    if len(bytes.fromhex(profile['implementation_code_sha256'])) != 32:
        raise ValueError('invalid implementation code hash')
    return profile


def check_block(profile, block):
    if not profile['from_block'] <= block < profile['to_block']:
        raise ValueError(f'block {block} is outside supported deployment/ABI range')


def verify_deployment(w3, profile, blocks):
    """Reject upgrade blocks as well as ranges crossing a proxy upgrade."""
    from web3 import Web3
    start, end = blocks[0], blocks[-1]
    check_block(profile, start)
    check_block(profile, end)
    if w3.eth.chain_id != profile['chain_id']:
        raise ValueError('RPC chain mismatch')
    address = Web3.to_checksum_address(profile['address'])
    expected = profile['implementation'].lower()
    for height in {max(0, start - 1), start, end}:
        slot = w3.eth.get_storage_at(address, IMPLEMENTATION_SLOT, block_identifier=height)
        if '0x' + bytes(slot)[-20:].hex() != expected:
            raise ValueError('unsupported proxy implementation or upgrade boundary')
    upgrades = w3.eth.get_logs({'address': address, 'fromBlock': start, 'toBlock': end,
                               'topics': ['0x' + Web3.keccak(text='Upgraded(address)').hex()]})
    if upgrades:
        raise ValueError('range contains a proxy upgrade; split and verify ABI profiles')
    code = w3.eth.get_code(Web3.to_checksum_address(expected), block_identifier=start)
    if not code or hashlib.sha256(bytes(code)).hexdigest() != profile['implementation_code_sha256']:
        raise ValueError('implementation bytecode pin mismatch')


def save_profile(output, profile):
    path = output / 'deployment.json'
    if path.exists() and json.loads(path.read_text()) != profile:
        raise ValueError('output belongs to a different deployment profile')
    output.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(profile, indent=2) + '\n')
