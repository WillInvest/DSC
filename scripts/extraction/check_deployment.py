#!/usr/bin/env python3
"""Verify a deployment before or after extracting an upgradeable contract."""
import argparse
import os
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from web3 import Web3
from protocols.protocol_config import load_protocol
from protocols.deployment import block_range, load_deployment, verify_deployment, save_profile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('protocol')
    parser.add_argument('address')
    parser.add_argument('blocks')
    parser.add_argument('output', type=Path)
    parser.add_argument('profile', type=Path)
    args = parser.parse_args()
    config = load_protocol(args.protocol)
    profile = load_deployment(args.profile, config, args.address)
    w3 = Web3(Web3.HTTPProvider(os.environ.get('ETH_RPC_URL', 'http://127.0.0.1:8545'), request_kwargs={'timeout': 60}))
    verify_deployment(w3, profile, block_range(args.blocks))
    save_profile(args.output, profile)


if __name__ == '__main__':
    main()
