"""Make the private Node research datasets discoverable from one index."""
import argparse
import json
from pathlib import Path
import re
import time

from shared.artifacts import write_json

FORMAT = 'dsc-research-dataset-index-v1'
LAYERS = {
    'raw/ethereum': 'Encoded protocol events, failed receipts and Aave replay inputs',
    'decoded/ethereum': 'ABI-decoded events by protocol and contract',
    'state/ethereum': 'Every-block core market state and predecessor checkpoints',
    'reverted/ethereum': 'Complete failed call trees and tracked-contract selections',
    'analysis/ethereum/aave-v3/hf': 'All-wallet HF and all, eMode and non-eMode aggregates',
}
EVIDENCE_ROLES = {'catalog', 'schema', 'node_acceptance', 'drive_acceptance', 'provenance'}


def local_path(value):
    path = Path(value)
    if not path.is_absolute() or '..' in path.parts:
        raise ValueError('Dataset index requires absolute local paths')
    return path


def evidence(reference):
    path = local_path(reference['path'])
    if reference['role'] not in EVIDENCE_ROLES:
        raise ValueError('Unknown dataset evidence role')
    result = dict(role=reference['role'], path=str(path))
    if path.is_file():
        info = path.stat()
        return dict(result, availability='present', bytes=info.st_size, mtime=info.st_mtime)
    if reference.get('pending') is True:
        return dict(result, availability='pending')
    raise FileNotFoundError(path)


def release(entry):
    name = entry['id']
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]*', name):
        raise ValueError('Invalid dataset release ID')
    layers = entry['layers']
    if not layers or len(set(layers)) != len(layers) or any(layer not in LAYERS for layer in layers):
        raise ValueError('Invalid or duplicate dataset layer')
    root = local_path(entry['node'])
    if not root.is_dir():
        raise FileNotFoundError(root)
    references = [evidence(item) for item in entry['evidence']]
    if not {'catalog', 'node_acceptance'} <= {item['role'] for item in references}:
        raise ValueError('Dataset release must reference catalog and Node acceptance')
    return dict(id=name, layers=layers, layout=entry['layout'], node=str(root),
                drive=entry['drive'], evidence=references)


def build(config):
    if config['format'] != FORMAT or config['chain_id'] != 1:
        raise ValueError('Unknown dataset index configuration')
    releases = [release(entry) for entry in config['releases']]
    if not releases or len({entry['id'] for entry in releases}) != len(releases):
        raise ValueError('Missing or duplicate dataset releases')
    return dict(format=FORMAT, chain_id=1, indexed_at=time.time(), layers=LAYERS,
                interval='[start, stop)', releases=releases)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('config', type=Path)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    index = build(json.loads(args.config.read_text()))
    if args.output:
        write_json(args.output, index)
    else:
        print(json.dumps(index, indent=2))


if __name__ == '__main__':
    main()
