#!/usr/bin/env python3
"""Run one DSC research pass. A scheduler can invoke this command later."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import subprocess

HOME = Path.home()
DATA = Path('/mnt/storage/dsc-data')
SCRIPTS = Path(__file__).resolve().parent/'engine/scripts'
PYTHON = HOME/'dsc-extraction/.venv-abi/bin/python'


def check_storage():
    if (not DATA.parent.is_mount() or not DATA.is_dir()
            or not (HOME/'dsc-data').samefile(DATA)
            or DATA.stat().st_dev == HOME.stat().st_dev):
        raise ValueError('DSC data disk is not mounted at the configured storage paths')


def check_hf_opening(baseline_path, opening_path):
    market = json.loads(Path(baseline_path).read_text())
    hf = json.loads((Path(opening_path)/'boundary.json').read_text())
    if (hf['next_block'], hf['predecessor_hash']) != (market['end_block']+1, market['end_hash']):
        raise ValueError('HF opening block and hash must join the sealed market baseline')
    return market['end_block']


def check_published_baseline(metadata, boundary):
    publication = json.loads((metadata/'publication.json').read_text())
    encoded = json.loads((metadata/'encoded-publication.json').read_text())
    through = publication.get('market_through', -1)
    if (publication['stage'] != 'complete' or boundary > through
            or publication.get('revert_through') != through):
        raise ValueError('Published market and Revert data must cover the sealed baseline together')
    if encoded['stage'] != 'complete' or encoded.get('market_through', -1) < through:
        raise ValueError('Published raw events must cover the accepted market boundary')


def run(config, *, local_only=False):
    image = config.get('hf_input_image')
    if image is not None and not Path('/home/dsc-data').samefile(image):
        raise ValueError('Mount the configured HF input image read-only at /home/dsc-data')
    boundary = check_hf_opening(config['market_baseline'], config['hf_opening'])
    check_published_baseline(DATA/'metadata', boundary)
    module_paths = [SCRIPTS, SCRIPTS/'user-history']
    if overlay := config.get('hf_python_overlay'):
        module_paths.append(Path(overlay))

    market = DATA/'metadata/market'
    hf = DATA/'lending/aaveV3/health-factor'
    catalog = market/'canonical'
    registry = SCRIPTS.parent/'contracts/ethereum/pools.csv'
    environment = dict(os.environ, DSC_PIPELINE_SITE='node', DSC_PIPELINE_SCRIPTS=str(SCRIPTS),
                       DSC_PIPELINE_REVERT_SSH_CONFIG=config['revert_ssh_config'],
                       PYTHONPATH=os.pathsep.join(map(str, module_paths)))

    def invoke(module, *arguments):
        subprocess.run([str(PYTHON), '-u', '-m', module, *map(str, arguments)],
                       env=environment, check=True)

    invoke('pipeline.main', 'once', '--root', market,
           '--baseline-plan', config['market_baseline'], '--profiles', config['market_profiles'],
           '--node-verification', config['market_verification'],
           '--node-production-fence', config['production_fence'])
    invoke('pipeline.research_layout', '--root', DATA, '--coordinator', market, '--registry', registry)
    invoke('hf_incremental_follow', '--continuous', '--once', '--catalog', catalog,
           '--opening', config['hf_opening'], '--historical-root', config['hf_history'],
           '--cryo', config['cryo'], '--helpers', config['hf_helpers'], '--windows', hf/'windows',
           '--work', hf/'compute', '--local-accepted', hf/'local-accepted')
    if local_only:
        return
    invoke('pipeline.compaction.backup_catalog', 'follow', market,
           DATA/'metadata/market-drive', config['market_drive'], '--once')
    invoke('hf_incremental_archive', '--continuous', '--once', '--windows', hf/'windows',
           '--work', hf/'drive', '--remote', config['hf_drive'], '--start', config['hf_backup_start'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('check', 'run'))
    parser.add_argument('--config', type=Path)
    parser.add_argument('--local-only', action='store_true', help='Run through local HF computation')
    args = parser.parse_args()
    if args.mode == 'run' and not args.config:
        parser.error('run requires --config')
    if args.local_only and args.mode != 'run':
        parser.error('--local-only requires run')
    check_storage()
    if args.mode == 'check':
        print(f'Scripts: {SCRIPTS.parent.parent}')
        print(f'Data: {DATA}')
        print(f'Market catalog: {DATA / "metadata/market/canonical"}')
        print(f'Aave HF: {DATA / "lending/aaveV3/health-factor"}')
        return
    config = json.loads(args.config.read_text())
    with (DATA/'metadata/scheduled-pass.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        run(config, local_only=args.local_only)


if __name__ == '__main__':
    main()
