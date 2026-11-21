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


def run(config):
    market = DATA/'metadata/market'
    hf = DATA/'lending/aaveV3/health-factor'
    catalog = market/'canonical'
    registry = SCRIPTS.parent/'contracts/ethereum/pools.csv'
    environment = dict(os.environ, DSC_PIPELINE_SITE='node', DSC_PIPELINE_SCRIPTS=str(SCRIPTS),
                       DSC_PIPELINE_REVERT_SSH_CONFIG=config['revert_ssh_config'],
                       PYTHONPATH=os.pathsep.join(map(str, (SCRIPTS, SCRIPTS/'user-history'))))

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
           '--work', hf/'compute')
    invoke('pipeline.compaction.backup_catalog', 'follow', market,
           DATA/'metadata/market-drive', config['market_drive'], '--once')
    invoke('hf_incremental_archive', '--continuous', '--once', '--windows', hf/'windows',
           '--work', hf/'drive', '--remote', config['hf_drive'], '--start', config['hf_backup_start'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('check', 'run'))
    parser.add_argument('--config', type=Path)
    args = parser.parse_args()
    if args.mode == 'run' and not args.config:
        parser.error('run requires --config')
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
        run(config)


if __name__ == '__main__':
    main()
