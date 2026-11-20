"""Prepare an immutable aggregate replay plan from the accepted frozen inputs."""
import argparse
import json
import sqlite3
from pathlib import Path

from shared.artifacts import checksum, digest, write_json

SCIENTIFIC_SOURCES = ('__init__.py', 'calculation.py', 'cohorts.py', 'validation.py')
OPERATIONAL_SOURCES = ('__main__.py', 'ledger.py', 'runner.py', 'coordinator.py',
                       'dispatch.py', 'transport.py')


def prepare(parent_plan, image, output, block_window=1000):
    """Keep historical scope and math while choosing bounded aggregate jobs."""
    parent_plan, image, output = Path(parent_plan), Path(image), Path(output)
    original = json.loads(parent_plan.read_text())
    contract = original['identity']['contract']
    if checksum(image / 'snapshot-manifest.json') != contract['snapshot_manifest_sha256']:
        raise ValueError('Frozen replay image identity differs')
    if not 1 <= block_window <= 10000:
        raise ValueError('Aggregate job window outside supported bounds')
    start, stop = contract['block_origin'], contract['end_block'] + 1
    database_path = image / 'runs/aave-user-history-v3/26407881/accounting.sqlite'
    with sqlite3.connect('file:' + str(database_path) + '?mode=ro', uri=True) as database:
        database.execute('begin')
        upgrades = [row[0] for row in database.execute(
            "select distinct block from pool_events where name='Upgraded' order by block")]
        database.rollback()
    boundaries = sorted({start, stop, 16800000, 16807201, 25199900,
                         *[block for block in upgrades if start < block < stop]})
    jobs = []
    for first, last in zip(boundaries, boundaries[1:]):
        for block in range(first, last, block_window):
            end = min(block + block_window, last)
            jobs.append(dict(id=f'{block}-{end - 1}', start=block, stop=end))
    sources = original['identity']['sources']
    package = Path(__file__).parent
    identity = dict(
        method='aave-v3-market-hf-cohort-replay-v1',
        scope=dict(start=start, stop=stop, end_hash=contract['end_hash']),
        policy=dict(cohorts=['all', 'non_emode', 'emode'],
                    non_emode='account eMode category equals zero at block end',
                    parent_contract=contract),
        parent_plan_sha256=checksum(parent_plan),
        parent_generation=original['generation'],
        jobs_sha256=digest(jobs), source_pins_sha256=digest(sources),
        encoding=dict(part_blocks=256, parquet='zstd9', schema='hf-market-cohort-v1'),
        runtime=dict(numpy=original['identity']['numpy'], pyarrow='24.0.0'),
        package_sources={name: checksum(package / name) for name in SCIENTIFIC_SOURCES},
    )
    plan = dict(generation=digest(identity), identity=identity, jobs=jobs,
                source_pins=sources, original_checkpoints=original['checkpoints'],
                operational_sources={name: checksum(package / name)
                                     for name in OPERATIONAL_SOURCES})
    if output.exists():
        if json.loads(output.read_text()) != plan:
            raise ValueError('Refuse to replace an immutable replay plan')
    else:
        write_json(output, plan)
    return plan


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--parent-plan', required=True)
    parser.add_argument('--image', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--block-window', type=int, default=1000)
    args = parser.parse_args()
    plan = prepare(args.parent_plan, args.image, args.output, args.block_window)
    print(json.dumps(dict(generation=plan['generation'], jobs=len(plan['jobs']),
                          scope=plan['identity']['scope'])))


if __name__ == '__main__':
    main()
