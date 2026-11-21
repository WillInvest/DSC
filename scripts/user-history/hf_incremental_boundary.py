"""Seal the accepted historical Aave HF state for the first incremental interval."""
import argparse
from contextlib import closing
import fcntl
import gzip
import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile

from shared.artifacts import checksum, digest, write_json

FORMAT = 'aave-v3-hf-boundary-v1'


def seal_record(evaluator, generation, last_block, last_hash, input_identity, output):
    from hf_replay_checkpoint import state_of

    if evaluator.block != last_block or evaluator.block_hash.lower() != last_hash.lower():
        raise ValueError('Evaluator is not at the accepted boundary')
    record = dict(format=FORMAT, release_generation=generation,
                  exclusive_before_block=last_block+1, predecessor_hash=last_hash,
                  input_identity=input_identity, state=state_of(evaluator),
                  reserves=evaluator.reserves)
    output = Path(output)
    with NamedTemporaryFile(dir=output.parent) as temporary:
        with gzip.GzipFile(fileobj=temporary, mode='wb', filename='', mtime=0) as stream:
            stream.write(json.dumps(record, sort_keys=True, separators=(',', ':')).encode())
        temporary.flush()
        os.fsync(temporary.fileno())
        if output.exists():
            if checksum(output) != checksum(temporary.name):
                raise ValueError('Existing boundary differs')
        else:
            os.link(temporary.name, output)
    return dict(path=str(output), sha256=checksum(output), bytes=output.stat().st_size,
                position_owners=len(record['state']['positions']),
                emode_owners=len(record['state']['emodes']), reserves=len(record['reserves']))


def read_boundary(path, sha256, start, predecessor_hash, generation):
    if checksum(path) != sha256:
        raise ValueError('Boundary file checksum differs')
    with gzip.open(path, 'rt') as stream:
        record = json.load(stream)
    if (record['format'], record['exclusive_before_block'], record['predecessor_hash'].lower(),
            record['release_generation']) != (FORMAT, start, predecessor_hash.lower(), generation):
        raise ValueError('Boundary does not join this incremental interval')
    return record


def build(plan_path, archive, image, role, output):
    from hf_f64_contract import CONTRACT, CUTOFF_HASH, FACTOR, IMAGE, REGISTRY, STOP
    from hf_batch_f64 import FloatBatch
    from hf_market_replay.archive import _completed_report
    from hf_market_replay.calculation import _categories, _samples, source_hashes
    from hf_market_replay.validation import validate_node_samples
    from hf_replay_checkpoint import restore
    import materialize_accounts as materializer

    output.mkdir(parents=True, exist_ok=True)
    with (output/'boundary.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        cohort = materializer.load(plan_path)
        generation = cohort['generation']
        scope = cohort['identity']['scope']
        if (digest(cohort['identity']) != generation or scope['stop'] != STOP
                or scope['end_hash'] != CUTOFF_HASH or not _completed_report(archive, cohort)):
            raise ValueError('Historical cohort release is not accepted at the requested boundary')
        root = image/'runs/aave-user-history-v3'/str(STOP-1)
        plan = materializer.load(root/'plan.json')
        if (plan['end_block'], plan['end_hash']) != (STOP-1, CUTOFF_HASH):
            raise ValueError('Accounting plan boundary differs')
        if checksum(image/'snapshot-manifest.json') != IMAGE:
            raise ValueError('Historical snapshot identity differs')
        sources = source_hashes(cohort['source_pins'])
        factor, waiting = materializer.factor_input(root, plan, FACTOR)
        if waiting or factor['through'] != STOP-1:
            raise ValueError(waiting or 'Historical factors end before the accepted boundary')
        owners = materializer.owner_universe(root, STOP)
        if digest(owners) != REGISTRY:
            raise ValueError('Historical wallet dictionary differs')
        candidates = [value for value in cohort['original_checkpoints']
                      if value['role'] == role and value['block'] < STOP]
        if not candidates:
            raise ValueError('No accepted replay seed for this worker')
        seed = max(candidates, key=lambda value: value['block'])
        identity = dict(snapshot_manifest_sha256=IMAGE, historical_plan_sha256=checksum(plan_path),
                        accepted_sha256=checksum(archive/'accepted.json'), wallet_registry_sha256=REGISTRY,
                        factor_receipt_sha256=factor['sha256'], replay_sources_sha256=digest(sources),
                        seed_sha256=seed['sha256'])
        receipt = output/'boundary.json'
        if receipt.exists():
            saved = materializer.load(receipt)
            if saved['input_identity'] != identity or saved['release_generation'] != generation:
                raise ValueError('Existing boundary input identity differs')
            if checksum(output/'state.json.gz') != saved['checkpoint']['sha256']:
                raise ValueError('Existing boundary checksum differs')
            return saved
        job = dict(start=STOP-1, stop=STOP)
        with closing(materializer.readonly(root/'accounting.sqlite')) as database:
            with closing(restore(root,database,plan,STOP-1,STOP,seed['path'],seed['sha256'])) as evaluator:
                evaluator.advance(STOP-1)
                model = FloatBatch(owners, plan['jobs'])
                value = model.evaluate(evaluator, dust_cutoff=CONTRACT['dust_debt_base_exclusive'])
                samples = _samples(value, _categories(value,model,evaluator), model, evaluator)
                if not samples:
                    raise ValueError('No historical account samples for the boundary')
                validation = validate_node_samples(samples, job)
                checkpoint = seal_record(evaluator,generation,STOP-1,CUTOFF_HASH,identity,
                                         output/'state.json.gz')
        result = dict(format=FORMAT, release_generation=generation, next_block=STOP,
                      predecessor_hash=CUTOFF_HASH, input_identity=identity, checkpoint=checkpoint,
                      historical_wallets=len(owners), validation=validation)
        write_json(receipt, result)
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ('plan', 'archive', 'image', 'output'):
        parser.add_argument('--'+flag, required=True, type=Path)
    parser.add_argument('--role', required=True)
    args = parser.parse_args()
    result = build(args.plan,args.archive,args.image,args.role,args.output.resolve())
    print(json.dumps({key: result[key] for key in ('next_block','predecessor_hash','checkpoint','historical_wallets')}))


if __name__ == '__main__':
    main()
