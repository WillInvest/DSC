"""Bounded aggregate-only replay over the existing frozen Aave account factors."""
import importlib
import resource
import time
from pathlib import Path

import numpy as np

from .cohorts import independent_summary, summarize
from .validation import FORMAT, encode_part, validate_result

ROOT = Path('/home/dsc-data/runs/aave-user-history-v3/26407881')
REPLAY_MODULES = (
    'materialize_accounts', 'native_batch', 'native_hf', 'hf_batch_f64',
    'hf_aggregates', 'hf_replay_checkpoint', 'hf_f64_contract',
    'accounting_inputs', 'accounting_query', 'gho_replay', 'replay_positions',
)


def source_hashes(expected=None):
    """Identify actual imported replay sources, including deployment overlays.

    Production callers pass their pinned source map. Filename keys and module-name
    keys are both accepted, so existing scientific manifests can be reused.
    """
    from shared.artifacts import checksum
    names = expected.keys() if expected is not None else REPLAY_MODULES
    hashes = {}
    for name in names:
        module_name = name[:-3] if name.endswith('.py') else name
        module = importlib.import_module(module_name)
        actual = checksum(Path(module.__file__))
        if expected is not None and actual != expected[name]:
            raise ValueError('Replay source identity differs: ' + name)
        hashes[name] = actual
    return hashes


def _categories(value, model, evaluator):
    """Read categories only for wallets with debt edges, avoiding a dense scan."""
    return np.fromiter(
        (evaluator.emodes.get(model.owners[int(uid)][0], 0)
         for uid in value['debt_user_ids']),
        dtype=np.int64,
        count=len(value['debt_user_ids']),
    )


def _samples(value, categories, model, evaluator):
    """Select deterministic debt-cohort and debt-free account comparisons."""
    debt_users = value['debt_user_ids']
    choices = []
    for selected in (categories == 0, categories != 0):
        candidates = debt_users[selected]
        if len(candidates):
            choices.append(int(candidates.min()))
    debt_free = np.flatnonzero(value['debt'] == 0)
    if len(debt_free):
        choices.append(int(debt_free[0]))
    result = []
    for uid in sorted(set(choices)):
        owner = model.owners[uid][0]
        reference = evaluator.account(owner)
        h = float(value['hf'][uid])
        selected = debt_users == uid
        result.append(dict(
            user=owner,
            entity_id=uid,
            block=evaluator.block,
            profile=evaluator.profile,
            block_hash=evaluator.block_hash,
            exact_reference=reference['account'],
            hf=h if np.isfinite(h) else '+inf',
            has_debt=bool(value['debt'][uid] > 0),
            emode_category=int(evaluator.emodes.get(owner, 0)),
            included_non_emode=int(evaluator.emodes.get(owner, 0)) == 0,
            debt_base=float(value['debt'][uid]),
            reserve_debt_base=[dict(scope_id=int(r), debt_base=float(d))
                               for r, d in zip(value['debt_reserve_ids'][selected],
                                               value['debt_values'][selected])],
        ))
    return result


def compute(job, checkpoint=None, *, root=ROOT, part_blocks=256, source_pins=None):
    """Return bounded Parquet bytes, source identity and verification evidence.

    The host runner owns generation binding, durable publication, own-node checks
    and catalog acceptance. Jobs use inclusive start and exclusive stop. No input
    or legacy output file is modified, and no RPC is needed to calculate values.
    """
    from shared.artifacts import checksum, digest
    from hf_f64_contract import CONTRACT, CUTOFF_HASH, FACTOR, IMAGE, ORIGIN, REGISTRY, STOP
    from hf_batch_f64 import FloatBatch
    from hf_replay_checkpoint import restore
    from native_batch import BatchEvaluator
    import materialize_accounts as materializer

    began = time.monotonic()
    root = Path(root)
    if not ORIGIN <= job['start'] < job['stop'] <= STOP:
        raise ValueError('Job outside frozen block scope')
    if job['stop'] - job['start'] > 10000 or not 1 <= part_blocks <= 512:
        raise ValueError('Aggregate job or encoding window exceeds bounded scope')
    pinned_sources = source_hashes(source_pins)
    plan = materializer.load(root / 'plan.json')
    if plan['end_block'] != STOP - 1 or plan['end_hash'] != CUTOFF_HASH:
        raise ValueError('Frozen finalized target differs')
    factor, waiting = materializer.factor_input(root, plan, FACTOR)
    if waiting:
        raise ValueError(waiting)
    if factor['through'] + 1 < job['stop']:
        raise ValueError('Factor inputs do not cover requested job')
    if checksum('/home/dsc-data/snapshot-manifest.json') != IMAGE:
        raise ValueError('Frozen execution image differs')
    owners = materializer.owner_universe(root, STOP)
    if digest(owners) != REGISTRY:
        raise ValueError('Frozen wallet dictionary differs')
    model = FloatBatch(owners, plan['jobs'])
    database = materializer.readonly(root / 'accounting.sqlite')
    evaluator = None
    parts, samples, proofs = [], [], []
    rows = fallback_count = dust_count = 0
    advance_seconds = arithmetic_seconds = encoding_seconds = 0.0
    sample_blocks = {job['start'], (job['start'] + job['stop'] - 1) // 2, job['stop'] - 1}
    try:
        if checkpoint is None:
            evaluator = BatchEvaluator(root, database, plan, job['start'], job['stop'])
        else:
            evaluator = restore(root, database, plan, job['start'], job['stop'],
                                checkpoint['path'], checkpoint['sha256'])
        initialized = time.monotonic()
        for start in range(job['start'], job['stop'], part_blocks):
            stop = min(job['stop'], start + part_blocks)
            block_results = []
            for block in range(start, stop):
                at = time.monotonic()
                evaluator.advance(block)
                advance_seconds += time.monotonic() - at
                at = time.monotonic()
                value = model.evaluate(evaluator, dust_cutoff=CONTRACT['dust_debt_base_exclusive'])
                categories = _categories(value, model, evaluator)
                cohorts = summarize(value['hf'], value['debt_user_ids'],
                                    value['debt_reserve_ids'], value['debt_values'],
                                    categories, len(model.reserves))
                block_results.append(dict(block=block, cohorts=cohorts,
                                          excluded_dust_users=value['excluded_users'],
                                          excluded_dust_debt_base=value['excluded_debt_base']))
                fallback_count += value['exact_fallback_users']
                dust_count += value['excluded_users']
                if block in sample_blocks:
                    reference = independent_summary(
                        value['hf'], value['debt_user_ids'], value['debt_reserve_ids'],
                        value['debt_values'], categories, len(model.reserves))
                    proofs.append(dict(block=block, cohorts=reference))
                    samples.extend(_samples(value, categories, model, evaluator))
                arithmetic_seconds += time.monotonic() - at
            at = time.monotonic()
            part = encode_part(block_results, start, stop, len(model.reserves))
            parts.append(part)
            rows += part['rows']
            encoding_seconds += time.monotonic() - at
        result = dict(
            job=dict(job), format=FORMAT, rows=rows, parts=parts,
            samples=samples, cohort_proofs=proofs, reserves=model.reserves,
            registry_digest=REGISTRY, factor_input=factor,
            snapshot_manifest_sha256=IMAGE, replay_sources=pinned_sources,
            policy=dict(non_emode='account category equals zero at end of block',
                        dust_debt_base_exclusive=CONTRACT['dust_debt_base_exclusive'],
                        base_currency_unit=CONTRACT['base_currency_unit'],
                        reserve_attribution=CONTRACT['reserve_attribution']),
            checkpoint_used=checkpoint,
            diagnostics=dict(exact_fallback_user_blocks=fallback_count,
                             excluded_dust_user_blocks=dust_count),
            timing=dict(init_seconds=initialized - began,
                        advance_seconds=advance_seconds,
                        arithmetic_seconds=arithmetic_seconds,
                        encoding_seconds=encoding_seconds,
                        total_seconds=time.monotonic() - began),
            peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            bytes=sum(len(part['data']) for part in parts),
            production_coverage=False,
        )
        validate_result(result)
        return result
    finally:
        if evaluator is not None:
            evaluator.close()
        database.close()
