"""Bounded Parquet encoding and independent acceptance of cohort sufficient statistics."""
import hashlib
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from .cohorts import COHORTS

FORMAT = 'hf-market-cohort-v1'
COLUMNS = ['block', 'scope_id', 'cohort', 'numerator', 'debt_base', 'hf',
           'borrower_count', 'excluded_dust_users', 'excluded_dust_debt_base']
SCHEMA = pa.schema([
    ('block', pa.uint32()), ('scope_id', pa.uint16()), ('cohort', pa.string()),
    ('numerator', pa.float64()), ('debt_base', pa.float64()), ('hf', pa.float64()),
    ('borrower_count', pa.uint32()), ('excluded_dust_users', pa.uint32()),
    ('excluded_dust_debt_base', pa.float64()),
])


def encode_part(blocks, start, stop, reserve_count):
    """Encode bounded block/scope/cohort rows, preserving zero-denominator nulls."""
    if [item['block'] for item in blocks] != list(range(start, stop)):
        raise ValueError('Noncontiguous aggregate input blocks')
    rows = {column: [] for column in COLUMNS}
    for item in blocks:
        for scope in range(reserve_count + 1):
            for cohort in COHORTS:
                value = item['cohorts'][cohort]
                rows['block'].append(item['block'])
                rows['scope_id'].append(scope)
                rows['cohort'].append(cohort)
                for name in ('numerator', 'debt_base', 'borrower_count'):
                    rows[name].append(value[name][scope])
                h = value['hf'][scope]
                rows['hf'].append(None if np.isnan(h) else float(h))
                global_diagnostics = scope == reserve_count and cohort == 'all'
                rows['excluded_dust_users'].append(
                    item['excluded_dust_users'] if global_diagnostics else None)
                rows['excluded_dust_debt_base'].append(
                    item['excluded_dust_debt_base'] if global_diagnostics else None)
    schema = SCHEMA.with_metadata({
        b'format': FORMAT.encode(), b'start': str(start).encode(), b'stop': str(stop).encode(),
        b'cohort_policy': b'non_emode means account category equals zero at end of block',
        b'base_currency_unit': b'100000000',
        b'numerator_unit': b'base_currency_times_dimensionless_user_HF',
        b'protocol_scope_id': str(reserve_count).encode(),
        b'dust_debt_base_exclusive': b'1000000',
    })
    table = pa.table(rows, schema=schema)
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression='zstd', compression_level=9,
                   row_group_size=65536, use_dictionary=['cohort', 'scope_id'])
    raw = sink.getvalue().to_pybytes()
    return dict(name=f'aggregates-{start}-{stop - 1}.parquet', data=raw,
                sha256=hashlib.sha256(raw).hexdigest(), start=start, stop=stop,
                rows=table.num_rows, bytes=len(raw))


def _raw(part):
    raw = part.get('data')
    if raw is None:
        raw = Path(part['path']).read_bytes()
    if hashlib.sha256(raw).hexdigest() != part['sha256']:
        raise ValueError('Aggregate part checksum differs')
    if 'bytes' in part and len(raw) != part['bytes']:
        raise ValueError('Aggregate part byte count differs')
    return raw


def validate_result(result):
    """Validate exact block/scope membership, cohort partitions and sample proofs.

    Fresh own-node getter comparison is exposed separately as validate_node_samples.
    This function reads in-memory bytes or persisted path descriptors interchangeably.
    """
    if result['format'] != FORMAT:
        raise ValueError('Wrong cohort format')
    job = result['job']
    cursor = job['start']
    scope_count = len(result['reserves']) + 1
    if result['reserves'] != sorted(set(result['reserves'])):
        raise ValueError('Reserve dictionary differs')
    proof_map = {p['block']: p['cohorts'] for p in result.get('cohort_proofs', [])}
    if len(proof_map) != len(result.get('cohort_proofs', [])):
        raise ValueError('Duplicate checkpoint reduction proof')
    required_proofs = {job['start'], (job['start'] + job['stop'] - 1) // 2, job['stop'] - 1}
    if set(proof_map) != required_proofs:
        raise ValueError('Missing independent checkpoint reductions')
    checked_proofs = set()
    row_count = 0
    for part in result['parts']:
        start, stop = part['start'], part['stop']
        if start != cursor or stop <= start or stop > job['stop']:
            raise ValueError('Aggregate part gap, overlap or scope mismatch')
        table = pq.read_table(pa.BufferReader(_raw(part)))
        metadata = table.schema.metadata or {}
        if not table.schema.equals(SCHEMA, check_metadata=False) or metadata.get(b'format') != FORMAT.encode():
            raise ValueError('Aggregate schema differs')
        if metadata.get(b'start') != str(start).encode() or metadata.get(b'stop') != str(stop).encode():
            raise ValueError('Parquet range differs from part range')
        if metadata.get(b'protocol_scope_id') != str(scope_count - 1).encode():
            raise ValueError('Protocol dictionary differs')
        if metadata.get(b'base_currency_unit') != b'100000000' or metadata.get(b'dust_debt_base_exclusive') != b'1000000':
            raise ValueError('Valuation or dust units differ')
        length = stop - start
        expected_rows = length * scope_count * len(COHORTS)
        if table.num_rows != expected_rows or part['rows'] != expected_rows:
            raise ValueError('Aggregate row coverage differs')
        if not np.array_equal(table['block'].to_numpy(), np.repeat(np.arange(start, stop), scope_count * 3)):
            raise ValueError('Block row ordering differs')
        if not np.array_equal(table['scope_id'].to_numpy(), np.tile(np.repeat(np.arange(scope_count), 3), length)):
            raise ValueError('Scope row coverage differs')
        if table['cohort'].to_pylist() != list(COHORTS) * (length * scope_count):
            raise ValueError('Cohort row coverage differs')
        values = {}
        for name in ('numerator', 'debt_base', 'hf', 'borrower_count'):
            if name != 'hf' and table[name].null_count:
                raise ValueError('Missing aggregate sufficient statistic')
            values[name] = table[name].to_numpy(zero_copy_only=False).reshape(length, scope_count, 3)
        numerator, debt, hf, counts = (values[name] for name in ('numerator', 'debt_base', 'hf', 'borrower_count'))
        if np.any(numerator < 0) or np.any(debt < 0) or not np.all(np.isfinite(numerator)) or not np.all(np.isfinite(debt)):
            raise ValueError('Invalid aggregate sufficient statistics')
        positive = debt > 0
        if not np.array_equal(np.isnan(hf), ~positive) or np.any(numerator[~positive] != 0):
            raise ValueError('Zero-debt HF/null semantics differ')
        if not np.allclose(hf[positive], numerator[positive] / debt[positive], rtol=1e-10, atol=1e-10):
            raise ValueError('HF does not equal numerator divided by debt')
        if not np.array_equal(counts > 0, positive):
            raise ValueError('Borrower counts disagree with positive debt')
        for name in ('numerator', 'debt_base'):
            a = values[name]
            if not np.allclose(a[:, :, 0], a[:, :, 1] + a[:, :, 2], rtol=1e-10, atol=1e-10):
                raise ValueError('All-account total differs from cohort partitions')
            if not np.allclose(a[:, -1, :], a[:, :-1, :].sum(axis=1), rtol=1e-10, atol=1e-10):
                raise ValueError('Protocol total differs from reserve reduction')
        if not np.array_equal(counts[:, :, 0], counts[:, :, 1] + counts[:, :, 2]):
            raise ValueError('Cohort borrower counts do not partition')
        for block, proofs in proof_map.items():
            if not start <= block < stop:
                continue
            for category, cohort in enumerate(COHORTS):
                for name in ('numerator', 'debt_base', 'borrower_count'):
                    actual = values[name][block - start, :, category]
                    expected = proofs[cohort][name]
                    if not np.allclose(actual, expected, rtol=1e-10, atol=1e-10):
                        raise ValueError('Independent cohort reduction differs')
            checked_proofs.add(block)
        dust_counts = table['excluded_dust_users'].to_pylist()
        dust_debt = table['excluded_dust_debt_base'].to_pylist()
        stride = scope_count * 3
        for offset, (count, amount) in enumerate(zip(dust_counts, dust_debt)):
            diagnostic = offset % stride == (scope_count - 1) * 3
            if diagnostic:
                if count is None or amount is None or count < 0 or not np.isfinite(amount) or not 0 <= amount <= count * 1_000_000:
                    raise ValueError('Invalid account dust diagnostics')
            elif count is not None or amount is not None:
                raise ValueError('Dust diagnostics repeated outside protocol all-account row')
        cursor = stop
        row_count += table.num_rows
    if cursor != job['stop'] or checked_proofs != set(proof_map):
        raise ValueError('Incomplete aggregate or checkpoint coverage')
    return dict(structural_exact=True, aggregate_blocks=cursor - job['start'],
                rows=row_count, reserve_count=scope_count - 1,
                independent_checkpoint_blocks=len(checked_proofs))


def validate_node_samples(samples, job, *, expected_headers=None):
    """Compare original account metrics, debt weights and eMode with own node.

    The existing account validator owns all historical account/HF comparisons.
    Its generic snapshot API also supports the additional getUserEMode calls.
    The caller can instrument HTTPProvider once around this whole function to
    record actual RPC counts without mixing them with calculation timings.
    """
    from hf_f64_accept import node_check
    import failover_hf
    import materialize_accounts as materializer
    from accounting_inputs import profiles
    from vector_accept import NODE_LOCK

    header_options = {} if expected_headers is None else {'expected_headers': expected_headers}
    report = node_check(samples, job, **header_options)
    for sample in samples:
        actual = float(sample['debt_base'])
        expected = int(sample['exact_reference'][1])
        if expected < 1_000_000:
            expected = 0
        if not np.isfinite(actual) or abs(actual - expected) > 1e-8 + 1e-6 * abs(expected):
            raise ValueError('Sampled model debt differs from node-validated account debt')
        edges = sample['reserve_debt_base']
        if not np.isclose(sum(edge['debt_base'] for edge in edges), actual,
                          rtol=1e-10, atol=1e-10):
            raise ValueError('Sampled reserve debts do not recover account debt')
        if sample['included_non_emode'] != (sample['emode_category'] == 0):
            raise ValueError('Sampled cohort selection differs from category')
    category_checks = []
    if samples:
        plan = materializer.load(failover_hf.ROOT / 'plan.json')
        selected_profiles = {sample['profile'] for sample in samples}
        abis = {p['key']: p['abi'] for p in profiles() if p['key'] in selected_profiles}
        tasks = [dict(kind='call', function='getUserEMode', args=[sample['user']],
                      profile=sample['profile'], block=sample['block'])
                 for sample in samples]
        with NODE_LOCK:
            references = failover_hf.direct_node(
                plan, dict(operation='snapshots', abis=abis, tasks=tasks))
        if len(references) != len(samples):
            raise ValueError('Node omitted eMode category samples')
        for sample, reference in zip(samples, references):
            if int(reference['result']) != sample['emode_category']:
                raise ValueError('Historical eMode category differs from own node')
            if reference['block_hash'].lower() != sample['block_hash'].lower():
                raise ValueError('eMode sample block hash differs')
            category_checks.append(dict(user=sample['user'], block=sample['block'],
                                        category=sample['emode_category'], exact=True))
    return dict(report, emode_category_samples=len(category_checks),
                emode_category_checks=category_checks, debt_attribution_samples=len(samples))
