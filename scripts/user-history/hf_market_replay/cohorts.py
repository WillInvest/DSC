"""Debt-edge cohort selection without changing any wallet's calculated HF."""
import math

import numpy as np

from hf_aggregates import aggregate

COHORTS = ('all', 'non_emode', 'emode')


def _edges(hf, user_ids, reserve_ids, debt_values, edge_categories, reserve_count):
    hf = np.asarray(hf, dtype=np.float64)
    users = np.asarray(user_ids, dtype=np.int64)
    reserves = np.asarray(reserve_ids, dtype=np.int64)
    debt = np.asarray(debt_values, dtype=np.float64)
    categories = np.asarray(edge_categories, dtype=np.int64)
    if any(a.ndim != 1 for a in (hf, users, reserves, debt, categories)):
        raise ValueError('Expected one-dimensional wallet and debt arrays')
    if not (len(users) == len(reserves) == len(debt) == len(categories)):
        raise ValueError('Debt edge dimensions differ')
    if reserve_count < 1 or np.any(users < 0) or np.any(users >= len(hf)):
        raise ValueError('Debt wallet outside registry')
    if np.any(reserves < 0) or np.any(reserves >= reserve_count):
        raise ValueError('Debt reserve outside dictionary')
    if np.any(categories < 0) or np.any(categories > 255):
        raise ValueError('Invalid eMode category')
    if np.any(debt < 0) or not np.all(np.isfinite(debt)):
        raise ValueError('Invalid debt values')
    # One account category governs every borrowed reserve at the same block.
    if len(users):
        order = np.argsort(users, kind='stable')
        same_user = users[order][1:] == users[order][:-1]
        if np.any(same_user & (categories[order][1:] != categories[order][:-1])):
            raise ValueError('Conflicting category for the same wallet')
    positive = debt > 0
    return hf, users[positive], reserves[positive], debt[positive], categories[positive]


def summarize(hf, user_ids, reserve_ids, debt_values, edge_categories, reserve_count):
    """Return reserve rows followed by protocol, for all and both eMode cohorts.

    Debt edges must already have passed the original account-level dust policy.
    Categories are selected at the end of the current block. Each cohort's HF is
    NaN when its denominator is zero, ready for encoding as a Parquet null.
    """
    hf, users, reserves, debt, categories = _edges(
        hf, user_ids, reserve_ids, debt_values, edge_categories, reserve_count
    )
    masks = dict(all=np.ones(len(users), dtype=bool),
                 non_emode=categories == 0, emode=categories != 0)
    result = {}
    for cohort in COHORTS:
        selected = masks[cohort]
        u, r, d = users[selected], reserves[selected], debt[selected]
        reduced = aggregate(hf, u, r, d, reserve_count)
        numerators = np.append(reduced['numerator'], reduced['protocol_numerator'])
        denominators = np.append(reduced['denominator'], reduced['protocol_denominator'])
        values = np.full(reserve_count + 1, np.nan)
        np.divide(numerators, denominators, out=values, where=denominators > 0)
        # Unique wallet/reserve pairs also support future multi-edge debt sources.
        pairs = np.unique(u * reserve_count + r)
        counts = np.bincount(pairs % reserve_count, minlength=reserve_count)
        result[cohort] = dict(
            numerator=numerators,
            debt_base=denominators,
            hf=values,
            borrower_count=np.append(counts, len(np.unique(u))).astype(np.uint32),
        )
    return result


def independent_summary(hf, user_ids, reserve_ids, debt_values, edge_categories, reserve_count):
    """Checkpoint reference using Python fsum over explicitly selected wallets.

    This reduction bypasses the vectorized aggregate function and its cohort masks.
    It checks the same raw debt edges with independent sums and membership tests.
    """
    groups = {cohort: [[] for _ in range(reserve_count + 1)] for cohort in COHORTS}
    for user, reserve, debt, category in zip(user_ids, reserve_ids, debt_values, edge_categories):
        if debt <= 0:
            continue
        cohort = 'non_emode' if int(category) == 0 else 'emode'
        edge = (int(user), float(debt), float(hf[user]))
        for group in ('all', cohort):
            groups[group][int(reserve)].append(edge)
            groups[group][-1].append(edge)
    result = {}
    for cohort in COHORTS:
        numerator, debt, counts = [], [], []
        for edges in groups[cohort]:
            numerator.append(math.fsum(d * h for _, d, h in edges))
            debt.append(math.fsum(d for _, d, _ in edges))
            counts.append(len({user for user, _, _ in edges}))
        result[cohort] = dict(numerator=numerator, debt_base=debt, borrower_count=counts)
    return result
