"""
strategies_config_b.py — Prod B-test variants (B only)
"""
from dataclasses import replace
from strategies_config import STRATEGIES, StrategyConfig

_prod_map = {s.label: s for s in STRATEGIES}

def _tweak(label, **kw):
    base = _prod_map[label]
    tag = '  '.join(f'{k}={v}' for k, v in kw.items())
    return replace(base, name=f"{label} [B: {tag[:60]}]", **kw)

STRATEGIES_B = [
    # B score threshold variants
    _tweak('B', min_score=25.0, live_exec=False),
    _tweak('B', min_score=40.0, live_exec=False),
    # B short-only monitor
    replace(_prod_map['B'], name='B [B: short-only monitor]',
            long_only=False, short_only=True, live_exec=False),
    # B no-blacklist
    _tweak('B', symbol_blacklist=frozenset(), live_exec=False),
]
