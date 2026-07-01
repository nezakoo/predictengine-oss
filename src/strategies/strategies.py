"""
PredictEngine - strategies.py
Thin public API wrapper. Import this file everywhere.

Internal split:
  strategies_config.py  — StrategyConfig dataclass + STRATEGIES list
  strategies_signals.py — all signal detection functions (K–Z)
  strategies_engine.py  — StrategyEngine class + global tick/check/snapshot

This file just re-exports the public API so all existing imports
(dashboard_multi, predict_engine, engine) continue to work unchanged.
"""

from strategies_config import StrategyConfig, STRATEGIES  # noqa: F401
from strategies_signals import *                           # noqa: F401,F403  (signal fns)
from strategies_engine  import StrategyEngine              # noqa: F401
from strategies_runtime import (                           # noqa: F401
    init_strategies,
    get_engines,
    tick_all,
    check_all,
    snapshots_all,
    config_watcher_task,
)
import strategies_engine as _se

# _engines_a / _engines_b are globals in strategies_engine that get reassigned
# by init_strategies(). Expose them as module-level properties so callers doing
# `import strategies as S; S._engines_a` always see the current list, not a
# stale copy taken at import time.
def __getattr__(name):
    if name == '_engines_a': return _se._engines_a
    if name == '_engines_b': return _se._engines_b
    raise AttributeError(name)
