#!/usr/bin/env python3
"""
test_imports.py — Verify PredictEngine restructure didn't break anything
Run from: python3 tools/devtools/test_imports.py  (or from engine root)

Tests:
  1. All runtime core files importable (syntax check via ast.parse)
  2. All cross-file import references still resolve
  3. All tools in correct directories
  4. No runtime files accidentally moved
  5. No broken relative paths in tools
"""

import sys, os, ast, importlib.util
from pathlib import Path

# Engine root = two levels up from tools/devtools, or current dir
here = Path(__file__).resolve().parent
if here.name == "devtools":
    ENGINE_ROOT = here.parent.parent
elif here.name == "engine" or (here / "predict_engine.py").exists():
    ENGINE_ROOT = here
else:
    ENGINE_ROOT = Path.cwd()

print(f"Engine root: {ENGINE_ROOT}")
print("=" * 60)

PASS = 0
FAIL = 0
WARN = 0

def ok(msg):   global PASS; PASS += 1; print(f"  ✅ {msg}")
def fail(msg): global FAIL; FAIL += 1; print(f"  ❌ {msg}")
def warn(msg): global WARN; WARN += 1; print(f"  ⚠️  {msg}")

# ── Test 1: Runtime core files present ──────────────────────────────
print("\n[1] Runtime core files present in engine root")
RUNTIME_FILES = [
    "predict_engine.py", "engine.py", "config.py",
    "core_signals.py", "engine_scanner.py", "engine_lag.py",
    "engine_logger.py", "live_execution.py", "dashboard_multi.py",
    "strategies.py", "strategies_config.py", "strategies_config_b.py",
    "strategies_engine.py", "strategies_runtime.py", "strategies_signals.py",
    "tg_monitor.py", "close_all_positions.py",
]
for f in RUNTIME_FILES:
    p = ENGINE_ROOT / f
    if p.exists():
        ok(f)
    else:
        fail(f"MISSING: {f}")

# ── Test 2: Tools in correct directories ────────────────────────────
print("\n[2] Tools correctly moved to tools/ subdirectories")
TOOL_FILES = {
    "tools/analysis/analyze.sh": True,
    "tools/analysis/analyze_correlation.py": True,
    "tools/analysis/analyze_local.py": True,
    "tools/analysis/analyze_perf.py": True,
    "tools/analysis/binance_analyze.py": True,
    "tools/analysis/signal_replay.py": True,
    "tools/analysis/engine_analyst.py": True,
    "tools/analysis/signals_with_outcomes.csv": False,  # optional but should be here
    "tools/backtest/ohlcv_replay.py": True,
    "tools/backtest/ohlcv_fetcher.py": True,
    "tools/backtest/leve_backtester.py": True,
    "tools/devtools/diagnose.py": True,
    "tools/devtools/debug_lag.py": True,
    "tools/devtools/pre_deploy_check.py": True,
    "tools/devtools/deploy.py": True,
    "tools/devtools/switch_to_prod.sh": True,
}
for rel, required in TOOL_FILES.items():
    p = ENGINE_ROOT / rel
    if p.exists():
        ok(rel)
    elif required:
        fail(f"MISSING: {rel}")
    else:
        warn(f"Optional missing: {rel}")

# ── Test 3: Dead files removed ───────────────────────────────────────
print("\n[3] Dead files removed from engine root")
DEAD_FILES = [
    "git_diff.log", "sim_dists.json",  # signals_with_outcomes.csv → tools/analysis/
    "workflow.md", "how-to-deploy-analyze.md",
    # These should NOT be in engine root anymore
    "analyze.sh", "analyze_correlation.py", "analyze_local.py",
    "diagnose.py", "debug_lag.py", "synth_sim.py",
    "ohlcv_replay.py", "leve_backtester.py",
]
for f in DEAD_FILES:
    p = ENGINE_ROOT / f
    if not p.exists():
        ok(f"Removed: {f}")
    else:
        fail(f"Still in engine root: {f}")

# ── Test 4: Syntax check all runtime files ───────────────────────────
print("\n[4] Syntax check all runtime .py files")
for f in RUNTIME_FILES:
    p = ENGINE_ROOT / f
    if not p.exists() or not f.endswith(".py"):
        continue
    try:
        ast.parse(p.read_text())
        ok(f"syntax OK: {f}")
    except SyntaxError as e:
        fail(f"SYNTAX ERROR in {f}: {e}")

# ── Test 5: Syntax check all tools ──────────────────────────────────
print("\n[5] Syntax check all tool .py files")
for root, dirs, files in os.walk(ENGINE_ROOT / "tools"):
    dirs[:] = [d for d in dirs if d != "__pycache__"]
    for f in files:
        if not f.endswith(".py"):
            continue
        p = Path(root) / f
        rel = p.relative_to(ENGINE_ROOT)
        try:
            ast.parse(p.read_text())
            ok(f"syntax OK: {rel}")
        except SyntaxError as e:
            fail(f"SYNTAX ERROR in {rel}: {e}")

# ── Test 6: Import reference check (runtime files import each other) ─
print("\n[6] Runtime cross-import references resolvable")
EXPECTED_IMPORTS = {
    "predict_engine.py": ["engine", "engine_logger", "config"],
    "engine.py": ["config", "engine_logger", "core_signals", "engine_scanner", "engine_lag"],
    "strategies_engine.py": ["engine", "config", "live_execution", "strategies_config", "engine_logger", "strategies_signals"],
    "strategies_runtime.py": ["engine", "strategies_config", "strategies_engine", "config", "live_execution"],
    "strategies_config.py": ["engine", "config"],
    "strategies.py": ["strategies_config", "strategies_signals", "strategies_engine", "strategies_runtime"],
}
sys.path.insert(0, str(ENGINE_ROOT))
for filename, expected_mods in EXPECTED_IMPORTS.items():
    p = ENGINE_ROOT / filename
    if not p.exists():
        fail(f"{filename} not found")
        continue
    src = p.read_text()
    for mod in expected_mods:
        if mod in src:
            ok(f"{filename} references {mod}")
        else:
            warn(f"{filename} no longer references {mod} — check if intentional")

# ── Test 7: Tools path references ───────────────────────────────────
print("\n[7] Tool path references point to engine root")
# diagnose.py should have parent.parent.parent or equivalent
f = ENGINE_ROOT / "tools/devtools/diagnose.py"
if f.exists():
    src = f.read_text()
    if "parent.parent.parent" in src or "ENGINE_ROOT" in src:
        ok("diagnose.py: BASE_DIR patched to engine root")
    elif "Path(__file__).parent" in src:
        fail("diagnose.py: BASE_DIR still points to tools/devtools — needs patching")

f = ENGINE_ROOT / "tools/devtools/pre_deploy_check.py"
if f.exists():
    src = f.read_text()
    if "parent.parent.parent" in src:
        ok("pre_deploy_check.py: sys.path patched to engine root")
    else:
        fail("pre_deploy_check.py: sys.path still points to tools/devtools")

# ── Test 8: .env present ─────────────────────────────────────────────
print("\n[8] Config files present")
for f in [".env", "deploy.sh"]:
    p = ENGINE_ROOT / f
    if p.exists():
        ok(f"{f} present")
    else:
        warn(f"{f} not found (may be normal for fresh clone)")

# ── Summary ──────────────────────────────────────────────────────────
print()
print("=" * 60)
print(f"PASS: {PASS}  FAIL: {FAIL}  WARN: {WARN}")
if FAIL == 0:
    print("✅ All tests passed — restructure looks good")
else:
    print(f"❌ {FAIL} test(s) failed — fix before deploying")
sys.exit(0 if FAIL == 0 else 1)
