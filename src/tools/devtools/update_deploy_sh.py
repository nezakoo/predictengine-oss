#!/usr/bin/env python3
"""
Updates deploy.sh to:
1. Only deploy runtime core files (not tools/)
2. Add --stage flag for deploying to stage server
3. Update file lists to match new structure
"""
from pathlib import Path
import sys

deploy = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("deploy.sh")
src = deploy.read_text()

# ── Replace ENGINE_FILES list ────────────────────────────────────────
OLD_ENGINE = '''ENGINE_FILES=(
    config.py core_signals.py engine.py engine_scanner.py engine_lag.py
    predict_engine.py dashboard_multi.py strategies.py strategies_signals.py
    strategies_engine.py strategies_runtime.py test_yk.py debug_lag.py
    engine_logger.py analyze_signals.py analyze.py diagnose.py deploy.py
    live_execution.py analyze.sh .env close_all_positions.py analyze_correlation.py
)'''

NEW_ENGINE = '''ENGINE_FILES=(
    config.py core_signals.py engine.py engine_scanner.py engine_lag.py
    predict_engine.py dashboard_multi.py strategies.py strategies_signals.py
    strategies_engine.py strategies_runtime.py engine_logger.py
    live_execution.py close_all_positions.py tg_monitor.py .env
)'''

if OLD_ENGINE in src:
    src = src.replace(OLD_ENGINE, NEW_ENGINE)
    print("✅ ENGINE_FILES list updated")
else:
    print("⚠️  ENGINE_FILES list not found verbatim — check deploy.sh manually")
    print("   Replace the ENGINE_FILES block to include only:")
    print("   config.py core_signals.py engine.py engine_scanner.py engine_lag.py")
    print("   predict_engine.py dashboard_multi.py strategies.py strategies_signals.py")
    print("   strategies_engine.py strategies_runtime.py engine_logger.py")
    print("   live_execution.py close_all_positions.py tg_monitor.py .env")

# ── Add stage server support ──────────────────────────────────────────
STAGE_ARG = '''        --stage)       STAGE=true; shift ;;'''
STAGE_SERVER_BLOCK = '''
# ── Stage server override ───────────────────────────────────────────
if [[ "${STAGE:-false}" == "true" ]]; then
    SERVER="${DEPLOY_HOST:-user@host.example.com}"
    echo "⚠️  Deploying to STAGE (0.0.0.0) — sim only"
fi
'''

if "--stage" not in src:
    src = src.replace(
        '        --watch)       WATCH=true; shift ;;',
        '        --stage)       STAGE=true; shift ;;\n        --watch)       WATCH=true; shift ;;'
    )
    # Insert after SERVER definition
    src = src.replace(
        'REMOTE_DIR="~/engine"',
        'REMOTE_DIR="~/engine"\nSTAGE=false'
    )
    src = src.replace(
        '# SSH helpers',
        '# SSH helpers' + STAGE_SERVER_BLOCK
    )
    print("✅ --stage flag added")
else:
    print("✅ --stage flag already present")

deploy.write_text(src)
print(f"✅ Written to {deploy}")
