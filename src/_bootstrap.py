"""Import-path shim so the reorganized modules still resolve their original flat imports
(e.g. `import live_execution`). Entry-point scripts do `import _bootstrap` first, or run
with PYTHONPATH covering the src subdirs. See README 'Running the code'."""
import os, sys
_here = os.path.dirname(os.path.abspath(__file__))
for sub in ("engine", "strategies", "execution", "carry", "ops", "tools",
            "tools/analysis", "tools/backtest", "tools/devtools", "tools/research"):
    p = os.path.join(_here, sub)
    if p not in sys.path:
        sys.path.insert(0, p)
