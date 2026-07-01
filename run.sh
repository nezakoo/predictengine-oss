#!/usr/bin/env bash
# Run any module with the flat-import path set up. Usage: ./run.sh src/carry/carry_paper.py --report
here="$(cd "$(dirname "$0")" && pwd)/src"
export PYTHONPATH="$here:$here/engine:$here/strategies:$here/execution:$here/carry:$here/ops:$here/tools:$here/tools/analysis:$here/tools/backtest:$here/tools/devtools:$here/tools/research:$PYTHONPATH"
exec python3 "$@"
