#!/usr/bin/env python3
"""
deploy.py — PredictEngine deploy stamp writer
=============================================
Run this every time you deploy a config change.
It writes a timestamp + optional label to .deploy_stamp,
which analyze.py --since-deploy uses to filter CSVs.

Usage:
    python3 deploy.py                                  # stamp with current time
    python3 deploy.py "added confluence filters"       # stamp with label
    python3 deploy.py --show                           # show current stamp
    python3 deploy.py --list                           # show full stamp history

The stamp file lives at: .deploy_stamp  (same dir as deploy.py)
The history file lives at: .deploy_history  (appended on each stamp)
"""

import sys, argparse
from datetime import datetime
from pathlib import Path

BASE    = Path(__file__).parent
# Stamps live in data_backup/ so they're alongside the CSVs they filter.
# deploy-hot.sh also writes here — both tools must agree on this path.
_BACKUP = BASE / 'data_backup'
_BACKUP.mkdir(exist_ok=True)
STAMP   = _BACKUP / '.deploy_stamp'
HISTORY = _BACKUP / '.deploy_history'


def write_stamp(label: str = None):
    now = datetime.now()
    ts  = now.strftime('%Y-%m-%d %H:%M:%S')
    line = f"{ts} | {label}" if label else ts

    # Write current stamp
    STAMP.write_text(line + '\n')

    # Append to history
    with open(HISTORY, 'a') as f:
        f.write(line + '\n')

    print(f"\n✅  Deploy stamp written: {ts}")
    if label:
        print(f"    Label: {label}")
    print(f"    File:  {STAMP}")
    print(f"\n    Analyze since this deploy:")
    print(f"      python3 analyze.py ./data_backup --since-deploy --claude")
    print(f"    History: {HISTORY}")


def show_stamp():
    if not STAMP.exists():
        print(f"❌  No stamp file found at {STAMP}")
        print(f"    Run: python3 deploy.py [optional label]")
        return
    raw = STAMP.read_text().strip()
    print(f"\n📌  Current deploy stamp:")
    print(f"    {raw}")
    if HISTORY.exists():
        lines = HISTORY.read_text().strip().split('\n')
        print(f"\n    Total deploys logged: {len(lines)}")


def show_history():
    if not HISTORY.exists():
        print(f"❌  No history file found at {HISTORY}")
        return
    lines = HISTORY.read_text().strip().split('\n')
    print(f"\n📋  Deploy history ({len(lines)} entries):")
    for i, line in enumerate(reversed(lines[-20:])):   # last 20
        idx = len(lines) - i
        print(f"    #{idx:3d}  {line}")
    if len(lines) > 20:
        print(f"    ... and {len(lines)-20} older entries")


def main():
    parser = argparse.ArgumentParser(description='PredictEngine deploy stamp writer')
    parser.add_argument('label', nargs='?', default=None,
                        help='Optional description of what changed (e.g. "added Z blacklist")')
    parser.add_argument('--show',    action='store_true', help='Show current stamp and exit')
    parser.add_argument('--list',    action='store_true', help='Show full stamp history and exit')
    args = parser.parse_args()

    if args.show:
        show_stamp(); return
    if args.list:
        show_history(); return

    write_stamp(args.label)


if __name__ == '__main__':
    main()