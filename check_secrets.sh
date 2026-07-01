#!/usr/bin/env bash
set -uo pipefail
fail=0
echo "scanning for secrets / IPs / hosts…"
grep -rEn '(API_KEY|API_SECRET|TG_BOT_TOKEN|TG_CHAT_ID)[[:space:]]*=[[:space:]]*["'"'"'][A-Za-z0-9]{8,}' --include=*.py --include=*.sh . && { echo '!! hardcoded secret'; fail=1; }
grep -rEn '[0-9]{1,3}(\.[0-9]{1,3}){3}' --include=*.py --include=*.sh . | grep -vE '0\.0\.0\.0|127\.0\.0\.1|REDACTED|example' && { echo '!! raw IP'; fail=1; }
grep -rEn --exclude=check_secrets.sh 'ubuntu@|bot[0-9]{6,}:' . && { echo '!! host/bot token'; fail=1; }
find . -name '.env' -o -name '*.env' | grep -q . && { echo '!! .env present'; fail=1; }
[ $fail -eq 0 ] && echo '✓ clean' || echo '✗ FIX BEFORE COMMIT'
exit $fail
