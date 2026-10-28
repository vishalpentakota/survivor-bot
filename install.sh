#!/usr/bin/env bash
set -eu
cd "$(dirname "$0")"
case "$(pwd)" in /opt/data/*) ;; *) printf '%s\n' 'Extract beneath /opt/data before installation.'; exit 1;; esac
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python tests/test_migration.py
printf '%s\n' 'Installed in READ-ONLY mode. No credentials, live jobs, or whitelist changes created.'
