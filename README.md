# Survivor migration to Virtuous Plurality — READ-ONLY STAGING

## Upload instructions for cloud agent
Extract this ZIP into /opt/data/Projects/Survivor-bot/survivor-cloud-migration (do not overwrite an existing install).
Read KNOWN_ISSUES.md first. Run `bash install.sh`, then `.venv/bin/python app/cloud_probe.py`.
Report real outputs. Do not register trading cron jobs, run original engine CLI, change IP whitelist, or place orders.
The guarded engine blocks its entire CLI; the broker adapter blocks write methods. These are staging safeguards, not an OS sandbox. Supporting modules and original reference are NOT safe live entrypoints.

## Credentials
Secrets intentionally excluded: .env, access token caches, PIN, TOTP secret, Telegram tokens, Hermes OAuth and browser sessions.
Configure only Dhan credentials privately in app/.env, permission 0600, or via the destination secret mechanism. Never print them in chat/logs. Do not copy the source Hermes .env wholesale.
Then `.venv/bin/python app/cloud_probe.py --broker` checks funds/positions/orders/index historical feeds and Dhan's whitelist via GET only. Authentication can renew a token; it cannot place orders through the probe. Historical feed success does not prove current market freshness.

## Contents
app/: source modules, CSV/data artifacts, guarded engine, probe, credential template.
history/: original redacted state, trade journals and logs (snapshots, not audited accounting).
reference/: original engine source and historical skill notes; not execution instructions.
requirements.txt: versions installed on source machine; Linux install still requires verification.
tests/: offline staging guard and syntax tests.
MANIFEST.json: SHA256 per file, excluding manifest itself.

## Cutover (separate approval required)
Audit/fix known issues; reconcile broker state and journal; verify destination IP eligibility and actual ordersAllowed; confirm timezone Asia/Kolkata, clock, calendar and reliable lifecycle recovery. Repeated pre-open checks do not guarantee IP stability mid-session.
Local heartbeat b0daf8d7b0d3 was paused previously. Verify ALL local jobs and processes before enabling any cloud trading owner. Local premarket 8d4e3676bf09 and EOD 464016ace8d7 may still run: disable/coordinate them at actual cutover to avoid token renewal/state conflicts. Do not blindly copy cron IDs to cloud.
Intended schedules (Asia/Kolkata): preflight 08:55 weekdays; entry scans every 5 minutes within permitted hours; no new entries after 14:15; flatten 15:10; reconcile afterward. Verify Chronos timezone and no-agent script support on destination. No schedules created by this bundle.
Persist code, virtualenv or rebuild recipe, credentials, state and logs beneath /opt/data. Cold restart kills processes; require startup broker reconciliation before any entry. Configure Telegram securely; avoid two gateway instances polling the same bot.
Do not auto-enable live trading by removing guards. Obtain explicit user approval only after real tests pass. Keep a rollback copy; never run two trading owners.
