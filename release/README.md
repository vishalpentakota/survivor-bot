# Release parity and offline rollback evidence

Run from the candidate checkout with Python 3.10 or newer. Choose a new output
directory under the staging parent.

```sh
python -B release/parity.py ../t_c53a542c-host-review
```

The command exports `candidate-source.zip`, `rollback-source.zip` and
`parity.json`. Exit 1 means the gate is blocked. The report captures each CLI
result, the exported test suite, the independent 40-test safety rerun, rollback observations, unreviewed drift,
and the two archive hashes. Both ZIPs contain exact application source bytes
from their respective trees. Each manifest hashes every payload file and
records separate aggregate digests for the app source and the whole payload.
Aggregate digests hash a key-sorted JSON mapping from relative paths to file
hashes. ZIP order, timestamps and permissions are fixed. Git metadata is
unnecessary. Use the report with the archives to interpret a blocked run.

## Fresh offline copy

Extract the candidate ZIP into a new scratch directory and run the following
from that directory on the release host.

```sh
local_python=/opt/data/Projects/Survivor-bot/survivor-cloud-migration/.venv/bin/python
env -i PATH=/usr/bin:/bin HOME="$PWD" TMPDIR="$PWD" TZ=UTC \
  "$local_python" -I -B offline.py build .venv --local-python "$local_python"
for action in status tick premarket reconcile flatten tests safety-tests; do
  .venv/bin/python -I -B -S offline.py "$action" .venv || exit
done
```

The build verifies the extracted manifest and installed distribution versions,
then copies pinned dependency files whose SHA-256 matches their installed
RECORD metadata into a fresh pip-free venv. The deployed venv and interpreter
are the local dependency source used in the independent QA audit
(`qa-t_97b7242a/SURVIVOR-QA.md`). The report records the venv path, Python
version, installed package versions and digests of the staged dependency
files. The pins cover the stubbed engine and test paths. This procedure
requires the local Python installation and `libseccomp.so.2`.

Each smoke runs the exported engine's CLI parser and dispatch under verified
kernel socket denial with a scrubbed environment, synthetic clock and broker
responses. Dry tick exercises the scan and checks state bytes. Flatten uses
the CLI's flatten role with an already-flat mock broker and verifies its scratch
recovery transition. The stub provides no order methods. The archives exclude
legacy executable and backtest modules, including the legacy fee simulator and
the reporter that imports `live_trader`. Credentials, real market metadata,
production state, logs and installed dependencies are outside the archives.

## Entry protections

The operator approved the deployed IST windows of 09:30–11:30 and
13:30–14:15, inclusive at minute precision, and the deployed equity CHOPPY
filter. The candidate shares the time gate across scan, submission preflight
and entry chase. The equity scanner checks regime before selecting either
direction, and order preflight checks the selected regime again. The operator
patch is recorded with its tests. The remaining live-to-candidate edits in the
tick and equity scanner scopes still require review.

## Source provenance

`release/source-provenance.json` records the reviewed source pairs, staging
cards and full commit IDs, and the pinned QA assertions. `parity.json`
includes raw and CRLF-normalized SHA-256 digests of both inspected sources,
the normalized line diff, and the full qualified function inventory for all
five modules. The inventory includes added, removed and unchanged functions,
methods, nested functions and a `<module>` row for source outside functions.
A nested function also appears within its enclosing function's source span.
The recorded staging history identifies each scope version by hash. The
`deployed_matches_staging_baseline` flag distinguishes actual equality with
the source-only staging base from scopes that already differed there.

A scope is intentional only when the inspected source pair and scope match
the ledger and the named regression assertions match the exported test
source. The offline suite must also pass for release. A related staging
commit or QA case can be recorded on an unreviewed scope as a lead.
`subdelta_review` records the exact operator port for the scanner and tick
separately from the full scope verdict. Outstanding scopes are enumerated
under each module's `unreviewed_scopes` list and block the gate.

- `cloud_guard.py` retains pending adapter surface review for the dedicated
  exit method and module-level capability declarations.
- `cloud_probe.py` is outside both archives. The one-line cache-only broker
  constructor change maps to `e67e554`. The pinned regressions cover the
  client directly. The standalone probe change remains unreviewed.
- `dhan_broker.py` traces to the guard source commits. Exact coverage gaps
  include the cache expiry margin, atomic replacement and exceptional cleanup,
  renewal retry semantics and the loader return value and exclusions.
- `risk_manager.py` is a retired legacy simulator outside both archives.
  Its two fee-subtraction edits are recorded under
  `TradeSimulator.run_backtest`. Independent QA reproduced the undefined
  deployed fee constant. Retirement keeps the simulator outside the gate's
  supported runtime and rollback scopes.
- `survivor_engine.py` maps the remaining drift to the committed guard,
  exit, EOD, entry, recovery, preflight, reconcile and harness evidence, plus
  the operator patch. The release gate stays blocked on unresolved integration
  scopes, including the replaced tick path, closure fallback and the removal
  of the deployed `state_transaction` helper.

## Scratch rollback gate

The rollback archive contains the deployed engine and its two local imports.
The runner validates all four evidence files before importing the old engine.
Its sole rollback operation is an isolated scratch reconcile. The parity
runner exercises that same invocation with a clean historical closure and
with open, unknown, pending, stale, inconsistent and unreadable evidence.
A refusal exits 2 before engine import. The candidate fixture separately
checks that the candidate's flatten path respects the pending replay fence.

Scratch evidence must contain a verified, all-account synthetic position and
pending-order snapshot, captured no more than 60 seconds before the drill's
synthetic clock. It binds the exact state, journal and ledger bytes by
SHA-256. The state must show no active trade or unresolved recovery, including the out-of-band
corrupt-state recovery marker. Every
journal closure must reconcile to balanced, uniquely attributed fills and
actual fees. The ledger must show a flat book consistent with the journal
and be free of pending cycles, alerts, unattributed fills and replay checks.
Missing, malformed or ambiguous evidence fails closed.

Operational rollback requires the operator to quiesce all trading writers
and independently verify current flat broker exposure and absence of working
orders for the entire account immediately before rollback. The operator must
also confirm that the current state, journal and ledger reconcile while those
writers remain stopped. The included runner consumes synthetic evidence
only. The drill establishes scratch rollback behavior. Live flat exposure
and operational rollback fitness remain unverified.

Changes are left uncommitted for independent host review.
