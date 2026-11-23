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

## Architecture decisions

| Decision | Staging evidence and limits |
| --- | --- |
| Status is read-only | Guard card `t_93df9573` resolves status to read-only. Status reads persisted state and reports a death-floor breach without writing state or placing orders. `test_role_permissions.test_status_at_floor` covers the floor path. The new incident display in `cmd_status` still needs direct QA. |
| Mortality requires an available authorized live tick | State-changing mortality runs through a tick with trading permission and available broker equity evidence. Status and dry tick can report the breach. `test_role_permissions` covers authorized live tick, dry tick and a live request with activation disabled using mocked equity. Actual authorized tick availability was outside this source review. |
| Quote outage persists an incident and uses no candle fallback | `6fabddd` persists and alerts on `QUOTE_UNAVAILABLE`. Broker position and stop checks and the EOD path continue. Price-driven actions wait for a validated quote for the affected instrument. A pending incident also fences new entries after position closure until the quote is validated. The outage cases in `test_preflight.py` cover missing, stale and malformed quotes, restart and the entry fence. |
| Option entry uses verified broker LTP and a bounded chase | `6c22302` computes the chase cap in decimal and rechecks order and exposure after reading the quote. `test_operator_option_pricing.py` covers one amendment at the 1.5% cap on the original order, cancellation above the cap, and reconciliation when a partial fill appears during the quote read. |
| Restore time-of-day and equity CHOPPY protections | `85296d1` ports the operator-approved windows of 09:30–11:30 and 13:30–14:15 IST, inclusive at minute precision. Scan, submission and chase share the time gate. Equity scan checks CHOPPY before either direction is selected, and submission preflight rechecks the selected regime. `test_entry_release_parity.py` covers window, scan and submission behavior. |

## Source provenance

`release/source-provenance.json` pins the deployed application bytes and
candidate application source at `6c223027251f5d1296c9e43c515384d3c4254607`.
It records each changed qualified scope, staging cards and commits,
rationale, and QA references with test file hashes. `parity.json` includes
the raw and CRLF-normalized source digests, normalized line diff and full
qualified scope inventory. The inventory includes added, removed and
unchanged scopes and a `<module>` row for source outside functions. Nested
functions also appear in their enclosing function's source span.
`deployed_matches_staging_baseline` records equality with the source-only
staging base where applicable.

The source diagnostic checks exact module and scope hashes and checks each
referenced test file and method against the candidate export payload.
`subdelta_review` records focused decisions for the time-of-day and CHOPPY
port, the quote incident, the option chase fix and token publication after
persistence. Unreviewed scopes still block the gate. The [source review
diagnostic](source-review-diagnostic.md) lists their exact names.

The standalone `cloud_probe.py` constructor change maps to `e67e554` and
has checkout-only CLI evidence in `test_cloud_probe_cli.py`. The application
probe is outside both archives, while the candidate export includes its
test file. Executing those cases requires a source checkout containing
the probe. The exported suite and rollback exercise were outside this
source-only diagnostic. The focused checkout run is recorded in
[operator regressions](operator-regressions.md).

The mixed guard declarations, token cache and refresh paths, and engine
integration scopes retain open decisions where the tests cover only part
of the behavior. The deployed `state_transaction` helper is absent from
the source-only staging base and has no direct removal approval.
`risk_manager.py` remains a retired simulator outside both archives.
Independent QA reproduced its undefined deployed fee constant.

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
