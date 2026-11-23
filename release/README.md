# Release parity and offline rollback evidence

Run from the candidate checkout with Python 3.10 or newer. Choose a new output
directory under the staging parent.

```sh
python -B release/parity.py ../t_c53a542c-review
```

The command exports `candidate-source.zip`, `rollback-source.zip` and
`parity.json`. Exit 1 means the gate is blocked. The report captures each CLI
result, the exported test suite, rollback observations, unreviewed drift,
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
for action in status tick premarket reconcile flatten tests; do
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

## Rollback gate

The rollback ZIP contains the deployed engine and its two local imports for
the drill. It is a deployed-source reference whose rollback fitness remains
blocked. The drill imports both archived engines and exercises real loaders
using a scratch fixture with a daily halt, risk counters, a closed trade with
verified fills and fees, and a pending exit-ledger replay fence. The candidate
reads the proof and refuses flatten while verification is pending. The deployed
engine reads the state and journal and reports flat in reconcile without
opening the exit ledger. This establishes read compatibility for the fixture
and a missing rollback safety control. In-flight positions, live runtime
data and any independently known-good rollback release remain outside the drill.

The strategy window and equity CHOPPY differences require explicit review,
as does the remainder of the functional engine delta. Source evidence in the
report identifies the lock/CLI, state and exit/fee changes and preserves the
full drift inventory, including excluded modules. Changes are left uncommitted
for host review.
