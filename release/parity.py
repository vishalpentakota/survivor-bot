"""Export reproducible source and run the offline release gate."""

import argparse
from hashlib import sha256
import json
from pathlib import Path
import shutil
import subprocess
from tempfile import TemporaryDirectory
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo


DEPLOYED = Path("/opt/data/Projects/Survivor-bot/survivor-cloud-migration")
CANDIDATE = Path(__file__).resolve().parents[1]
BASELINE_MODULES = (
    "backtester", "cloud_guard", "cloud_probe", "config", "data_fetcher",
    "dhan_broker", "live_trader", "risk_manager", "run_backtest", "signals",
    "survivor_engine", "telegram_reporter", "trade_logger", "warrior_config",
    "warrior_scanner",
)
ADDED_MODULES = ("entry_protection", "eod_ownership", "market_preflight", "verified_exit")
EXCLUSIONS = [
    "Application source outside each archive allowlist",
    "Environment and credential files, token caches and local configuration",
    "Trading state, journals, exit ledgers, EOD stores, locks, logs and reports",
    "VCS metadata, reference trees, caches and virtual environments",
]
ENGINE = "app/survivor_engine.py"


def source_bundle(root, modules):
    app = root / "app"
    if app.is_symlink():
        raise ValueError(f"Symlinked source directory: {app}")
    sources = {}
    for path in sorted(app.glob("*.py")):
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"Expected a regular application source file: {path}")
        raw = path.read_bytes()
        compile(raw, str(path), "exec")
        sources[path.relative_to(root).as_posix()] = raw
    missing = {f"app/{module}.py" for module in modules} - sources.keys()
    if missing:
        raise ValueError(f"Missing expected source files: {sorted(missing)}")
    return sources


def hashes(sources):
    return {name: sha256(raw).hexdigest() for name, raw in sources.items()}


def source_hash(file_hashes):
    return sha256(json.dumps(file_hashes, sort_keys=True).encode("utf-8")).hexdigest()


def json_bytes(value):
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")


def write_bundle(output, kind, sources):
    file_hashes = hashes(sources)
    manifest = {
        "artifact": kind,
        "payload_sha256": source_hash(file_hashes),
        "source_sha256": source_hash({name: digest for name, digest in file_hashes.items()
                                         if name.startswith("app/")}),
        "files_sha256": file_hashes,
        "exclusions": EXCLUSIONS,
    }
    if kind == "rollback":
        manifest["provenance"] = "Documented pre-cutover deployed source; trading fitness unverified"
    files = {**sources, "manifest.json": json_bytes(manifest)}
    with ZipFile(output, "x", compression=ZIP_DEFLATED) as package:
        for name, raw in sorted(files.items()):
            entry = ZipInfo(name)
            entry.create_system = 3
            entry.external_attr = 0o100644 << 16
            package.writestr(entry, raw, compress_type=ZIP_DEFLATED)
    with ZipFile(output) as package:
        if sorted(package.namelist()) != sorted(files):
            raise ValueError("Archive inventory mismatch")
        if any(package.read(name) != raw for name, raw in files.items()):
            raise ValueError("Archive payload mismatch")
    return manifest


CANDIDATE_MODULES = ("cloud_guard", "dhan_broker", "survivor_engine", *ADDED_MODULES)
ROLLBACK_MODULES = ("cloud_guard", "dhan_broker", "survivor_engine")
HARNESS = ("offline.py", "requirements-offline.txt", "README.md")
LOCAL_PYTHON = DEPLOYED / ".venv/bin/python"
REVIEWED_ADDITIONS = {
    "entry_protection": ("Entry fill and protective stop coordination", "b3c324a5a40a22273a1d9677c4fce01f435e9ea11fc7e01099bfecf9aa357e89"),
    "eod_ownership": ("Scheduled flatten ownership and incident persistence", "da8f6279ebddd900947070cb6ca7647619b21949debe5e02642e786e028648ed"),
    "market_preflight": ("Calendar, bars, quotes and expiry validation", "bae5a9ef58dd84e841599c354898b111bd40f8f4799a6dd83fb01cba77c5b08f"),
    "verified_exit": ("Execution proof and persistent exit coordination", "ff91989dfd58fd293a90a38d15d05f682c595ebbc512d52df484f4f3446040ce"),
}


def normalized(raw):
    return raw.replace(b"\r\n", b"\n")


def evidence(raw, *needles, within=None):
    lines = raw.decode().splitlines()
    first = 0
    if within:
        first = next(index for index, line in enumerate(lines) if line.startswith(within))
    last = next((index for index in range(first + 1, len(lines))
                 if within and lines[index].startswith("def ")), len(lines))
    matches = [{"line": index + 1, "text": line.strip()}
               for index, line in enumerate(lines[first:last], first)
               if any(needle in line for needle in needles)]
    if not matches:
        raise ValueError(f"Evidence anchors disappeared: {needles}")
    return matches


def compare(candidate, baseline):
    drift = {}
    for name in sorted(candidate.keys() | baseline.keys()):
        before, after = baseline.get(name), candidate.get(name)
        module = Path(name).stem
        record = {"rollback_sha256": sha256(before).hexdigest() if before else None,
                  "candidate_sha256": sha256(after).hexdigest() if after else None,
                  "candidate_exported": module in CANDIDATE_MODULES,
                  "rollback_exported": module in ROLLBACK_MODULES}
        if before == after:
            record.update(change="identical", classification="identical")
        elif before and after and normalized(before) == normalized(after):
            record.update(change="line_endings_only", classification="intentional",
                          reason="Source matches after CRLF normalization; export retains exact bytes")
        elif before is None and module in REVIEWED_ADDITIONS and (
                sha256(normalized(after)).hexdigest() == REVIEWED_ADDITIONS[module][1]):
            record.update(change="added", classification="intentional",
                          reason=REVIEWED_ADDITIONS[module][0])
        else:
            change = "added" if before is None else "removed" if after is None else "modified"
            record.update(change=change, classification="unreviewed",
                          reason="No approval recorded for this exact source delta")
            if module in {"risk_manager", "cloud_probe"}:
                record["reason"] += "; module excluded from both supported bundles"
        drift[name] = record
    old, new = baseline[ENGINE], candidate[ENGINE]
    scoped = {
        "strategy_windows": {
            "classification": "unreviewed",
            "baseline": evidence(old, "_tod_ok =", 'if not ("09:20"'),
            "candidate": evidence(new, 'return "09:20"', 'if not ("09:20"'),
            "finding": "Candidate permits 09:20–14:15 continuously; deployed permits 09:30–11:30 and 13:30–14:15 IST.",
        },
        "equity_choppy": {
            "classification": "unreviewed",
            "baseline": evidence(old, 'if regime == "CHOPPY":', within="def scan_equity_candidates"),
            "candidate": evidence(new, 'def scan_equity_candidates', 'if atr <= 0', 'if st_dir == 1',
                                  within='def scan_equity_candidates'),
            "finding": "Deployed rejects CHOPPY in the equity scanner; candidate retains the index filter but omits the equity rejection.",
        },
        "locks_cli": {
            "classification": "intentional",
            "baseline": evidence(old, "os.O_CREAT | os.O_EXCL", 'args.action == "flatten"', "def acquire_engine_lock"),
            "candidate": evidence(new, "def acquire_engine_lock", "flock(", "if not policy.flatten",
                                  "def _scheduled_flatten", "def main()", "RolePolicy.resolve"),
            "finding": "Persistent flock ownership and explicit CLI capability dispatch replace the PID-file ownership and legacy dispatch.",
        },
        "state": {
            "classification": "intentional",
            "baseline": evidence(old, "def load_state", "default_state.update(saved)", "Failed to read state file"),
            "candidate": evidence(new, "def _read_state_storage", "raise CorruptState", "def load_state",
                                  "return StateSnapshot(default_state, raw)", "def _state_transaction"),
            "finding": "Candidate distinguishes corrupt state from missing state, retains raw snapshots and serializes writes.",
        },
        "exit_and_fees": {
            "classification": "intentional",
            "baseline": evidence(old, "def _finalize_closed_trade", "actual_gross - ESTIMATED_ROUNDTRIP_CHARGES",
                                 "Net Account Equity Change", "Brokerage & Statutory Charges"),
            "candidate": evidence(new, "def _verified_trade_fills", "def _actual_charges", "pnl_provenance=",
                                  "Actual broker charges:", "def _finalize_closed_trade"),
            "finding": "Candidate separates verified per-order charges from the estimate and requires attributed exit evidence.",
        },
    }
    # Bind approvals for the described mechanisms to the exact inspected pair.
    # The whole engine delta and the two strategy choices remain unreviewed.
    if (sha256(old).hexdigest(), sha256(normalized(new)).hexdigest()) != (
            "60cb6bb1203de2fc7f141513307e4a62cceb1a4f2d8a0c807d9e8b6be47c9b80",
            "959049dd3e8a22f94db3e7a8e85981725fc531230c2d28df4a7ffc7baad71bce",
    ):
        for item in scoped.values():
            item["classification"] = "unreviewed"
    return drift, scoped


def run(root, python, *args, bootstrap=False):
    flags = ["-I", "-B"] if bootstrap else ["-I", "-B", "-S"]
    process = subprocess.run([str(python), *flags, str(root / "offline.py"), *map(str, args)],
                             cwd=root, env={"HOME": str(root), "TMPDIR": str(root),
                                            "PATH": "/usr/bin:/bin", "TZ": "UTC"},
                             capture_output=True, text=True, timeout=360)
    return {"returncode": process.returncode, "stdout": process.stdout, "stderr": process.stderr}


def export_files(source, modules, *, tests=False):
    payload = {f"app/{name}.py": source[f"app/{name}.py"] for name in modules}
    for name in HARNESS:
        payload[name] = (CANDIDATE / "release" / name).read_bytes()
    if tests:
        for path in sorted((CANDIDATE / "tests").glob("test_*.py")):
            if path.is_symlink():
                raise ValueError(f"Unexpected test symlink: {path}")
            payload[path.relative_to(CANDIDATE).as_posix()] = path.read_bytes()
    return payload


def exercise(candidate_archive, rollback_archive, report):
    with TemporaryDirectory(prefix="survivor-release-") as temporary:
        staged = Path(temporary)
        current, deployed = staged / "candidate", staged / "rollback"
        for archive, root in ((candidate_archive, current), (rollback_archive, deployed)):
            with ZipFile(archive) as package:
                package.extractall(root)
        for root in (current, deployed):
            fresh = root / ".venv"
            result = run(root, LOCAL_PYTHON, "build", fresh, "--local-python", LOCAL_PYTHON, bootstrap=True)
            if result["returncode"]:
                raise RuntimeError(f"Dependency build failed for {root.name}: {result['stderr']}")
            report.setdefault("dependency_provenance", {})[root.name] = json.loads(result["stdout"])

        fresh = current / ".venv"
        python = fresh / "bin/python"
        smoke = report["cli"] = {}
        for action in ("status", "tick", "premarket", "reconcile", "flatten"):
            smoke[action] = run(current, python, action, fresh)
        report["tests"] = run(current, python, "tests", fresh)
        for name in ("survivor_state.json", "survivor_trades.json", "survivor_exits.json"):
            (current / "app" / name).unlink(missing_ok=True)
        drill = report["rollback_drill"] = {}
        drill["candidate_fixture"] = run(current, python, "fixture", fresh)
        if drill["candidate_fixture"]["returncode"]:
            raise RuntimeError("Candidate rollback fixture failed")
        for name in ("survivor_state.json", "survivor_trades.json", "survivor_exits.json"):
            shutil.copyfile(current / "app" / name, deployed / "app" / name)
        deployed_fresh = deployed / ".venv"
        drill["deployed"] = run(deployed, deployed_fresh / "bin/python", "rollback", deployed_fresh)
        drill["scope"] = "Synthetic flat, halted state with verified historical fills and replay fence; in-flight trades, operational state, and trading fitness remain unverified."


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact_dir", type=Path, help="New directory under staging scratch")
    args = parser.parse_args()
    output = args.artifact_dir.resolve()
    if output == CANDIDATE.parent or not output.is_relative_to(CANDIDATE.parent):
        parser.error("Artifact directory must be under staging scratch")
    candidate = source_bundle(CANDIDATE, BASELINE_MODULES + ADDED_MODULES)
    rollback = source_bundle(DEPLOYED, BASELINE_MODULES)
    output.mkdir()
    candidate_archive = output / "candidate-source.zip"
    rollback_archive = output / "rollback-source.zip"
    report = {
        "candidate": write_bundle(candidate_archive, "candidate", export_files(candidate, CANDIDATE_MODULES, tests=True)),
        "rollback": write_bundle(rollback_archive, "rollback", export_files(rollback, ROLLBACK_MODULES)),
        "rollback_source": str(DEPLOYED),
    }
    report["archive_sha256"] = {path.name: sha256(path.read_bytes()).hexdigest()
                                for path in (candidate_archive, rollback_archive)}
    blockers = report["blockers"] = []
    try:
        report["drift"], report["scoped_comparison"] = compare(candidate, rollback)
    except (StopIteration, ValueError) as exc:
        blockers.append(f"Source comparison requires review: {exc or 'evidence anchor missing'}")
    for group in (report.get("drift", {}), report.get("scoped_comparison", {})):
        blockers.extend(f"Unreviewed drift: {name}" for name, row in group.items()
                        if row["classification"] == "unreviewed")
    try:
        exercise(candidate_archive, rollback_archive, report)
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as exc:
        blockers.append(str(exc))
    for action, result in report.get("cli", {}).items():
        if result["returncode"]:
            blockers.append(f"Exported CLI failed: {action}")
    if report.get("tests", {}).get("returncode"):
        blockers.append("Exported tests failed")
    deployed = report.get("rollback_drill", {}).get("deployed", {})
    if deployed.get("returncode") or not deployed:
        blockers.append("Rollback compatibility was not demonstrated")
    elif "BLOCKED:" in deployed["stdout"]:
        blockers.append("Deployed reconcile ignores candidate exit-ledger replay authority")
    else:
        blockers.append("Rollback ledger and journal compatibility requires review")
    report["gate"] = "blocked" if blockers else "passed"
    (output / "parity.json").write_bytes(json_bytes(report))
    print(f"{report['gate']}: {output}")
    return bool(blockers)


if __name__ == "__main__":
    raise SystemExit(main())
