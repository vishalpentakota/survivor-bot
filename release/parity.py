"""Export reproducible source and run the offline release gate."""

import argparse
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

if __package__:
    from .source_review import normalized, review_sources
else:
    from source_review import normalized, review_sources


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
    "Retired legacy risk_manager.py simulator",
    "Environment and credential files, token caches and local configuration",
    "Trading state, journals, exit ledgers, EOD stores, locks, logs and reports",
    "VCS metadata, reference trees, caches and virtual environments",
]


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


CANDIDATE_MODULES = ("cloud_guard", "cloud_probe", "dhan_broker", "survivor_engine", *ADDED_MODULES)
ROLLBACK_MODULES = ("cloud_guard", "dhan_broker", "survivor_engine")
HARNESS = ("offline.py", "rollback_preflight.py", "requirements-offline.txt", "README.md")
LOCAL_PYTHON = DEPLOYED / ".venv/bin/python"
REVIEWED_ADDITIONS = {
    "entry_protection": ("Entry fill and protective stop coordination", "b3c324a5a40a22273a1d9677c4fce01f435e9ea11fc7e01099bfecf9aa357e89"),
    "eod_ownership": ("Scheduled flatten ownership and incident persistence", "da8f6279ebddd900947070cb6ca7647619b21949debe5e02642e786e028648ed"),
    "market_preflight": ("Calendar, bars, quotes and expiry validation", "bae5a9ef58dd84e841599c354898b111bd40f8f4799a6dd83fb01cba77c5b08f"),
    "verified_exit": ("Execution proof and persistent exit coordination", "ff91989dfd58fd293a90a38d15d05f682c595ebbc512d52df484f4f3446040ce"),
}


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
            if module == "risk_manager":
                record["reason"] += "; module excluded from both supported bundles"
        drift[name] = record
    return drift


def apply_source_review(drift, review):
    for name, module in review["modules"].items():
        inventory = module["inventory"]
        pending = [scope for scope, row in inventory.items()
                   if row["classification"] == "unreviewed"]
        drift[name]["unreviewed_scopes"] = pending
        if not module["matches_ledger"] and drift[name]["change"] == "modified":
            drift[name].update(classification="unreviewed",
                               reason="Module source pair differs from the pinned ledger")
        elif pending:
            drift[name].update(classification="unreviewed",
                               reason="See source_review for unresolved function and module edits")
        elif module["lifecycle"] == "retired_excluded":
            if drift[name]["candidate_exported"] or drift[name]["rollback_exported"]:
                raise ValueError(f"Retired module included in a supported archive: {name}")
            drift[name].update(classification="retired_excluded",
                               reason="Retired legacy simulator outside both archive allowlists")
        elif drift[name]["change"] == "modified":
            drift[name].update(classification="intentional",
                               reason="Every changed scope has pinned provenance and QA")


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


def rollback_matrix(current, deployed, fresh):
    names = ("survivor_state.json", "survivor_trades.json", "survivor_exits.json")
    state_name, journal_name, ledger_name = names
    originals = {name: json.loads((current / "app" / name).read_bytes()) for name in names}
    app = deployed / "app"
    position_name = "rollback_positions.json"
    observed = datetime(2026, 11, 23, 4, 30, tzinfo=timezone.utc)
    position = dict(source="synthetic-broker-fixture", status="verified", scope="all-account-exposure",
                    observed_at=observed.isoformat(), positions=[], pending_orders=[])
    cases = {
        "open_position": (position_name, {"positions": [{"net_quantity": -1}]}),
        "pending_order": (position_name, {"pending_orders": ["order"]}),
        "unknown_position": (position_name, {"status": "unknown"}),
        "position_error": (position_name, {"status": "error"}),
        "wrong_provenance": (position_name, {"source": "unverified"}),
        "stale_position": (position_name, {"observed_at": (observed - timedelta(seconds=61)).isoformat()}),
        "future_position": (position_name, {"observed_at": (observed + timedelta(seconds=1)).isoformat()}),
        "active_trade": (state_name, {"active_trade": {"order_id": "open"}}),
        "pending_recovery": (state_name, {"emergency_recovery": {"status": "pending"}}),
        "unknown_cycle": (ledger_name, {"cycle": "unknown"}),
        "pending_cycle": (ledger_name, {"cycle": {"id": "pending"}}),
        "pending_replay": (ledger_name, {"replay_check": "verification_required"}),
        "unknown_replay": (ledger_name, {"replay_check": None}),
        "ledger_mismatch": (ledger_name, {"flat_book": {}}),
        "state_journal_mismatch": (state_name, {"total_trades": 0}),
        "unreconciled_journal": (journal_name, [dict(originals[journal_name][0], reconciled_with_broker=False)]),
        "journal_proof_mismatch": (journal_name, [dict(originals[journal_name][0], net_pnl=0)]),
        "snapshot_hash_mismatch": (position_name, {"evidence_sha256": {}}),
        "clean_flat": (None, None),
    }
    results = {}
    for label, (name, update) in cases.items():
        evidence = {**originals, ledger_name: dict(originals[ledger_name])}
        evidence[ledger_name].pop("replay_check")
        if name in evidence:
            evidence[name] = {**evidence[name], **update} if isinstance(update, dict) else update
        payload = {key: json_bytes(value) for key, value in evidence.items()}
        snapshot = dict(position, evidence_sha256=hashes(payload))
        if name == position_name:
            snapshot.update(update)
        payload[position_name] = json_bytes(snapshot)
        for target, raw in payload.items():
            (app / target).write_bytes(raw)
        results[label] = run(deployed, fresh / "bin/python", "rollback", fresh)
    for name in (*names, position_name):
        path = app / name
        original = path.read_bytes()
        for label in ("missing", "malformed", "unreadable"):
            path.unlink()
            if label == "malformed":
                path.write_bytes(b"{invalid")
            if label == "unreadable":
                path.mkdir()  # Non-regular evidence cannot be read as a snapshot.
            results[f"{label}:{name}"] = run(deployed, fresh / "bin/python", "rollback", fresh)
            if label == "unreadable":
                path.rmdir()
            path.write_bytes(original)
    recovery = app / (state_name + ".recovery")
    recovery.write_bytes(json_bytes({"reason": "STATE_CORRUPTION", "status": "pending"}))
    results["out_of_band_recovery"] = run(deployed, fresh / "bin/python", "rollback", fresh)
    recovery.unlink()
    return results


def exercise(candidate_archive, rollback_archive, report):
    with TemporaryDirectory(prefix="survivor-release-", dir=candidate_archive.parent) as temporary:
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
        report["safety_tests"] = run(current, python, "safety-tests", fresh)
        for name in ("survivor_state.json", "survivor_trades.json", "survivor_exits.json"):
            (current / "app" / name).unlink(missing_ok=True)
        drill = report["rollback_drill"] = {}
        drill["candidate_fixture"] = run(current, python, "fixture", fresh)
        if drill["candidate_fixture"]["returncode"]:
            raise RuntimeError("Candidate rollback fixture failed")
        drill["cases"] = rollback_matrix(current, deployed, deployed / ".venv")
        drill["scope"] = "Offline gated reconcile with synthetic exposure only. Operational rollback requires independently verified current broker evidence."


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
    candidate_payload = export_files(candidate, CANDIDATE_MODULES, tests=True)
    report = {
        "candidate": write_bundle(candidate_archive, "candidate", candidate_payload),
        "rollback": write_bundle(rollback_archive, "rollback", export_files(rollback, ROLLBACK_MODULES)),
        "rollback_source": str(DEPLOYED),
    }
    report["archive_sha256"] = {path.name: sha256(path.read_bytes()).hexdigest()
                                for path in (candidate_archive, rollback_archive)}
    blockers = report["blockers"] = []
    try:
        report["drift"] = compare(candidate, rollback)
        report["source_review"] = review_sources(candidate, rollback, candidate_payload)
        apply_source_review(report["drift"], report["source_review"])
    except (KeyError, OSError, SyntaxError, ValueError) as exc:
        blockers.append(f"Source comparison requires review: {exc}")
    blockers.extend(
        f"Unreviewed drift: {name} ({len(row['unreviewed_scopes'])} scopes)"
        if "unreviewed_scopes" in row else f"Unreviewed drift: {name}"
        for name, row in report.get("drift", {}).items() if row["classification"] == "unreviewed"
    )
    try:
        exercise(candidate_archive, rollback_archive, report)
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as exc:
        blockers.append(str(exc))
    for action, result in report.get("cli", {}).items():
        if result["returncode"]:
            blockers.append(f"Exported CLI failed: {action}")
    for suite in ("tests", "safety_tests"):
        if report.get(suite, {}).get("returncode") != 0:
            blockers.append(f"Exported suite failed or was unavailable: {suite}")
    cases = report.get("rollback_drill", {}).get("cases", {})
    clean = cases.get("clean_flat", {})
    if clean.get("returncode") != 0 or "Scratch rollback reconcile completed" not in clean.get("stdout", ""):
        blockers.append("Gated clean-flat rollback reconcile was not demonstrated")
    for name, result in cases.items():
        if name != "clean_flat" and (result["returncode"] != 2
                                   or "ROLLBACK REFUSED:" not in result["stderr"]
                                   or "preflight accepted" in result["stdout"]):
            blockers.append(f"Rollback preflight did not refuse as expected: {name}")
    report["gate"] = "blocked" if blockers else "passed"
    (output / "parity.json").write_bytes(json_bytes(report))
    print(f"{report['gate']}: {output}")
    return bool(blockers)


if __name__ == "__main__":
    raise SystemExit(main())
