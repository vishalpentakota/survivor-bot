"""Build and exercise a scratch release using verified local dependencies and synthetic data."""

import argparse
from base64 import urlsafe_b64encode
from contextlib import redirect_stdout
import ctypes
from datetime import datetime, timedelta
import errno
from hashlib import sha256
from importlib.metadata import distributions
import io
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parent
APP = ROOT / "app"
SUCCESS = ("status", "tick", "premarket", "flatten", "reconcile")
DAY = "2026-11-23"


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def isolated_env(root):
    return {"HOME": str(root), "TMPDIR": str(root), "PATH": "/usr/bin:/bin", "TZ": "UTC"}


def verify_payload():
    manifest = json.loads((ROOT / "manifest.json").read_text())
    file_hashes = {}
    for name, expected in manifest["files_sha256"].items():
        path = ROOT / name
        require(path.resolve() == path and path.is_relative_to(ROOT) and ".." not in Path(name).parts,
                f"Unsafe payload path: {name}")
        digest = sha256(path.read_bytes()).hexdigest()
        require(digest == expected, f"Payload hash mismatch: {name}")
        file_hashes[name] = digest
    require(sha256(json.dumps(file_hashes, sort_keys=True).encode()).hexdigest() ==
            manifest["payload_sha256"], "Aggregate payload hash mismatch")
    actual = {path.relative_to(ROOT).as_posix() for path in ROOT.rglob("*") if path.is_file()}
    require(actual == set(file_hashes) | {"manifest.json"}, "Build requires a fresh archive extraction")


def build(local_python, fresh):
    """Stage only hashed RECORD entries under site-packages into a pip-free venv."""
    require(Path(sys.executable).resolve() == local_python.resolve(),
            "Start build with the local venv interpreter")
    require(sys.prefix != sys.base_prefix, "Dependency interpreter must be a venv")
    verify_payload()
    site = Path(sys.prefix) / "lib" / f"python{sys.version_info.major}.{sys.version_info.minor}" / "site-packages"
    pins = dict(line.split("==") for line in (ROOT / "requirements-offline.txt").read_text().splitlines())
    installed = {}
    for dist in distributions(path=[str(site)]):
        name = dist.metadata["Name"].lower()
        if name in pins:
            require(name not in installed, f"Duplicate installed distribution {name}")
            installed[name] = dist
    require({name: dist.version for name, dist in installed.items()} == pins,
            "Installed versions differ from the reviewed offline pins")
    require(not fresh.exists() and fresh.is_relative_to(ROOT),
            "Use a fresh venv path inside the extracted bundle")
    subprocess.run([sys.executable, "-I", "-B", "-m", "venv", "--without-pip", str(fresh)],
                   check=True, env=isolated_env(ROOT), cwd=ROOT)
    target = fresh / site.relative_to(sys.prefix)
    dependencies = {}
    for name, dist in sorted(installed.items()):
        copied = {}
        for entry in dist.files:
            relative = Path(entry)
            if ".." in relative.parts or relative.suffix in {".pyc", ".pth"} or entry.hash is None:
                continue
            src = site / relative
            require(not relative.is_absolute() and src.resolve() == src,
                    f"Unsafe dependency path {relative}")
            raw = src.read_bytes()
            digest = sha256(raw)
            require(entry.hash.mode == "sha256" and entry.hash.value ==
                    urlsafe_b64encode(digest.digest()).decode().rstrip("="),
                    f"Installed file differs from RECORD: {name}/{relative}")
            dest = target / relative
            dest.parent.mkdir(parents=True, exist_ok=True)
            with dest.open("xb") as output:
                output.write(raw)
            copied[relative.as_posix()] = digest.hexdigest()
        require(copied, f"No RECORD entries copied for {name}")
        dependencies[name] = {"version": dist.version, "files": len(copied),
                              "sha256": sha256(json.dumps(copied, sort_keys=True).encode()).hexdigest()}
    print(json.dumps({"venv": sys.prefix, "interpreter": str(local_python),
                      "python": sys.version.split()[0], "installed_distributions": dependencies}))


def isolate(fresh):
    """Deny sockets in native code as well as Python before importing app code."""
    require(sys.flags.isolated and sys.flags.no_site and sys.dont_write_bytecode,
            "Run with -I -B -S")
    require(Path(sys.executable).parent.parent == fresh and fresh.is_relative_to(ROOT),
            "Use the fresh bundle venv")
    lib = ctypes.CDLL("libseccomp.so.2")
    lib.seccomp_init.argtypes = [ctypes.c_uint32]
    lib.seccomp_init.restype = ctypes.c_void_p
    lib.seccomp_rule_add.argtypes = [ctypes.c_void_p, ctypes.c_uint32, ctypes.c_int, ctypes.c_uint]
    lib.seccomp_syscall_resolve_name.argtypes = [ctypes.c_char_p]
    lib.seccomp_load.argtypes = [ctypes.c_void_p]
    lib.seccomp_release.argtypes = [ctypes.c_void_p]
    ctx = lib.seccomp_init(0x7fff0000)
    require(ctx, "seccomp unavailable")
    try:
        for call in (b"socket", b"socketpair", b"connect", b"sendto", b"sendmsg", b"sendmmsg", b"io_uring_setup"):
            number = lib.seccomp_syscall_resolve_name(call)
            require(number >= 0 and lib.seccomp_rule_add(ctx, 0x00050000 | errno.EPERM, number, 0) == 0,
                    f"Cannot deny syscall {call!r}")
        require(lib.seccomp_load(ctx) == 0, "Cannot load seccomp filter")
    finally:
        lib.seccomp_release(ctx)
    try:
        socket.socket()
    except PermissionError:
        print("Kernel socket denial verified")
    else:
        raise RuntimeError("Kernel socket denial failed")

    # No caller credentials, user site, installed .pth execution or external launchers.
    os.environ.clear()
    os.environ.update(isolated_env(ROOT))
    os.chdir(ROOT)
    site = fresh / "lib" / f"python{sys.version_info.major}.{sys.version_info.minor}" / "site-packages"
    sys.path.extend([str(APP), str(site)])

    def audit(event, args):
        if event == "open" and not isinstance(args[0], int):
            path = Path(os.fsdecode(args[0])).resolve()
            writing = args[2] & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND)
            if not path.is_relative_to(ROOT) and (writing or path.is_relative_to("/opt/data")):
                raise PermissionError(f"Read/write outside scratch denied: {path.name}")
        if event in {"os.remove", "os.rmdir", "os.mkdir", "os.rename", "os.link", "os.symlink", "os.chmod", "os.utime", "os.truncate"}:
            paths = args[:2] if event in {"os.rename", "os.link", "os.symlink"} else args[:1]
            for path in paths:
                # multiprocessing truncates its unlinked scratch arena by descriptor.
                name = os.readlink(f"/proc/self/fd/{path}") if isinstance(path, int) else os.fsdecode(path)
                if not Path(name).resolve().is_relative_to(ROOT):
                    raise PermissionError("Mutation outside scratch denied")
        if event == "subprocess.Popen" and args[:2] == ("/bin/sleep", ["/bin/sleep", "0.05"]):
            return  # Existing dead-owner test; seccomp is inherited.
        if event in {"subprocess.Popen", "os.system", "os.exec", "os.posix_spawn"}:
            raise PermissionError("External process denied")
    sys.addaudithook(audit)


class Clock(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls.fromisoformat(DAY + "T10:00:00+05:30").astimezone(tz).replace(tzinfo=tz)


def success(data):
    return {"status": "success", "data": data}


class Broker:
    def get_positions(self):
        return success([])

    def get_order_list(self):
        return success([])

    def get_fund_limits(self):
        return success({"availabelBalance": 30000, "utilizedAmount": 0})

    def expiry_list(self, **kwargs):
        return success({"data": ["2026-11-24"]})


def stub(engine):
    class Client(engine.SurvivorDhanClient):
        def __init__(self, *, policy=None):
            self._policy = policy
            self.dhan = Broker()
            self.tm = SimpleNamespace(access_token="synthetic", token_expiry=Clock.now() + timedelta(days=1))

        def get_live_equity_and_positions(self):
            return 30000, 30000, 0, []

        def fetch_candles(self, *args, **kwargs):
            return engine.pd.DataFrame()

        def ensure_ip_whitelisted(self, *, allow_update):
            require(not allow_update, "Unexpected IP mutation")
            return {"ordersAllowed": True}

        def _candle_response(self, *args):
            stamp = Clock.now(engine.IST).timestamp()
            return success({"timestamp": [stamp], "open": [100], "high": [101],
                            "low": [99], "close": [100], "volume": [10]})

    engine.SurvivorDhanClient = Client
    engine.datetime = Clock
    engine.load_env_file = lambda *args, **kwargs: {}
    def unexpected(*args, **kwargs):
        raise RuntimeError("Unexpected token management or notification path")
    engine.DhanTokenManager = unexpected
    engine.send_telegram = unexpected


def write(path, data):
    path.write_text(json.dumps(data), encoding="utf-8")


def smoke(engine, action):
    state_path = APP / "survivor_state.json"
    if action == "tick":
        state = dict(engine.load_state(read_only=True))
        state.update(status="ALIVE", mortality_armed=True, daily_date=DAY, daily_start_equity=30000)
        write(state_path, state)
        original = state_path.read_bytes()
    if action == "premarket":
        write(APP / "exchange_calendar.json", {"source": "synthetic fixture",
              "sessions": {date: ["09:15", "15:30"] for date in (DAY, "2026-11-24")}})
    # The manual flatten CLI role is necessary to enter the actual flatten code.
    # The client has no order methods and cannot instantiate the real SDK.
    if action == "flatten":
        os.environ["SURVIVOR_LIVE_TRADING"] = "1"
    sys.argv = ["survivor_engine.py", action] + (["--live"] if action == "flatten" else [])
    result = engine.main()
    require(not result, f"{action} returned {result}")
    if action == "tick":
        require(state_path.read_bytes() == original, "Dry tick changed on-disk state")
        print("Dry tick preserved on-disk state")
    if action == "flatten":
        require(engine.load_state(read_only=True)["emergency_recovery"]["status"] == "verified",
                "Flatten did not complete its recovery state transition")


def fixture(engine):
    state = dict(engine.load_state(read_only=True))
    state.update(status="ALIVE", mortality_armed=True, current_equity=30000, available_cash=30000,
                 daily_start_equity=30200, daily_date=DAY, daily_trades_count=1, daily_consecutive_losses=1,
                 daily_halted=True, daily_halt_reason="Synthetic loss limit", total_trades=1,
                 wins=0, losses=1, realized_pnl_total=-140, last_sl_direction="LONG",
                 last_sl_time=DAY + "T09:55:00+05:30")
    row = dict(trade_no=1, order_id="entry-fixture", security_id="99001", exchange_segment="NSE_FNO",
               entry_time=DAY + "T09:40:00+05:30", weapon="OPTIONS", direction="LONG",
               symbol="SYNTHETIC", exit_reason="STOP", exit_time=state["last_sl_time"])
    instrument = [row["security_id"], row["exchange_segment"], "INTRADAY"]
    entry = dict(id=row["order_id"], status="TRADED", side="BUY", quantity=65, price=100.0,
                 instrument=instrument, broker_charge=5.0, created=row["entry_time"])
    exit_fill = dict(entry, id="exit-fixture", side="SELL", price=98.0, created=row["exit_time"])
    proof = dict(trade=engine._trade_identity(row), epoch="synthetic-epoch", instrument=instrument,
                 entry=entry, exits=[exit_fill])
    row.update(engine._closure_fields(proof))
    ledger = dict(cycle=None, alerts={}, fills={}, flat_book={"entry-fixture": 65, "exit-fixture": 65},
                  replay_epoch=proof["epoch"], replay_executions=engine._proof_executions(proof),
                  replay_check="verification_required")
    write(APP / "survivor_state.json", state)
    write(APP / "survivor_trades.json", [row])
    write(APP / "survivor_exits.json", ledger)
    require(dict(engine.load_state(read_only=True)) == state, "Candidate lost state data")
    require(engine._journal_proof(engine._read_exit_journal()[0]) == proof, "Candidate rejected proof")
    with engine.exit_lock(engine.EXIT_LEDGER) as observed:
        require(observed == ledger, "Candidate rejected ledger")
    result = engine.SurvivorDhanClient(policy=engine.RolePolicy(flatten=True)).emergency_flatten_all()
    require(not result.flat and "replay evidence requires verification" in str(result),
            "Candidate accepted flatten with a pending replay verification")
    print("Candidate flatten refused to claim flat while the replay fence required verification")
    print(f"Candidate parsed halted risk state, verified journal proof and replay-fenced exit ledger; net {row['net_pnl']:.2f}")


def rollback(engine):
    state = json.loads(Path(engine.STATE_FILE).read_text())
    journal = json.loads(Path(engine.JOURNAL_FILE).read_text())
    require(engine.load_state() == state, "Rollback loader lost risk state")
    require(engine.load_journal() == journal, "Rollback loader lost journal rows")
    print("Deployed loaders preserved halted risk state and journal fields on read")
    opened = []
    def trace(event, args):
        if event == "open" and not isinstance(args[0], int):
            opened.append(Path(os.fsdecode(args[0])).name)
    sys.addaudithook(trace)
    output = io.StringIO()
    with redirect_stdout(output):
        engine.cmd_reconcile()
    print(output.getvalue(), end="")
    require("100% Cash Overnight: True" in output.getvalue(), "Deployed flat report not reproduced")
    require(json.loads(Path(engine.JOURNAL_FILE).read_text()) == journal,
            "Deployed reconcile altered the fixture journal")
    require("survivor_exits.json" not in opened, "Deployed reconcile unexpectedly consumes exit ledger")
    print("BLOCKED: deployed reconcile reported flat without reading the candidate replay-fenced ledger")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=(*SUCCESS, "build", "fixture", "rollback", "tests"))
    parser.add_argument("fresh", type=Path)
    parser.add_argument("--local-python", type=Path)
    args = parser.parse_args()
    fresh = args.fresh.resolve()
    if args.action == "build":
        require(args.local_python is not None, "Supply the reviewed local venv interpreter")
        build(args.local_python.absolute(), fresh)
        return
    isolate(fresh)
    if args.action == "tests":
        import multiprocessing.heap
        import unittest
        multiprocessing.heap.Arena._dir_candidates = [str(ROOT)]
        suite = unittest.defaultTestLoader.discover(str(ROOT / "tests"))
        result = unittest.TextTestRunner(verbosity=1).run(suite)
        require(result.testsRun > 0 and result.wasSuccessful(), "Exported tests failed")
        return
    import survivor_engine as engine
    require(Path(engine.__file__).resolve().parent == APP, "Engine was not loaded from artifact")
    stub(engine)
    if args.action == "fixture":
        fixture(engine)
    elif args.action == "rollback":
        rollback(engine)
    else:
        smoke(engine, args.action)


if __name__ == "__main__":
    main()
