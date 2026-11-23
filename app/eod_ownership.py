"""Scheduled liquidation, incident persistence and independent alert retries."""

from contextlib import closing, contextmanager
from datetime import timedelta, timezone
import fcntl
import json
import os
from pathlib import Path
import sqlite3

IST = timezone(timedelta(hours=5, minutes=30))
SLOTS = ("15:10", "15:15", "15:20", "15:25", "15:30")
WINDOW_SECONDS = 30
LOCK_POLL_SECONDS = 0.010


def scheduled_slot(instant):
    if instant.utcoffset() is None:
        raise ValueError("Scheduled dispatch requires a timezone-aware instant")
    local = instant.astimezone(IST)
    if local.strftime("%H:%M") in SLOTS:
        return local.replace(second=0, microsecond=0)
    return None


def scheduled_dispatch(instant, cli):
    """Pass the injected instant through to a fake or real CLI dispatcher."""
    if scheduled_slot(instant) is not None:
        return cli(instant, ["flatten", "--live", "--scheduled", "--quiet"])
    return None


class EodStore:
    def __init__(self, path):
        self.path = Path(path)

    @contextmanager
    def transaction(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("PRAGMA synchronous=FULL")
            db.execute("BEGIN IMMEDIATE")
            db.execute("""CREATE TABLE IF NOT EXISTS attempts (
                        id INTEGER PRIMARY KEY, day TEXT NOT NULL, slot TEXT NOT NULL,
                        status TEXT NOT NULL, pid INTEGER NOT NULL, UNIQUE(day, slot))""")
            db.execute("""CREATE TABLE IF NOT EXISTS incidents (
                        id TEXT PRIMARY KEY, day TEXT NOT NULL, kind TEXT NOT NULL,
                        message TEXT NOT NULL, resolved INTEGER NOT NULL DEFAULT 0)""")
            db.execute("""CREATE TABLE IF NOT EXISTS outbox (
                        id TEXT PRIMARY KEY, acknowledged INTEGER NOT NULL DEFAULT 0)""")
            yield db

    def begin(self, day, slot):
        with self.transaction() as db:
            cursor = db.execute("INSERT OR IGNORE INTO attempts(day, slot, status, pid) "
                                "VALUES (?, ?, 'running', ?)", (day, slot, os.getpid()))
            return cursor.lastrowid if cursor.rowcount else None

    def claimed(self, slot):
        if not self.path.is_file():
            return False
        with closing(sqlite3.connect(self.path.resolve().as_uri() + "?mode=ro", uri=True)) as db:
            if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='attempts'").fetchone() is None:
                return False
            return db.execute("SELECT 1 FROM attempts WHERE day=? AND slot=?",
                              (slot.date().isoformat(), slot.strftime("%H:%M"))).fetchone() is not None

    def _record(self, db, day, kind, details):
        alert_id = f"eod:{day}:{kind}"
        message = f"{alert_id}: {details}"
        db.execute("INSERT OR IGNORE INTO incidents(id, day, kind, message) VALUES (?, ?, ?, ?)",
                   (alert_id, day, kind, message))
        db.execute("INSERT OR IGNORE INTO outbox(id) VALUES (?)", (alert_id,))
        return message

    def record(self, day, kind, details="Exposure UNKNOWN"):
        with self.transaction() as db:
            return self._record(db, day, kind, details)

    def interrupted(self, day, attempt, pid_alive):
        with self.transaction() as db:
            earlier = db.execute("SELECT id, pid FROM attempts WHERE day=? AND status='running' AND id<?",
                                 (day, attempt)).fetchall()
            stopped = [(attempt_id,) for attempt_id, pid in earlier if not pid_alive(pid)]
            if stopped:
                db.executemany("UPDATE attempts SET status='interrupted' WHERE id=? AND status='running'", stopped)
                self._record(db, day, "interrupted", "Previous scheduled attempt stopped before confirmation")

    def finish(self, attempt, day, status, details="Exposure UNKNOWN"):
        with self.transaction() as db:
            db.execute("UPDATE attempts SET status=? WHERE id=?", (status, attempt))
            if status == "flat":
                db.execute("UPDATE incidents SET resolved=1 WHERE day=?", (day,))
                return None
            return self._record(db, day, status, details)

    def drain(self, notify):
        with open(str(self.path) + ".delivery.lock", "a", encoding="utf-8") as owner:
            try:
                fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return
            with self.transaction() as db:
                pending = db.execute("""SELECT incidents.id, message FROM incidents
                                         JOIN outbox USING(id) WHERE acknowledged=0
                                         ORDER BY incidents.rowid""").fetchall()
            for alert_id, message in pending:
                try:
                    acknowledged = notify(message) is True
                except Exception:
                    continue
                if acknowledged:
                    with self.transaction() as db:
                        db.execute("UPDATE outbox SET acknowledged=1 WHERE id=?", (alert_id,))


def owner_record(path):
    try:
        with open(path, encoding="utf-8") as lock:
            record = json.load(lock)
    except (OSError, ValueError):
        return {}
    return record if isinstance(record, dict) else {}


def publish_owner(fd, record):
    # Update in one write, padding any previous record with JSON whitespace.
    content = json.dumps(record).encode().ljust(os.fstat(fd).st_size)
    if os.pwrite(fd, content, 0) != len(content):
        raise OSError("Incomplete engine ownership record")
