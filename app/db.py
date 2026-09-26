"""SQLite persistence layer (settings + targets + events/deliveries/attempts)."""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any

DB_PATH = os.environ.get("REPLAY_DB", "/data/webhook_replay.db")

# Bump whenever the settings/targets/events/deliveries/attempts table definitions
# change; backup manifests record this and incompatible snapshots are rejected
# on restore.
#   v1: settings / events / attempts
#   v2: + targets table, + deliveries table, + attempts.target column
SCHEMA_VERSION = 2

DEFAULT_TARGET = "default"

_lock = threading.RLock()


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def schema_version(conn: sqlite3.Connection | None = None) -> int:
    own = conn is not None
    conn = conn or get_conn()
    try:
        row = conn.execute("PRAGMA user_version").fetchone()
        return int(row[0])
    finally:
        if not own:
            conn.close()


def init_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    with _lock, get_conn() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS settings (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                target_url TEXT NOT NULL DEFAULT 'http://127.0.0.1:8000/recv',
                hmac_secret TEXT NOT NULL DEFAULT 'shhh',
                max_retries INTEGER NOT NULL DEFAULT 3,
                timeout_seconds REAL NOT NULL DEFAULT 5.0
            );
            INSERT OR IGNORE INTO settings (id) VALUES (1);

            CREATE TABLE IF NOT EXISTS targets (
                name       TEXT PRIMARY KEY,
                url        TEXT NOT NULL,
                enabled    INTEGER NOT NULL DEFAULT 1,
                position   INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS events (
                event_id   TEXT PRIMARY KEY,
                seq        INTEGER NOT NULL,
                payload    TEXT NOT NULL,
                status     TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS deliveries (
                event_id   TEXT NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
                target     TEXT NOT NULL,
                status     TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (event_id, target)
            );

            CREATE TABLE IF NOT EXISTS attempts (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id     TEXT NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
                target       TEXT NOT NULL DEFAULT 'default',
                attempt_no   INTEGER NOT NULL,
                replay_no    INTEGER NOT NULL,
                idem_key     TEXT NOT NULL,
                request_at   TEXT NOT NULL,
                response_at  TEXT,
                http_status  INTEGER,
                outcome      TEXT NOT NULL,
                elapsed_ms   INTEGER,
                req_headers  TEXT,
                resp_excerpt TEXT
            );
            """
        )
        # migration from v1: attempts gained a target column
        cols = [r[1] for r in conn.execute("PRAGMA table_info(attempts)")]
        if "target" not in cols:
            conn.execute(
                "ALTER TABLE attempts ADD COLUMN target TEXT NOT NULL DEFAULT 'default'")
        # seed the implicit single target from the legacy settings URL
        row = conn.execute("SELECT COUNT(*) AS n FROM targets").fetchone()
        if int(row["n"]) == 0:
            legacy = conn.execute(
                "SELECT target_url FROM settings WHERE id = 1").fetchone()
            now = utcnow()
            conn.execute(
                "INSERT INTO targets (name, url, enabled, position, created_at, updated_at)"
                " VALUES (?, ?, 1, 0, ?, ?)",
                (DEFAULT_TARGET, legacy["target_url"], now, now),
            )
        conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")


# ---------- settings ----------

def get_settings() -> dict[str, Any]:
    with _lock, get_conn() as conn:
        row = conn.execute("SELECT * FROM settings WHERE id = 1").fetchone()
        return dict(row)


def update_settings(target_url: str, hmac_secret: str, max_retries: int,
                    timeout_seconds: float) -> dict[str, Any]:
    """Legacy single-target update: keeps the 'default' target in sync."""
    with _lock, get_conn() as conn:
        conn.execute(
            "UPDATE settings SET target_url=?, hmac_secret=?, max_retries=?, timeout_seconds=? WHERE id=1",
            (target_url, hmac_secret, max_retries, timeout_seconds),
        )
        _upsert_default_target(conn, target_url)
    return get_settings()


def update_globals(hmac_secret: str, max_retries: int,
                   timeout_seconds: float) -> None:
    """Update only the global delivery knobs; leave every target untouched."""
    with _lock, get_conn() as conn:
        conn.execute(
            "UPDATE settings SET hmac_secret=?, max_retries=?, timeout_seconds=? WHERE id=1",
            (hmac_secret, max_retries, timeout_seconds),
        )


def _upsert_default_target(conn: sqlite3.Connection, url: str) -> None:
    now = utcnow()
    row = conn.execute(
        "SELECT name FROM targets WHERE name=?", (DEFAULT_TARGET,)).fetchone()
    if row is None:
        pos = conn.execute(
            "SELECT COALESCE(MAX(position), -1) + 1 AS p FROM targets").fetchone()
        conn.execute(
            "INSERT INTO targets (name, url, enabled, position, created_at, updated_at)"
            " VALUES (?, ?, 1, ?, ?, ?)",
            (DEFAULT_TARGET, url, int(pos["p"]), now, now),
        )
    else:
        conn.execute(
            "UPDATE targets SET url=?, updated_at=? WHERE name=?",
            (url, now, DEFAULT_TARGET),
        )


# ---------- targets ----------

def list_targets() -> list[dict[str, Any]]:
    with _lock, get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM targets ORDER BY position ASC, name ASC").fetchall()
        return [dict(r) for r in rows]


def enabled_targets() -> list[dict[str, Any]]:
    """Dispatch fan-out set, in configured order."""
    return [t for t in list_targets() if t["enabled"]]


def replace_targets(targets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Atomically replace the whole target set (already validated)."""
    now = utcnow()
    with _lock, get_conn() as conn:
        conn.execute("DELETE FROM targets")
        for pos, t in enumerate(targets):
            conn.execute(
                "INSERT INTO targets (name, url, enabled, position, created_at, updated_at)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (t["name"], t["url"], 1 if t.get("enabled", True) else 0,
                 pos, now, now),
            )
        # keep the legacy single-target column pointing at something sensible
        primary = next((t["url"] for t in targets if t["name"] == DEFAULT_TARGET),
                       targets[0]["url"])
        conn.execute("UPDATE settings SET target_url=? WHERE id=1", (primary,))
    return list_targets()


# ---------- events ----------

def next_seq(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COALESCE(MAX(seq), 0) + 1 AS s FROM events").fetchone()
    return int(row["s"])


def upsert_event(event_id: str, payload: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    """Insert or replace an event. Returns (event, created)."""
    now = utcnow()
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    with _lock, get_conn() as conn:
        existing = conn.execute(
            "SELECT seq FROM events WHERE event_id=?", (event_id,)
        ).fetchone()
        if existing is None:
            seq = next_seq(conn)
            conn.execute(
                "INSERT INTO events (event_id, seq, payload, status, created_at, updated_at)"
                " VALUES (?, ?, ?, 'pending', ?, ?)",
                (event_id, seq, raw, now, now),
            )
            created = True
        else:
            # new content -> fresh lifecycle (attempts + per-target states reset)
            conn.execute(
                "DELETE FROM attempts WHERE event_id=?", (event_id,)
            )
            conn.execute(
                "DELETE FROM deliveries WHERE event_id=?", (event_id,)
            )
            conn.execute(
                "UPDATE events SET payload=?, status='pending', updated_at=? WHERE event_id=?",
                (raw, now, event_id),
            )
            seq = existing["seq"]
            created = False
        row = conn.execute("SELECT * FROM events WHERE event_id=?", (event_id,)).fetchone()
        return dict(row), created


def get_event(event_id: str) -> dict[str, Any] | None:
    with _lock, get_conn() as conn:
        row = conn.execute("SELECT * FROM events WHERE event_id=?", (event_id,)).fetchone()
        return dict(row) if row else None


def list_events() -> list[dict[str, Any]]:
    with _lock, get_conn() as conn:
        rows = conn.execute(
            """
            SELECT e.*,
                   (SELECT COUNT(*) FROM attempts a WHERE a.event_id=e.event_id) AS attempt_count,
                   (SELECT MAX(replay_no) FROM attempts a WHERE a.event_id=e.event_id) AS last_replay_no
            FROM events e ORDER BY e.seq ASC
            """
        ).fetchall()
        events = [dict(r) for r in rows]
        dmap = deliveries_map()
    for e in events:
        e["deliveries"] = dmap.get(e["event_id"], {})
    return events


def events_by_status(statuses: tuple[str, ...]) -> list[dict[str, Any]]:
    placeholders = ",".join("?" for _ in statuses)
    with _lock, get_conn() as conn:
        rows = conn.execute(
            f"SELECT * FROM events WHERE status IN ({placeholders}) ORDER BY seq ASC",
            statuses,
        ).fetchall()
        return [dict(r) for r in rows]


def set_event_status(event_id: str, status: str) -> None:
    with _lock, get_conn() as conn:
        conn.execute(
            "UPDATE events SET status=?, updated_at=? WHERE event_id=?",
            (status, utcnow(), event_id),
        )


# ---------- deliveries (per event x target state) ----------

DELIVERY_DELIVERED = "delivered"
DELIVERY_FAILED = "failed"


def upsert_delivery(event_id: str, target: str, status: str) -> None:
    now = utcnow()
    with _lock, get_conn() as conn:
        conn.execute(
            """INSERT INTO deliveries (event_id, target, status, updated_at)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(event_id, target)
               DO UPDATE SET status=excluded.status, updated_at=excluded.updated_at""",
            (event_id, target, status, now),
        )
        conn.execute("UPDATE events SET updated_at=? WHERE event_id=?",
                     (now, event_id))


def deliveries_for_event(event_id: str) -> list[dict[str, Any]]:
    with _lock, get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM deliveries WHERE event_id=? ORDER BY target ASC",
            (event_id,),
        ).fetchall()
        return [dict(r) for r in rows]


def deliveries_map() -> dict[str, dict[str, str]]:
    """{event_id: {target: status}} for every recorded delivery."""
    with _lock, get_conn() as conn:
        rows = conn.execute("SELECT event_id, target, status FROM deliveries").fetchall()
    out: dict[str, dict[str, str]] = {}
    for r in rows:
        out.setdefault(r["event_id"], {})[r["target"]] = r["status"]
    return out


def pending_targets(event_id: str, enabled_names: list[str]) -> list[str]:
    """Enabled targets this event has not been successfully delivered to."""
    state = deliveries_map().get(event_id, {})
    return [n for n in enabled_names if state.get(n) != DELIVERY_DELIVERED]


# ---------- attempts ----------

def latest_replay_no(conn: sqlite3.Connection, event_id: str,
                     target: str) -> int:
    row = conn.execute(
        "SELECT COALESCE(MAX(replay_no), 0) AS n FROM attempts"
        " WHERE event_id=? AND target=?",
        (event_id, target),
    ).fetchone()
    return int(row["n"])


def record_attempt(event_id: str, *, target: str, attempt_no: int, replay_no: int,
                   idem_key: str, request_at: str, req_headers: dict[str, str],
                   response_at: str | None, http_status: int | None,
                   outcome: str, elapsed_ms: int, resp_excerpt: str) -> None:
    with _lock, get_conn() as conn:
        conn.execute(
            """INSERT INTO attempts
               (event_id, target, attempt_no, replay_no, idem_key, request_at,
                response_at, http_status, outcome, elapsed_ms, req_headers, resp_excerpt)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (event_id, target, attempt_no, replay_no, idem_key, request_at,
             response_at, http_status, outcome, elapsed_ms,
             json.dumps(req_headers, ensure_ascii=False), resp_excerpt[:2000]),
        )
        conn.execute("UPDATE events SET updated_at=? WHERE event_id=?",
                     (utcnow(), event_id))


def timeline_for_event(event_id: str) -> list[dict[str, Any]]:
    with _lock, get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM attempts WHERE event_id=? ORDER BY id ASC", (event_id,)
        ).fetchall()
        return [dict(r) for r in rows]


def timeline(limit: int = 200) -> list[dict[str, Any]]:
    with _lock, get_conn() as conn:
        rows = conn.execute(
            """SELECT a.*, e.seq FROM attempts a
               JOIN events e ON e.event_id = a.event_id
               ORDER BY a.id DESC LIMIT ?""",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]


def clear_history() -> None:
    """Delete all events, deliveries and attempts. Settings/targets preserved."""
    with _lock, get_conn() as conn:
        conn.execute("DELETE FROM attempts")
        conn.execute("DELETE FROM deliveries")
        conn.execute("DELETE FROM events")


# ---------- snapshot backup / restore ----------

def dump_snapshot() -> dict[str, list[dict[str, Any]]]:
    """Read all tables in one consistent transaction."""
    with _lock, get_conn() as conn:
        settings = [dict(r) for r in conn.execute("SELECT * FROM settings ORDER BY id")]
        targets = [dict(r) for r in
                   conn.execute("SELECT * FROM targets ORDER BY position, name")]
        events = [dict(r) for r in conn.execute("SELECT * FROM events ORDER BY seq")]
        deliveries = [dict(r) for r in
                      conn.execute("SELECT * FROM deliveries ORDER BY event_id, target")]
        attempts = [dict(r) for r in conn.execute("SELECT * FROM attempts ORDER BY id")]
    return {"settings": settings, "targets": targets, "events": events,
            "deliveries": deliveries, "attempts": attempts}


def restore_rows(settings: list[dict[str, Any]], targets: list[dict[str, Any]],
                 events: list[dict[str, Any]], deliveries: list[dict[str, Any]],
                 attempts: list[dict[str, Any]]) -> None:
    """Replace events/deliveries/attempts inside a single transaction.

    Settings and targets are intentionally untouched (current configuration is
    preserved); the snapshot's copies are archived for reference only.
    Must be called with data already validated; FK enforcement is on and the
    whole statement sequence commits atomically or rolls back.
    """
    with _lock, get_conn() as conn:
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("DELETE FROM attempts")
            conn.execute("DELETE FROM deliveries")
            conn.execute("DELETE FROM events")
            conn.executemany(
                "INSERT INTO events (event_id, seq, payload, status, created_at, updated_at)"
                " VALUES (:event_id, :seq, :payload, :status, :created_at, :updated_at)",
                events,
            )
            conn.executemany(
                "INSERT INTO deliveries (event_id, target, status, updated_at)"
                " VALUES (:event_id, :target, :status, :updated_at)",
                deliveries,
            )
            conn.executemany(
                """INSERT INTO attempts
                   (id, event_id, target, attempt_no, replay_no, idem_key, request_at,
                    response_at, http_status, outcome, elapsed_ms, req_headers, resp_excerpt)
                   VALUES (:id, :event_id, :target, :attempt_no, :replay_no, :idem_key,
                           :request_at, :response_at, :http_status, :outcome, :elapsed_ms,
                           :req_headers, :resp_excerpt)""",
                attempts,
            )
            # keep AUTOINCREMENT counter above the highest restored id
            if attempts:
                conn.execute(
                    "INSERT OR IGNORE INTO sqlite_sequence(name, seq) VALUES('attempts', 0)")
                conn.execute(
                    "UPDATE sqlite_sequence SET seq = (SELECT MAX(id) FROM attempts)"
                    " WHERE name = 'attempts'")
            else:
                conn.execute("DELETE FROM sqlite_sequence WHERE name = 'attempts'")
            conn.commit()
        except Exception:
            conn.rollback()
            raise
