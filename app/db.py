"""SQLite persistence layer.

Tables:
    settings    -- the legacy single-target configuration (always preserved)
    targets     -- 2-5 named fanout targets (empty => single-target mode)
    events      -- imported events; status aggregates per-target deliveries
    attempts    -- one row per HTTP attempt (target_id NULL = legacy target)
    deliveries  -- per (event, target) delivery state, used for partial-failure
                   retries and per-target result display
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any

DB_PATH = os.environ.get("REPLAY_DB", "/data/webhook_replay.db")

# Bump whenever the settings/events/attempts/targets/deliveries table
# definitions change; backup manifests record this and incompatible snapshots
# are rejected on restore (v1 snapshots are migrated on read).
SCHEMA_VERSION = 2

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

            CREATE TABLE IF NOT EXISTS events (
                event_id   TEXT PRIMARY KEY,
                seq        INTEGER NOT NULL,
                payload    TEXT NOT NULL,
                status     TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS targets (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                name            TEXT NOT NULL UNIQUE,
                url             TEXT NOT NULL,
                hmac_secret     TEXT NOT NULL DEFAULT '',
                max_retries     INTEGER NOT NULL DEFAULT 3,
                timeout_seconds REAL NOT NULL DEFAULT 5.0,
                created_at      TEXT NOT NULL,
                updated_at      TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS attempts (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id     TEXT NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
                target_id    INTEGER REFERENCES targets(id) ON DELETE CASCADE,
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

            CREATE TABLE IF NOT EXISTS deliveries (
                event_id         TEXT NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
                target_id        INTEGER NOT NULL REFERENCES targets(id) ON DELETE CASCADE,
                status           TEXT NOT NULL DEFAULT 'pending',
                replay_no        INTEGER NOT NULL DEFAULT 0,
                attempts_count   INTEGER NOT NULL DEFAULT 0,
                last_http_status INTEGER,
                last_outcome     TEXT,
                last_attempt_at  TEXT,
                updated_at       TEXT NOT NULL,
                PRIMARY KEY (event_id, target_id)
            );
            """
        )
        # Migration for v1 database files: attempts existed without target_id
        # and the targets/deliveries tables were created just above.
        cols = {r[1] for r in conn.execute("PRAGMA table_info(attempts)")}
        if "target_id" not in cols:
            conn.execute(
                "ALTER TABLE attempts ADD COLUMN target_id INTEGER"
                " REFERENCES targets(id) ON DELETE CASCADE")
        conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")


# ---------- settings (legacy single target) ----------

def get_settings() -> dict[str, Any]:
    with _lock, get_conn() as conn:
        row = conn.execute("SELECT * FROM settings WHERE id = 1").fetchone()
        return dict(row)


def update_settings(target_url: str, hmac_secret: str, max_retries: int,
                    timeout_seconds: float) -> dict[str, Any]:
    with _lock, get_conn() as conn:
        conn.execute(
            "UPDATE settings SET target_url=?, hmac_secret=?, max_retries=?, timeout_seconds=? WHERE id=1",
            (target_url, hmac_secret, max_retries, timeout_seconds),
        )
    return get_settings()


# ---------- named fanout targets ----------

def list_targets() -> list[dict[str, Any]]:
    with _lock, get_conn() as conn:
        rows = conn.execute("SELECT * FROM targets ORDER BY id ASC").fetchall()
        return [dict(r) for r in rows]


def replace_targets(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Replace the whole named-target set, matching rows by name.

    Existing names are updated in place (ids stable); new names are inserted;
    names no longer present are deleted (attempts/deliveries cascade away).
    Caller is responsible for 2-5 / uniqueness validation and for ensuring
    the dispatcher is idle.
    """
    now = utcnow()
    with _lock, get_conn() as conn:
        existing = {r["name"]: r for r in
                    conn.execute("SELECT * FROM targets").fetchall()}
        keep: set[str] = set()
        for item in items:
            name = item["name"]
            keep.add(name)
            if name in existing:
                conn.execute(
                    "UPDATE targets SET url=?, hmac_secret=?, max_retries=?,"
                    " timeout_seconds=?, updated_at=? WHERE id=?",
                    (item["url"], item["hmac_secret"], item["max_retries"],
                     item["timeout_seconds"], now, existing[name]["id"]),
                )
            else:
                conn.execute(
                    "INSERT INTO targets (name, url, hmac_secret, max_retries,"
                    " timeout_seconds, created_at, updated_at)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (name, item["url"], item["hmac_secret"], item["max_retries"],
                     item["timeout_seconds"], now, now),
                )
        stale = [r["id"] for n, r in existing.items() if n not in keep]
        if stale:
            placeholders = ",".join("?" for _ in stale)
            conn.execute(
                f"DELETE FROM targets WHERE id IN ({placeholders})", stale)
        rows = conn.execute("SELECT * FROM targets ORDER BY id ASC").fetchall()
        return [dict(r) for r in rows]


def clear_targets() -> None:
    """Drop all named targets and their attempts/deliveries (cascade)."""
    with _lock, get_conn() as conn:
        conn.execute("DELETE FROM targets")


def effective_targets(settings: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Named targets if configured, otherwise a single legacy pseudo-target.

    The pseudo-target has id=None: attempts keep NULL target_id, no delivery
    rows are written and the legacy idempotency-key format is used.
    """
    named = list_targets()
    if named:
        return named
    s = settings or get_settings()
    return [{
        "id": None,
        "name": "default",
        "url": s["target_url"],
        "hmac_secret": s["hmac_secret"],
        "max_retries": int(s["max_retries"]),
        "timeout_seconds": float(s["timeout_seconds"]),
    }]


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
            # new content -> fresh lifecycle
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
        return [dict(r) for r in rows]


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


def latest_replay_no(conn: sqlite3.Connection, event_id: str) -> int:
    row = conn.execute(
        "SELECT COALESCE(MAX(replay_no), 0) AS n FROM attempts WHERE event_id=?",
        (event_id,),
    ).fetchone()
    return int(row["n"])


def record_attempt(event_id: str, *, attempt_no: int, replay_no: int,
                   idem_key: str, request_at: str, req_headers: dict[str, str],
                   response_at: str | None, http_status: int | None,
                   outcome: str, elapsed_ms: int, resp_excerpt: str,
                   target_id: int | None = None) -> None:
    with _lock, get_conn() as conn:
        conn.execute(
            """INSERT INTO attempts
               (event_id, target_id, attempt_no, replay_no, idem_key, request_at, response_at,
                http_status, outcome, elapsed_ms, req_headers, resp_excerpt)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (event_id, target_id, attempt_no, replay_no, idem_key, request_at, response_at,
             http_status, outcome, elapsed_ms,
             json.dumps(req_headers, ensure_ascii=False), resp_excerpt[:2000]),
        )
        conn.execute("UPDATE events SET updated_at=? WHERE event_id=?",
                     (utcnow(), event_id))


def timeline_for_event(event_id: str) -> list[dict[str, Any]]:
    with _lock, get_conn() as conn:
        rows = conn.execute(
            """SELECT a.*, t.name AS target_name FROM attempts a
               LEFT JOIN targets t ON t.id = a.target_id
               WHERE a.event_id=? ORDER BY a.id ASC""",
            (event_id,),
        ).fetchall()
        return [dict(r) for r in rows]


def timeline(limit: int = 200) -> list[dict[str, Any]]:
    with _lock, get_conn() as conn:
        rows = conn.execute(
            """SELECT a.*, e.seq, t.name AS target_name FROM attempts a
               JOIN events e ON e.event_id = a.event_id
               LEFT JOIN targets t ON t.id = a.target_id
               ORDER BY a.id DESC LIMIT ?""",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]


def clear_history() -> None:
    """Delete all events and attempts/deliveries. Settings/targets preserved."""
    with _lock, get_conn() as conn:
        conn.execute("DELETE FROM attempts")
        conn.execute("DELETE FROM deliveries")
        conn.execute("DELETE FROM events")


# ---------- per-target delivery state ----------

def upsert_delivery(event_id: str, target_id: int, status: str,
                    replay_no: int) -> None:
    """(Re)start a delivery: reset counters while keeping the row identity."""
    now = utcnow()
    with _lock, get_conn() as conn:
        conn.execute(
            """INSERT INTO deliveries
               (event_id, target_id, status, replay_no, attempts_count,
                last_http_status, last_outcome, last_attempt_at, updated_at)
               VALUES (?, ?, ?, ?, 0, NULL, NULL, NULL, ?)
               ON CONFLICT(event_id, target_id) DO UPDATE SET
                 status=excluded.status,
                 replay_no=excluded.replay_no,
                 attempts_count=0,
                 last_http_status=NULL,
                 last_outcome=NULL,
                 last_attempt_at=NULL,
                 updated_at=excluded.updated_at""",
            (event_id, target_id, status, replay_no, now),
        )


def update_delivery_attempt(event_id: str, target_id: int, *, status: str,
                            attempts_count: int, http_status: int | None,
                            outcome: str, attempt_at: str) -> None:
    with _lock, get_conn() as conn:
        conn.execute(
            """UPDATE deliveries SET status=?, attempts_count=?,
                   last_http_status=?, last_outcome=?, last_attempt_at=?, updated_at=?
               WHERE event_id=? AND target_id=?""",
            (status, attempts_count, http_status, outcome, attempt_at,
             utcnow(), event_id, target_id),
        )


def _delivery_rows(conn: sqlite3.Connection, where: str = "",
                   params: tuple = ()) -> list[dict[str, Any]]:
    rows = conn.execute(
        f"""SELECT d.*, t.name AS target_name FROM deliveries d
            JOIN targets t ON t.id = d.target_id {where}
            ORDER BY d.event_id ASC, d.target_id ASC""",
        params,
    ).fetchall()
    return [dict(r) for r in rows]


def deliveries_for_event(event_id: str) -> list[dict[str, Any]]:
    with _lock, get_conn() as conn:
        return _delivery_rows(conn, "WHERE d.event_id=?", (event_id,))


def list_deliveries() -> list[dict[str, Any]]:
    with _lock, get_conn() as conn:
        return _delivery_rows(conn)


def reset_transient_deliveries() -> int:
    """Move queued/running deliveries back to pending (startup/restore safety)."""
    with _lock, get_conn() as conn:
        cur = conn.execute(
            "UPDATE deliveries SET status='pending', updated_at=? "
            "WHERE status IN ('queued','running')",
            (utcnow(),),
        )
        return cur.rowcount


# ---------- snapshot backup / restore ----------

def dump_snapshot() -> dict[str, list[dict[str, Any]]]:
    """Read all tables in one consistent transaction."""
    with _lock, get_conn() as conn:
        settings = [dict(r) for r in conn.execute("SELECT * FROM settings ORDER BY id")]
        targets = [dict(r) for r in conn.execute("SELECT * FROM targets ORDER BY id")]
        events = [dict(r) for r in conn.execute("SELECT * FROM events ORDER BY seq")]
        deliveries = [dict(r) for r in conn.execute(
            "SELECT * FROM deliveries ORDER BY event_id, target_id")]
        attempts = [dict(r) for r in conn.execute("SELECT * FROM attempts ORDER BY id")]
    return {"settings": settings, "targets": targets, "events": events,
            "deliveries": deliveries, "attempts": attempts}


def restore_rows(settings: list[dict[str, Any]], targets: list[dict[str, Any]],
                 events: list[dict[str, Any]], attempts: list[dict[str, Any]],
                 deliveries: list[dict[str, Any]]) -> None:
    """Replace targets/events/attempts/deliveries inside a single transaction.

    Legacy settings are intentionally untouched (current single-target
    configuration is preserved). Must be called with data already validated;
    FK enforcement is on and the whole sequence commits atomically or rolls
    back.
    """
    with _lock, get_conn() as conn:
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("DELETE FROM attempts")
            conn.execute("DELETE FROM deliveries")
            conn.execute("DELETE FROM events")
            conn.execute("DELETE FROM targets")
            conn.executemany(
                """INSERT INTO targets
                   (id, name, url, hmac_secret, max_retries, timeout_seconds,
                    created_at, updated_at)
                   VALUES (:id, :name, :url, :hmac_secret, :max_retries,
                           :timeout_seconds, :created_at, :updated_at)""",
                targets,
            )
            conn.executemany(
                "INSERT INTO events (event_id, seq, payload, status, created_at, updated_at)"
                " VALUES (:event_id, :seq, :payload, :status, :created_at, :updated_at)",
                events,
            )
            conn.executemany(
                """INSERT INTO deliveries
                   (event_id, target_id, status, replay_no, attempts_count,
                    last_http_status, last_outcome, last_attempt_at, updated_at)
                   VALUES (:event_id, :target_id, :status, :replay_no, :attempts_count,
                           :last_http_status, :last_outcome, :last_attempt_at, :updated_at)""",
                deliveries,
            )
            conn.executemany(
                """INSERT INTO attempts
                   (id, event_id, target_id, attempt_no, replay_no, idem_key, request_at,
                    response_at, http_status, outcome, elapsed_ms, req_headers,
                    resp_excerpt)
                   VALUES (:id, :event_id, :target_id, :attempt_no, :replay_no, :idem_key,
                           :request_at, :response_at, :http_status, :outcome, :elapsed_ms,
                           :req_headers, :resp_excerpt)""",
                attempts,
            )
            # keep AUTOINCREMENT counters above the highest restored id
            for table, rows in (("attempts", attempts), ("targets", targets)):
                if rows:
                    conn.execute(
                        "INSERT OR IGNORE INTO sqlite_sequence(name, seq) VALUES(?, 0)",
                        (table,))
                    conn.execute(
                        "UPDATE sqlite_sequence SET seq="
                        f" (SELECT MAX(id) FROM {table}) WHERE name=?",
                        (table,))
                else:
                    conn.execute(
                        "DELETE FROM sqlite_sequence WHERE name=?", (table,))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
