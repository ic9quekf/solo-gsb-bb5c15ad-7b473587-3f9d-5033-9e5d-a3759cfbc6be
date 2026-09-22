"""FastAPI application: API + static UI + built-in test receiver."""
from __future__ import annotations

import hashlib
import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles

from . import db, snapshot
from .dispatcher import dispatcher
from .models import ReplayIn, SettingsIn
from .receiver import router as receiver_router

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    # events left mid-flight by a previous process are picked up again
    for ev in db.events_by_status(("queued", "running")):
        db.set_event_status(ev["event_id"], "pending")
    dispatcher.start()
    yield


app = FastAPI(title="Webhook Replay Lab", version="1.1.0", lifespan=lifespan)
app.include_router(receiver_router)


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


# ---------- settings ----------

@app.get("/api/settings")
def get_settings() -> dict[str, Any]:
    return db.get_settings()


@app.post("/api/settings")
async def save_settings(request: Request) -> dict[str, Any]:
    try:
        data = SettingsIn(**await request.json())
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return db.update_settings(
        data.target_url.strip(),
        data.hmac_secret,
        data.max_retries,
        data.timeout_seconds,
    )


# ---------- events ----------

def _extract_event_id(obj: dict[str, Any]) -> str:
    eid = obj.get("event_id") or obj.get("id")
    if eid is None or str(eid).strip() == "":
        digest = hashlib.sha256(
            json.dumps(obj, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()[:12]
        return f"evt-{digest}"
    return str(eid)


def _normalize_events(doc: Any) -> list[dict[str, Any]]:
    if isinstance(doc, list):
        raw = doc
    elif isinstance(doc, dict) and isinstance(doc.get("events"), list):
        raw = doc["events"]
    elif isinstance(doc, dict):
        raw = [doc]
    else:
        raise HTTPException(status_code=400,
                            detail="Body must be a JSON object, an array, or {\"events\": [...]}")
    if not raw:
        raise HTTPException(status_code=400, detail="No events supplied")
    for item in raw:
        if not isinstance(item, dict):
            raise HTTPException(status_code=400, detail="Each event must be a JSON object")
    return raw


@app.post("/api/events/import")
async def import_events(request: Request) -> dict[str, Any]:
    try:
        doc = json.loads(await request.body())
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {exc}")

    created_ids: list[str] = []
    updated_ids: list[str] = []
    for obj in _normalize_events(doc):
        eid = _extract_event_id(obj)
        _, created = db.upsert_event(eid, obj)
        (created_ids if created else updated_ids).append(eid)
    return {"imported": len(created_ids) + len(updated_ids),
            "created": created_ids, "updated": updated_ids}


@app.get("/api/events")
def get_events() -> dict[str, Any]:
    return {"events": db.list_events()}


@app.get("/api/events/{event_id}")
def get_event_detail(event_id: str) -> dict[str, Any]:
    event = db.get_event(event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="event not found")
    event["payload_parsed"] = json.loads(event["payload"])
    event["timeline"] = db.timeline_for_event(event_id)
    return event


@app.delete("/api/events")
def delete_events() -> dict[str, Any]:
    db.clear_history()
    return {"ok": True}


# ---------- replay ----------

@app.post("/api/replay")
async def replay_all(request: Request) -> dict[str, Any]:
    body = ReplayIn()
    raw = await request.body()
    if raw.strip():
        try:
            body = ReplayIn(**json.loads(raw))
        except (ValueError, json.JSONDecodeError) as exc:
            raise HTTPException(status_code=422, detail=str(exc))
    only_failed = body.only_failed
    if only_failed:
        candidates = db.events_by_status(("failed",))
    else:
        candidates = [e for e in db.list_events()
                      if e["status"] in ("pending", "failed", "success")]
    queued = dispatcher.enqueue_many([e["event_id"] for e in candidates])
    return {"queued": queued, "skipped": len(candidates) - len(queued)}


@app.post("/api/events/{event_id}/replay")
def replay_one(event_id: str) -> dict[str, Any]:
    event = db.get_event(event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="event not found")
    if event["status"] in ("queued", "running"):
        raise HTTPException(status_code=409, detail=f"event is {event['status']}")
    ok = dispatcher.enqueue(event_id)
    return {"queued": ok, "event_id": event_id}


# ---------- timeline / status ----------

@app.get("/api/timeline")
def get_timeline(limit: int = 200) -> dict[str, Any]:
    return {"attempts": db.timeline(limit)}


@app.get("/api/status")
def get_status() -> dict[str, Any]:
    events = db.list_events()
    counts = {"pending": 0, "queued": 0, "running": 0, "success": 0, "failed": 0}
    for e in events:
        counts[e["status"]] = counts.get(e["status"], 0) + 1
    return {"queue_depth": dispatcher.queue_depth(),
            "inflight": dispatcher.inflight(),
            "total": len(events),
            "counts": counts}


# ---------- snapshot backup / restore ----------

@app.get("/api/backup")
def backup_snapshot():
    """Download a JSONL-in-zip snapshot of settings/events/attempts."""
    data, name, _manifest = snapshot.create_backup()
    return Response(
        content=data,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{name}"'},
    )


@app.post("/api/restore")
async def restore_snapshot(request: Request) -> dict[str, Any]:
    """Validate the uploaded snapshot, then atomically replace events/attempts.

    Current settings are always preserved. Any validation error rejects the
    whole upload before a single row is written.
    """
    raw = await request.body()
    try:
        parsed = snapshot.parse_snapshot(raw)
    except snapshot.SnapshotError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    if dispatcher.queue_depth() or dispatcher.inflight():
        raise HTTPException(
            status_code=409,
            detail="仍有事件在队列中或正在投递，请等待投递完成后再恢复（现有数据未改动）")

    events = parsed["events"]
    attempts = parsed["attempts"]

    # dispatch work recorded in the snapshot cannot exist anymore; treat it
    # like a restart would: move queued/running events back to pending
    normalized = 0
    for e in events:
        if e["status"] in snapshot.TRANSIENT_STATUSES:
            e["status"] = "pending"
            normalized += 1

    before = len(db.list_events())
    try:
        db.restore_rows(parsed["settings"], events, attempts)
    except Exception as exc:  # defensive: transaction rolled back either way
        raise HTTPException(status_code=422, detail=f"恢复事务失败已回滚：{exc}")

    current = db.get_settings()
    archived = parsed["settings"][0]
    return {
        "ok": True,
        "restored_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "db_version": parsed["manifest"].get("db_version"),
        "snapshot_created_at": parsed["manifest"].get("created_at"),
        "counts": {
            "events_before": before,
            "events_after": len(events),
            "attempts_after": len(attempts),
            "statuses_normalized_to_pending": normalized,
        },
        "settings": "preserved_current",
        "current_settings": {
            "target_url": current["target_url"],
            "hmac_secret": current["hmac_secret"],
            "max_retries": current["max_retries"],
            "timeout_seconds": current["timeout_seconds"],
        },
        "snapshot_settings_note":
            "快照中的设置仅存档未应用："
            f"target_url={archived.get('target_url')!r}, "
            f"max_retries={archived.get('max_retries')}, "
            f"timeout_seconds={archived.get('timeout_seconds')}",
    }


# ---------- static UI (mounted last so /api and /recv win) ----------

@app.get("/", include_in_schema=False)
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
