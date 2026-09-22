"""Built-in receiver so the lab works out of the box.

POST /recv                -> log request, honour ?fail=NNN&times=N&sleep=S
GET  /recv/logs           -> inspect the last received requests
DELETE /recv/logs         -> clear the log
"""
from __future__ import annotations

import asyncio
import json
import threading
from collections import deque
from datetime import datetime, timezone

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse

router = APIRouter(tags=["receiver"])

_log: deque[dict] = deque(maxlen=200)
_lock = threading.Lock()
# counters keyed by idempotency key: simulate transient 5xx for the first N calls
_fail_counts: dict[str, int] = {}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


@router.post("/recv")
async def receive(
    request: Request,
    fail: int = Query(default=0, description="HTTP status to return for the first `times` calls"),
    times: int = Query(default=1, ge=0, le=10),
    sleep: float = Query(default=0, ge=0, le=30, description="artificial delay in seconds"),
):
    raw = await request.body()
    idem = request.headers.get("Idempotency-Key", "")

    await asyncio.sleep(sleep)

    status = 200
    if fail:
        with _lock:
            seen = _fail_counts.get(idem, 0)
            if seen < times:
                _fail_counts[idem] = seen + 1
                status = fail

    try:
        parsed: object = json.loads(raw)
    except Exception:
        parsed = raw.decode("utf-8", "replace")

    entry = {
        "received_at": _now(),
        "status_returned": status,
        "idempotency_key": idem,
        "signature": request.headers.get("X-Webhook-Signature", ""),
        "event": request.headers.get("X-Webhook-Event", ""),
        "attempt": request.headers.get("X-Replay-Attempt", ""),
        "remote": request.client.host if request.client else "",
        "payload": parsed,
    }
    with _lock:
        _log.append(entry)

    if status >= 400:
        return JSONResponse(
            status_code=status,
            content={"error": f"injected failure {status}", "entry": entry},
        )
    return {"ok": True, "received": entry["event"], "note": "logged by built-in receiver"}


@router.get("/recv/logs")
def get_logs(limit: int = 50):
    with _lock:
        items = list(_log)[-limit:]
    return {"count": len(items), "logs": items}


@router.delete("/recv/logs")
def clear_logs():
    with _lock:
        _log.clear()
        _fail_counts.clear()
    return {"ok": True}
