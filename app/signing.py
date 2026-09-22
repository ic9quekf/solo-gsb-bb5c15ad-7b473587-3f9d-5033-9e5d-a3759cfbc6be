"""HMAC signature and idempotency-key helpers."""
from __future__ import annotations

import hashlib
import hmac
import json
import time


def canonical_body(payload: dict) -> bytes:
    # Stable serialization so signature verification is reproducible.
    return json.dumps(payload, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":")).encode("utf-8")


def sign(body: bytes, secret: str, timestamp: int | None = None) -> tuple[str, int]:
    """Stripe-style scheme: t=<ts>,v1=<hex hmac sha256 of 'ts.body'>."""
    ts = timestamp if timestamp is not None else int(time.time())
    msg = f"{ts}.".encode("ascii") + body
    digest = hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).hexdigest()
    return f"t={ts},v1={digest}", ts


def verify(body: bytes, secret: str, header: str, tolerance: int = 300) -> bool:
    try:
        parts = dict(p.split("=", 1) for p in header.split(","))
        ts = int(parts["t"])
        sig = parts["v1"]
    except Exception:
        return False
    if abs(time.time() - ts) > tolerance:
        return False
    expected, _ = sign(body, secret, ts)
    return hmac.compare_digest(expected.split("v1=", 1)[1], sig)


def idempotency_key(event_id: str, replay_no: int) -> str:
    raw = f"{event_id}#replay={replay_no}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
