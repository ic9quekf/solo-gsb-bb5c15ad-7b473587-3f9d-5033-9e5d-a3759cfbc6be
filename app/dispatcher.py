"""Background worker: sends events in FIFO order with exponential backoff retries.

The worker runs an asyncio loop on a dedicated daemon thread. The HTTP client
is an ``httpx.AsyncClient``; the enqueue API is thread-safe (called from
FastAPI's sync endpoints) via ``run_coroutine_threadsafe``.
"""
from __future__ import annotations

import asyncio
import json
import random
import threading
import time
from datetime import timezone
from datetime import datetime
from typing import Any

import httpx

from . import db
from .signing import canonical_body, idempotency_key, sign

# Terminal outcomes for one HTTP attempt
DELIVERED = "delivered"          # 2xx
RETRIABLE = "retriable"          # timeout / network error / 408 / 429 / 5xx
NON_RETRIABLE = "non_retriable"  # any other status (e.g. 4xx)


def _ts() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def excerpt(response: httpx.Response) -> str:
    try:
        return response.text[:1000]
    except Exception:
        return ""


def classify(status_code: int) -> str:
    if 200 <= status_code < 300:
        return DELIVERED
    if status_code >= 500 or status_code in (408, 429):
        return RETRIABLE
    return NON_RETRIABLE


class Dispatcher:
    def __init__(self) -> None:
        self._inflight: set[str] = set()
        self._lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._q: asyncio.Queue[str] | None = None
        self._client_factory: Any = None  # async httpx client factory (test hook)

    # ---------- lifecycle ----------

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._thread_main, daemon=True)
        self._thread.start()
        # wait until the loop is ready so enqueue() can be called immediately
        while self._loop is None:
            time.sleep(0.005)

    async def astart(self) -> None:
        """Hook for an in-loop lifespan (unused in the thread setup)."""
        self.start()

    def stop(self) -> None:
        if self._loop and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)

    def _thread_main(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        try:
            loop.run_until_complete(self._run())
        except asyncio.CancelledError:
            pass
        finally:
            loop.close()

    # ---------- enqueue (safe to call from other threads) ----------

    async def _aenqueue(self, event_id: str) -> bool:
        while self._q is None:
            await asyncio.sleep(0.01)
        with self._lock:
            if event_id in self._inflight:
                return False
            self._inflight.add(event_id)
        db.set_event_status(event_id, "queued")
        await self._q.put(event_id)
        return True

    def enqueue(self, event_id: str) -> bool:
        if self._loop is None:
            raise RuntimeError("dispatcher not started")
        fut = asyncio.run_coroutine_threadsafe(self._aenqueue(event_id), self._loop)
        return fut.result()

    def enqueue_many(self, event_ids: list[str]) -> list[str]:
        return [eid for eid in event_ids if self.enqueue(eid)]

    def queue_depth(self) -> int:
        return self._q.qsize() if self._q else 0

    def inflight(self) -> list[str]:
        with self._lock:
            return sorted(self._inflight)

    # ---------- worker ----------

    async def _run(self) -> None:
        self._q = asyncio.Queue()
        if self._client_factory is not None:
            client = self._client_factory()
        else:
            client = httpx.AsyncClient()
        try:
            while True:
                event_id = await self._q.get()
                try:
                    await self._process(client, event_id)
                except Exception as exc:  # never let the worker die
                    db.set_event_status(event_id, "failed")
                    print(f"[dispatcher] event {event_id} crashed: {exc!r}", flush=True)
                finally:
                    self._q.task_done()
                    with self._lock:
                        self._inflight.discard(event_id)
        finally:
            await client.aclose()

    async def _process(self, client: httpx.AsyncClient, event_id: str) -> None:
        event = db.get_event(event_id)
        if event is None:
            return
        settings = db.get_settings()
        db.set_event_status(event_id, "running")

        payload = json.loads(event["payload"])
        body = canonical_body(payload)

        with db.get_conn() as conn:
            replay_no = db.latest_replay_no(conn, event_id) + 1

        max_retries = int(settings["max_retries"])
        timeout = float(settings["timeout_seconds"])
        max_attempts = max_retries + 1

        for attempt_no in range(1, max_attempts + 1):
            idem = idempotency_key(event_id, replay_no)
            sig_header, ts = sign(body, settings["hmac_secret"])
            headers = {
                "Content-Type": "application/json",
                "X-Webhook-Event": event_id,
                "X-Webhook-Timestamp": str(ts),
                "X-Webhook-Signature": sig_header,
                "Idempotency-Key": idem,
                "X-Idempotency-Key": idem,
                "X-Replay-Attempt": str(attempt_no),
                "X-Replay-Number": str(replay_no),
            }
            started = time.monotonic()
            request_at = _ts()
            result = await self._send_once(
                client, settings["target_url"], body, headers, timeout)
            elapsed_ms = int((time.monotonic() - started) * 1000)
            response_at = _ts()

            db.record_attempt(
                event_id,
                attempt_no=attempt_no,
                replay_no=replay_no,
                idem_key=idem,
                request_at=request_at,
                req_headers=headers,
                response_at=response_at,
                http_status=result.get("status"),
                outcome=result["outcome"],
                elapsed_ms=elapsed_ms,
                resp_excerpt=result["excerpt"],
            )

            if result["outcome"] == DELIVERED:
                db.set_event_status(event_id, "success")
                return

            if result["outcome"] == NON_RETRIABLE or attempt_no >= max_attempts:
                db.set_event_status(event_id, "failed")
                return

            # exponential backoff: 0.5s, 1s, 2s ... capped at 30s, 50-100% jitter
            delay = min(30.0, (2 ** (attempt_no - 1)) * 0.5)
            delay *= 0.5 + random.random() * 0.5
            await asyncio.sleep(delay)

    async def _send_once(self, client: httpx.AsyncClient, url: str,
                         body: bytes, headers: dict[str, str],
                         timeout: float) -> dict[str, Any]:
        try:
            resp = await client.post(url, content=body, headers=headers, timeout=timeout)
        except httpx.TimeoutException as exc:
            return {"status": None, "outcome": RETRIABLE,
                    "excerpt": f"Timeout after {timeout}s: {exc!r}"}
        except httpx.TransportError as exc:
            return {"status": None, "outcome": RETRIABLE,
                    "excerpt": f"Network error: {exc!r}"}
        return {"status": resp.status_code,
                "outcome": classify(resp.status_code),
                "excerpt": excerpt(resp)}


dispatcher = Dispatcher()
