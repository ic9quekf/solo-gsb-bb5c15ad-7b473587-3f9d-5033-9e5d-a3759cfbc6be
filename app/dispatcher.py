"""Background worker: fan out events to targets in FIFO order.

One event is delivered to every configured target *independently*: each
target request is signed with that target's HMAC secret, gets its own
idempotency key, retries with its own exponential backoff and records its own
attempts / delivery row. Target deliveries run concurrently; the event is
``success`` only when *all* targets succeed. A partial failure can be retried
with ``scope="failed"``, which re-sends only targets whose delivery is not
yet successful (per-target idempotency keys stay unique per replay round).

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
from datetime import datetime, timezone
from typing import Any

import httpx

from . import db
from .signing import canonical_body, idempotency_key, sign

# Terminal outcomes for one HTTP attempt
DELIVERED = "delivered"          # 2xx
RETRIABLE = "retriable"          # timeout / network error / 408 / 429 / 5xx
NON_RETRIABLE = "non_retriable"  # any other status (e.g. 4xx)

# Per-target delivery states
D_PENDING = "pending"
D_QUEUED = "queued"
D_RUNNING = "running"
D_SUCCESS = "success"
D_FAILED = "failed"

SCOPE_ALL = "all"
SCOPE_FAILED = "failed"


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
        self._inflight: dict[str, str] = {}  # event_id -> scope
        self._lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._q: asyncio.Queue[tuple[str, str]] | None = None
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

    async def _aenqueue(self, event_id: str, scope: str) -> bool:
        while self._q is None:
            await asyncio.sleep(0.01)
        with self._lock:
            if event_id in self._inflight:
                return False
            # Nothing to do for a failed-scope replay whose targets are all
            # already successful: don't move the event into a queued state.
            if scope == SCOPE_FAILED and db.list_targets() \
                    and not self._pending_targets(event_id, db.list_targets()):
                return False
            self._inflight[event_id] = scope
        db.set_event_status(event_id, "queued")
        await self._q.put((event_id, scope))
        return True

    def enqueue(self, event_id: str, scope: str = SCOPE_ALL) -> bool:
        if self._loop is None:
            raise RuntimeError("dispatcher not started")
        fut = asyncio.run_coroutine_threadsafe(
            self._aenqueue(event_id, scope), self._loop)
        return fut.result()

    def enqueue_many(self, items: list[tuple[str, str]]) -> list[str]:
        return [eid for eid, scope in items if self.enqueue(eid, scope)]

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
                event_id, scope = await self._q.get()
                try:
                    await self._process(client, event_id, scope)
                except Exception as exc:  # never let the worker die
                    db.set_event_status(event_id, "failed")
                    print(f"[dispatcher] event {event_id} crashed: {exc!r}",
                          flush=True)
                finally:
                    self._q.task_done()
                    with self._lock:
                        self._inflight.pop(event_id, None)
        finally:
            await client.aclose()

    # ---------- one event = fan out to every applicable target ----------

    async def _process(self, client: httpx.AsyncClient, event_id: str,
                       scope: str) -> None:
        event = db.get_event(event_id)
        if event is None:
            return
        settings = db.get_settings()
        targets = db.effective_targets(settings)
        named_mode = any(t["id"] is not None for t in targets)

        if named_mode:
            targets = self._filter_scope(event_id, targets, scope)
            if not targets:
                # nothing left to retry: leave the existing status untouched
                return
        db.set_event_status(event_id, "running")

        payload = json.loads(event["payload"])
        body = canonical_body(payload)

        with db.get_conn() as conn:
            replay_no = db.latest_replay_no(conn, event_id) + 1

        results = await asyncio.gather(
            *(self._safe_deliver(client, event_id, event, body,
                                 replay_no, target, named_mode)
              for target in targets),
            return_exceptions=True,
        )
        delivered = [r for r in results if r is True]

        if not named_mode:
            # legacy single pseudo-target: result[0] carries the outcome
            db.set_event_status(
                event_id, "success" if delivered else "failed")
            return

        if len(delivered) == len(targets):
            db.set_event_status(event_id, "success")
        else:
            # partial or total failure: per-target states stay in deliveries
            db.set_event_status(event_id, "failed")

    @staticmethod
    def _pending_targets(event_id: str, targets: list[dict[str, Any]]
                         ) -> list[dict[str, Any]]:
        """Targets that a failed-scope replay for this event should (re)send to.

        Everything not already delivered -- including a target added after the
        event succeeded, which has no delivery row yet -- is included.
        """
        states = {d["target_id"]: d["status"]
                  for d in db.deliveries_for_event(event_id)}
        return [t for t in targets if states.get(t["id"]) != D_SUCCESS]

    @staticmethod
    def _filter_scope(event_id: str, targets: list[dict[str, Any]],
                      scope: str) -> list[dict[str, Any]]:
        if scope != SCOPE_FAILED:
            return targets
        return Dispatcher._pending_targets(event_id, targets)

    async def _safe_deliver(self, client, event_id, event, body,
                            replay_no, target, named) -> bool:
        """Wrapper that never propagates: a crashed target delivery is a
        failure for that target, not for the whole fanout/worker."""
        try:
            return await self._deliver_to_target(
                client, event_id, event, body, replay_no, target, named)
        except Exception as exc:
            print(f"[dispatcher] event {event_id} target {target.get('name')}"
                  f" crashed: {exc!r}", flush=True)
            if named:
                db.update_delivery_attempt(
                    event_id, target["id"], status=D_FAILED,
                    attempts_count=0, http_status=None,
                    outcome=RETRIABLE, attempt_at=_ts())
            return False

    async def _deliver_to_target(self, client: httpx.AsyncClient,
                                 event_id: str, event: dict[str, Any],
                                 body: bytes, replay_no: int,
                                 target: dict[str, Any],
                                 named: bool) -> bool:
        """Deliver one event to one target with its own retry/backoff loop."""
        target_id = target["id"]
        target_name = target["name"] if named else None

        if named:
            db.upsert_delivery(event_id, target_id, D_QUEUED, replay_no)
            db.upsert_delivery(event_id, target_id, D_RUNNING, replay_no)

        max_retries = int(target["max_retries"])
        timeout = float(target["timeout_seconds"])
        max_attempts = max_retries + 1

        ok = False
        for attempt_no in range(1, max_attempts + 1):
            idem = idempotency_key(event_id, replay_no, target_name)
            sig_header, ts = sign(body, target["hmac_secret"])
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
            if named:
                headers["X-Webhook-Target"] = target["name"]

            started = time.monotonic()
            request_at = _ts()
            result = await self._send_once(
                client, target["url"], body, headers, timeout)
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
                target_id=target_id,
            )
            if named:
                state = (D_SUCCESS if result["outcome"] == DELIVERED
                         else D_FAILED)
                db.update_delivery_attempt(
                    event_id, target_id,
                    status=state,
                    attempts_count=attempt_no,
                    http_status=result.get("status"),
                    outcome=result["outcome"],
                    attempt_at=response_at,
                )

            if result["outcome"] == DELIVERED:
                ok = True
                break

            if result["outcome"] == NON_RETRIABLE or attempt_no >= max_attempts:
                break

            # exponential backoff: 0.5s, 1s, 2s ... capped at 30s, 50-100% jitter
            delay = min(30.0, (2 ** (attempt_no - 1)) * 0.5)
            delay *= 0.5 + random.random() * 0.5
            await asyncio.sleep(delay)

        return ok

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
