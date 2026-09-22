"""End-to-end smoke test using FastAPI TestClient."""
import asyncio
import hashlib
import io
import json
import os
import tempfile
import time
import zipfile

_tmp = tempfile.mkdtemp()
os.environ["REPLAY_DB"] = os.path.join(_tmp, "test.db")

from fastapi.testclient import TestClient  # noqa: E402

from app.dispatcher import dispatcher  # noqa: E402
from app.main import app  # noqa: E402


def wait_until(predicate, timeout=20, label="condition"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.2)
    raise AssertionError(f"timeout waiting for {label}")


def _build_v1_snapshot(v2_blob: bytes) -> bytes:
    """Downgrade a current v2 snapshot zip into a legacy v1 archive.

    Drops targets.jsonl/deliveries.jsonl, removes target_id from attempts and
    rewrites the manifest with format_version/db_version = 1.
    """
    zin = zipfile.ZipFile(io.BytesIO(v2_blob))
    v1_fields = {
        "settings.jsonl": ["id", "target_url", "hmac_secret", "max_retries",
                           "timeout_seconds"],
        "events.jsonl": ["event_id", "seq", "payload", "status",
                         "created_at", "updated_at"],
        "attempts.jsonl": ["id", "event_id", "attempt_no", "replay_no",
                           "idem_key", "request_at", "response_at",
                           "http_status", "outcome", "elapsed_ms",
                           "req_headers", "resp_excerpt"],
    }
    payload = {}
    for fname, fields in v1_fields.items():
        rows = [json.loads(line) for line in zin.read(fname).decode().splitlines()
                if line]
        buf = io.BytesIO()
        for row in rows:
            buf.write((json.dumps({f: row.get(f) for f in fields},
                                  ensure_ascii=False, separators=(",", ":"))
                       + "\n").encode())
        payload[fname] = buf.getvalue()
    files = {}
    for name, data in payload.items():
        files[name] = {
            "name": name, "lines": data.count(b"\n"), "bytes": len(data),
            "sha256": hashlib.sha256(data).hexdigest()}
    manifest = {
        "format": "webhook-replay-lab-snapshot", "format_version": 1,
        "db_version": 1, "created_at": "2026-09-22T00:00:00+00:00",
        "files": files,
    }
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zout:
        zout.writestr("manifest.json", json.dumps(manifest, indent=2))
        for name, data in payload.items():
            zout.writestr(name, data)
    zin.close()
    return out.getvalue()


def run(client: TestClient) -> None:
    assert client.get("/healthz").json() == {"status": "ok"}

    # receiver: first 2 calls per idem key fail with 500
    settings = {
        "target_url": "http://testserver/recv?fail=500&times=2",
        "hmac_secret": "topsecret",
        "max_retries": 3,
        "timeout_seconds": 5.0,
    }
    r = client.post("/api/settings", json=settings)
    assert r.status_code == 200, r.text
    assert r.json()["target_url"].endswith("times=2")

    # import: array form with event_id / id / auto-generated id
    events = [
        {"event_id": "evt-001", "type": "a"},
        {"event_id": "evt-002", "type": "b"},
        {"id": "evt-003", "type": "c"},
        {"type": "no-id", "x": 1},
    ]
    r = client.post("/api/events/import", json=events)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["imported"] == 4, body
    assert len(body["created"]) == 4

    # re-import same id -> update, not create
    r = client.post("/api/events/import", json=[{"event_id": "evt-001", "type": "a2"}])
    assert r.json()["created"] == [] and r.json()["updated"] == ["evt-001"]

    # invalid JSON
    r = client.post("/api/events/import", content=b"{not json",
                    headers={"Content-Type": "application/json"})
    assert r.status_code == 400

    # replay all
    r = client.post("/api/replay", json={})
    assert r.status_code == 200
    queued = r.json()["queued"]
    assert len(queued) == 4, r.text

    wait_until(
        lambda: client.get("/api/status").json()["counts"]["success"] == 4,
        timeout=60, label="all 4 delivered")
    st = client.get("/api/status").json()
    assert st["counts"]["failed"] == 0, st
    print("status after replay:", st)

    # timeline: each event had 500,500,200 with a shared idem key
    detail = client.get("/api/events/evt-001").json()
    attempts = detail["timeline"]
    assert len(attempts) == 3, [(a["outcome"], a["http_status"]) for a in attempts]
    assert [a["http_status"] for a in attempts] == [500, 500, 200]
    assert [a["outcome"] for a in attempts] == ["retriable", "retriable", "delivered"]
    keys = {a["idem_key"] for a in attempts}
    assert len(keys) == 1, "retries within one replay must share idempotency key"
    hdr = json.loads(attempts[0]["req_headers"])
    assert hdr["X-Webhook-Event"] == "evt-001"
    assert hdr["Idempotency-Key"] == list(keys)[0]
    assert "t=" in hdr["X-Webhook-Signature"] and "v1=" in hdr["X-Webhook-Signature"]
    assert detail["payload_parsed"]["type"] == "a2"
    print("evt-001 attempts:", [(a["http_status"], a["elapsed_ms"]) for a in attempts])

    # signature verifies against the canonical body
    from app.signing import canonical_body, verify
    raw = canonical_body(detail["payload_parsed"])
    assert verify(raw, "topsecret", hdr["X-Webhook-Signature"])
    assert not verify(raw, "wrong", hdr["X-Webhook-Signature"])

    # receiver logged one 200 per unique event
    logs = client.get("/recv/logs?limit=200").json()["logs"]
    delivered = [l for l in logs if l["status_returned"] == 200]
    assert {l["event"] for l in delivered} == set(queued), [l["event"] for l in delivered]
    print("receiver log entries:", len(logs), "200s:", len(delivered))

    # 4xx -> non_retriable, fail immediately without retry
    client.post("/api/settings", json={**settings,
                "target_url": "http://testserver/recv?fail=422&times=9"})
    client.post("/api/events/import", json=[{"event_id": "evt-bad", "type": "x"}])
    client.post("/api/events/evt-bad/replay")
    wait_until(lambda: client.get("/api/events/evt-bad").json()["status"] == "failed",
               timeout=10, label="evt-bad failed")
    d = client.get("/api/events/evt-bad").json()
    assert len(d["timeline"]) == 1 and d["timeline"][0]["http_status"] == 422
    assert d["timeline"][0]["outcome"] == "non_retriable"

    # manual replay against healthy target -> success with a NEW idem key
    client.post("/api/settings", json={**settings, "target_url": "http://testserver/recv"})
    client.post("/api/events/evt-bad/replay")
    wait_until(lambda: client.get("/api/events/evt-bad").json()["status"] == "success",
               timeout=10, label="evt-bad replay success")
    d = client.get("/api/events/evt-bad").json()
    assert len(d["timeline"]) == 2
    k1, k2 = d["timeline"][0]["idem_key"], d["timeline"][1]["idem_key"]
    assert k1 != k2, "manual replay must use a fresh idempotency key"

    # timeout -> retriable, recorded without an http status
    client.post("/api/settings", json={**settings,
                "target_url": "http://testserver/recv?sleep=2",
                "timeout_seconds": 0.3, "max_retries": 0})
    client.post("/api/events/import", json=[{"event_id": "evt-slow", "type": "x"}])
    t0 = time.time()
    client.post("/api/events/evt-slow/replay")
    wait_until(lambda: client.get("/api/events/evt-slow").json()["status"] == "failed",
               timeout=15, label="evt-slow failed")
    a = client.get("/api/events/evt-slow").json()["timeline"][0]
    assert a["http_status"] is None and a["outcome"] == "retriable"
    assert "Timeout" in a["resp_excerpt"]
    print("timeout path ok in %.1fs, excerpt:" % (time.time() - t0),
          a["resp_excerpt"][:60])

    # detail for missing event
    assert client.get("/api/events/nope").status_code == 404

    # ---------- multi-target fanout ----------

    def fanout_targets(*specs):
        return {"targets": [
            {"name": n, "url": u, "hmac_secret": sec,
             "max_retries": mr, "timeout_seconds": to}
            for (n, u, sec, mr, to) in specs]}

    # config validation: 2-5 targets, unique names, http(s) URLs
    r = client.post("/api/targets", json=fanout_targets(
        ("only-one", "http://testserver/recv", "s1", 1, 5.0)))
    assert r.status_code == 422, r.text
    r = client.post("/api/targets", json=fanout_targets(
        ("dup", "http://testserver/recv", "s1", 1, 5.0),
        ("dup", "http://testserver/recv", "s2", 1, 5.0)))
    assert r.status_code == 422 and "唯一" in r.text, r.text
    r = client.post("/api/targets", json=fanout_targets(
        ("a", "ftp://nope", "s1", 1, 5.0),
        ("b", "http://testserver/recv", "s2", 1, 5.0)))
    assert r.status_code == 422 and "http" in r.text, r.text

    # three targets: alpha fails 2x then ok, beta/gamma healthy; each own secret
    cfg = fanout_targets(
        ("alpha", "http://testserver/recv?fail=500&times=2", "secret-alpha", 3, 5.0),
        ("beta", "http://testserver/recv", "secret-beta", 3, 5.0),
        ("gamma", "http://testserver/recv", "secret-gamma", 0, 5.0))
    r = client.post("/api/targets", json=cfg)
    assert r.status_code == 200, r.text
    assert r.json()["mode"] == "fanout" and r.json()["count"] == 3
    got = client.get("/api/targets").json()
    assert [t["name"] for t in got["targets"]] == ["alpha", "beta", "gamma"]
    # legacy single-target settings are untouched by target config
    assert client.get("/api/settings").json()["target_url"].endswith("sleep=2")

    client.post("/api/events/import", json=[
        {"event_id": "fan-1", "type": "fanout.a"},
        {"event_id": "fan-2", "type": "fanout.b"},
    ])
    r = client.post("/api/replay", json={})
    assert r.status_code == 200 and r.json()["scope"] == "all"
    wait_until(
        lambda: client.get("/api/status").json()["counts"]["success"]
        == client.get("/api/status").json()["total"],
        timeout=90, label="all events delivered to every target")
    st = client.get("/api/status").json()
    assert st["mode"] == "fanout" and st["targets"] == 3
    assert st["delivery_counts"]["success"] == 3 * st["total"], st

    d = client.get("/api/events/fan-1").json()
    by_name = {x["target_name"]: x for x in d["deliveries"]}
    assert set(by_name) == {"alpha", "beta", "gamma"}
    assert all(x["status"] == "success" for x in by_name.values())
    # alpha needed 3 attempts (500,500,200); beta/gamma one each
    assert by_name["alpha"]["attempts_count"] == 3
    assert by_name["beta"]["attempts_count"] == by_name["gamma"]["attempts_count"] == 1

    # per-target independent signatures + distinct idempotency keys per target
    from app.signing import canonical_body as cb, verify as vfy
    per_target_headers: dict[str, dict] = {}
    for a in d["timeline"]:
        if a["http_status"] == 200:
            per_target_headers.setdefault(a["target_name"], json.loads(a["req_headers"]))
    assert set(per_target_headers) == {"alpha", "beta", "gamma"}
    secrets = {"alpha": "secret-alpha", "beta": "secret-beta", "gamma": "secret-gamma"}
    raw = cb(d["payload_parsed"])
    idem_keys = set()
    for name, hdr in per_target_headers.items():
        assert hdr["X-Webhook-Target"] == name
        assert vfy(raw, secrets[name], hdr["X-Webhook-Signature"]), name
        assert not vfy(raw, "wrong-secret", hdr["X-Webhook-Signature"])
        idem_keys.add(hdr["Idempotency-Key"])
    assert len(idem_keys) == 3, "each target gets its own idempotency key"

    # each successful 2xx was logged by the receiver with the target header
    logs = client.get("/recv/logs?limit=500").json()["logs"]
    fan1_ok = [l for l in logs if l["event"] == "fan-1" and l["status_returned"] == 200]
    assert {l["target"] for l in fan1_ok} == {"alpha", "beta", "gamma"}

    # partial failure: one target is unconditionally 422 (non-retriable).
    # existing targets keep their ids (matched by name); gamma is removed ->
    # its historical attempts/deliveries cascade away by design.
    client.post("/api/targets", json=fanout_targets(
        ("alpha", "http://testserver/recv", "secret-alpha", 0, 5.0),
        ("beta", "http://testserver/recv", "secret-beta", 0, 5.0),
        ("broken", "http://testserver/recv?fail=422&times=9", "secret-broken", 0, 5.0)))
    client.post("/api/events/import", json=[{"event_id": "fan-3", "type": "fanout.c"}])
    client.post("/api/events/fan-3/replay", json={"scope": "all"})
    wait_until(lambda: client.get("/api/events/fan-3").json()["status"] == "failed",
               timeout=15, label="fan-3 partial failure")
    d = client.get("/api/events/fan-3").json()
    by_name = {x["target_name"]: x for x in d["deliveries"]}
    assert by_name["alpha"]["status"] == by_name["beta"]["status"] == "success"
    assert by_name["broken"]["status"] == "failed"
    assert by_name["broken"]["last_http_status"] == 422
    broken_attempts = [a for a in d["timeline"] if a["target_name"] == "broken"]
    assert len(broken_attempts) == 1 and broken_attempts[0]["outcome"] == "non_retriable"
    # the good targets are not touched again: still one attempt each
    for name in ("alpha", "beta"):
        ats = [a for a in d["timeline"] if a["target_name"] == name]
        assert len(ats) == 1 and ats[0]["http_status"] == 200

    # retry ONLY the failed target -> it is now healthy, others are skipped
    client.post("/api/targets", json=fanout_targets(
        ("alpha", "http://testserver/recv", "secret-alpha", 0, 5.0),
        ("beta", "http://testserver/recv", "secret-beta", 0, 5.0),
        ("broken", "http://testserver/recv", "secret-broken", 0, 5.0)))
    r = client.post("/api/events/fan-3/replay", json={"scope": "failed"})
    assert r.status_code == 200 and r.json()["scope"] == "failed"
    wait_until(lambda: client.get("/api/events/fan-3").json()["status"] == "success",
               timeout=15, label="fan-3 healed via failed-only retry")
    d = client.get("/api/events/fan-3").json()
    by_name = {x["target_name"]: x for x in d["deliveries"]}
    assert all(x["status"] == "success" for x in by_name.values())
    # alpha/beta attempts counts stay at 1; broken gained a fresh round
    assert by_name["alpha"]["attempts_count"] == by_name["beta"]["attempts_count"] == 1
    assert by_name["broken"]["attempts_count"] == 1  # reset counters, one 200
    broken_keys = [a["idem_key"] for a in d["timeline"] if a["target_name"] == "broken"]
    assert len(set(broken_keys)) == 2, "new replay round must mint a new idem key"

    # a newly added target (and any target without a delivery row) is picked up
    # by a failed-scope replay; already-successful targets are skipped
    client.post("/api/targets", json=fanout_targets(
        ("alpha", "http://testserver/recv", "secret-alpha", 0, 5.0),
        ("beta", "http://testserver/recv", "secret-beta", 0, 5.0),
        ("broken", "http://testserver/recv", "secret-broken", 0, 5.0),
        ("newbie", "http://testserver/recv", "secret-new", 0, 5.0)))
    before_alpha = client.get("/api/events/fan-1").json()["deliveries"]
    assert {x["target_name"] for x in before_alpha} == {"alpha", "beta"}
    # fan-1 is already success, so the only_failed BATCH replay must not touch it
    r = client.post("/api/replay", json={"only_failed": True})
    assert r.status_code == 200 and r.json()["queued"] == [], r.text
    # ...and an explicit failed-scope replay is a no-op once every current
    # target already has a successful delivery (status stays untouched).
    # use a fresh event because fan-1..fan-3 predate the newbie target and
    # legitimately still owe it a delivery in failed scope
    client.post("/api/events/import", json=[{"event_id": "fan-allok", "type": "ok"}])
    client.post("/api/events/fan-allok/replay", json={"scope": "all"})
    wait_until(
        lambda: client.get("/api/events/fan-allok").json()["status"] == "success",
        timeout=15, label="fan-allok delivered to all 4 targets")
    r = client.post("/api/events/fan-allok/replay", json={"scope": "failed"})
    assert r.status_code == 200 and r.json()["queued"] is False, r.text
    assert client.get("/api/events/fan-allok").json()["status"] == "success"
    # explicit failed-scope replay sends to every target not yet successful
    r = client.post("/api/events/fan-1/replay", json={"scope": "failed"})
    assert r.status_code == 200
    wait_until(
        lambda: {x["target_name"]: x["status"] for x in
                 client.get("/api/events/fan-1").json()["deliveries"]}.get("newbie")
        == "success", timeout=15, label="fan-1 delivered to pending targets")
    d = client.get("/api/events/fan-1").json()
    by_name = {x["target_name"]: x for x in d["deliveries"]}
    assert set(by_name) == {"alpha", "beta", "broken", "newbie"}
    assert all(x["status"] == "success" for x in by_name.values())
    # alpha/beta counters untouched; broken+newbie got exactly one new attempt
    assert by_name["alpha"]["attempts_count"] == 3
    assert by_name["beta"]["attempts_count"] == 1
    assert by_name["broken"]["attempts_count"] == by_name["newbie"]["attempts_count"] == 1
    assert d["status"] == "success"

    # cannot edit targets while a delivery is in flight
    client.post("/api/targets", json=fanout_targets(
        ("slow", "http://testserver/recv?sleep=2", "s", 0, 5.0),
        ("fast", "http://testserver/recv", "f", 0, 5.0)))
    client.post("/api/events/import", json=[{"event_id": "fan-busy", "type": "z"}])
    client.post("/api/events/fan-busy/replay", json={"scope": "all"})
    time.sleep(0.3)
    r = client.post("/api/targets", json=fanout_targets(
        ("x", "http://testserver/recv", "s", 0, 5.0),
        ("y", "http://testserver/recv", "f", 0, 5.0)))
    assert r.status_code == 409, r.text
    assert client.delete("/api/targets").status_code == 409
    wait_until(lambda: client.get("/api/events/fan-busy").json()["status"] == "success",
               timeout=15, label="fan-busy delivered")

    # snapshots carry targets/deliveries; restore brings the fanout state back
    client.post("/api/targets", json=fanout_targets(
        ("alpha", "http://testserver/recv", "secret-alpha", 0, 5.0),
        ("beta", "http://testserver/recv", "secret-beta", 0, 5.0)))
    client.post("/api/events/import", json=[{"event_id": "fan-cap", "type": "cap"}])
    client.post("/api/events/fan-cap/replay", json={"scope": "all"})
    wait_until(lambda: client.get("/api/events/fan-cap").json()["status"] == "success",
               timeout=15, label="fan-cap delivered")
    fan_zip = client.get("/api/backup").content
    fz = zipfile.ZipFile(io.BytesIO(fan_zip))
    t_rows = [json.loads(x) for x in fz.read("targets.jsonl").decode().splitlines()]
    dl_rows = [json.loads(x) for x in fz.read("deliveries.jsonl").decode().splitlines()]
    assert {t["name"] for t in t_rows} == {"alpha", "beta"}
    cap_dl = [x for x in dl_rows if x["event_id"] == "fan-cap"]
    assert len(cap_dl) == 2 and all(x["status"] == "success" for x in cap_dl)
    fz.close()
    client.delete("/api/targets")
    assert client.get("/api/targets").json()["mode"] == "single"
    assert client.get("/api/deliveries").json()["deliveries"] == []
    assert client.get("/api/events/fan-cap").json()["timeline"] == []
    r = client.post("/api/restore", content=fan_zip,
                    headers={"Content-Type": "application/zip"})
    assert r.status_code == 200, r.text
    assert r.json()["mode"] == "fanout" and r.json()["counts"]["targets_after"] == 2
    assert {t["name"] for t in client.get("/api/targets").json()["targets"]} == {"alpha", "beta"}
    # fan-cap's per-target attempts survived restore with target linkage
    d = client.get("/api/events/fan-cap").json()
    assert {a["target_name"] for a in d["timeline"]} == {"alpha", "beta"}
    assert {x["target_name"] for x in d["deliveries"]} == {"alpha", "beta"}
    # legacy settings still preserved through a fanout restore
    assert client.get("/api/settings").json()["target_url"].endswith("sleep=2")

    # v1 snapshots (4 members, no target_id) are migrated on restore
    v1_blob = _build_v1_snapshot(fan_zip)
    client.delete("/api/targets")
    r = client.post("/api/restore", content=v1_blob,
                    headers={"Content-Type": "application/zip"})
    assert r.status_code == 200, r.text
    assert r.json()["mode"] == "single" and r.json()["format_version"] == 1
    assert client.get("/api/targets").json()["count"] == 0
    d = client.get("/api/events/fan-cap").json()
    assert d["status"] == "success" and d["deliveries"] == []
    assert all(a["target_name"] is None for a in d["timeline"])

    # back to single-target mode for the remaining legacy snapshot tests
    assert client.delete("/api/targets").status_code == 200

    # ---------- snapshot backup / restore ----------

    def repack(blob, rewrites=None, manifest_edit=None):
        """Rewrite members / manifest of a snapshot zip, recomputing meta."""
        rewrites = rewrites or {}
        zin = zipfile.ZipFile(io.BytesIO(blob))
        manifest = json.loads(zin.read("manifest.json").decode())
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
            for name in zin.namelist():
                if name == "manifest.json":
                    continue
                data = rewrites.get(name, zin.read(name))
                zout.writestr(name, data)
                if name in rewrites:
                    manifest["files"][name].update(
                        lines=data.count(b"\n"), bytes=len(data),
                        sha256=hashlib.sha256(data).hexdigest())
            if manifest_edit:
                manifest_edit(manifest)
            zout.writestr("manifest.json", json.dumps(manifest, indent=2))
        return buf.getvalue()

    n_events = len(client.get("/api/events").json()["events"])
    n_attempts_before = len(client.get("/api/timeline?limit=2000").json()["attempts"])

    r = client.get("/api/backup")
    assert r.status_code == 200, r.text
    assert r.headers["content-type"] == "application/zip"
    cd = r.headers["content-disposition"]
    assert "attachment" in cd and cd.endswith('.zip"'), cd
    good_zip = r.content
    zf = zipfile.ZipFile(io.BytesIO(good_zip))
    assert set(zf.namelist()) == {
        "manifest.json", "settings.jsonl", "targets.jsonl", "events.jsonl",
        "deliveries.jsonl", "attempts.jsonl"}
    manifest = json.loads(zf.read("manifest.json"))
    assert manifest["format"] == "webhook-replay-lab-snapshot"
    assert manifest["format_version"] == 2
    assert manifest["db_version"] >= 2
    for fname in ("settings.jsonl", "targets.jsonl", "events.jsonl",
                  "deliveries.jsonl", "attempts.jsonl"):
        data = zf.read(fname)
        meta = manifest["files"][fname]
        assert hashlib.sha256(data).hexdigest() == meta["sha256"]
        assert data.count(b"\n") == meta["lines"]
        assert len(data) == meta["bytes"]
        if not data:
            continue  # empty table (targets/deliveries in single-target mode)
        assert list(json.loads(data.split(b"\n")[0]).keys()) == {
            "settings.jsonl": ["id", "target_url", "hmac_secret", "max_retries",
                               "timeout_seconds"],
            "targets.jsonl": ["id", "name", "url", "hmac_secret", "max_retries",
                              "timeout_seconds", "created_at", "updated_at"],
            "events.jsonl": ["event_id", "seq", "payload", "status",
                             "created_at", "updated_at"],
            "deliveries.jsonl": ["event_id", "target_id", "status", "replay_no",
                                 "attempts_count", "last_http_status",
                                 "last_outcome", "last_attempt_at", "updated_at"],
            "attempts.jsonl": ["id", "event_id", "target_id", "attempt_no",
                               "replay_no", "idem_key", "request_at", "response_at",
                               "http_status", "outcome", "elapsed_ms",
                               "req_headers", "resp_excerpt"],
        }[fname]
    # empty targets/deliveries while in single-target mode
    assert zf.read("targets.jsonl") == b""
    assert zf.read("deliveries.jsonl") == b""
    zf.close()

    # restore preserves current settings: change them *after* the backup
    client.post("/api/settings", json={**settings, "hmac_secret": "after-backup",
                "target_url": "http://testserver/recv"})
    r = client.post("/api/restore", content=good_zip,
                    headers={"Content-Type": "application/zip"})
    assert r.status_code == 200, r.text
    summary = r.json()
    assert summary["ok"] is True
    assert summary["counts"]["events_after"] == n_events
    assert summary["counts"]["attempts_after"] == n_attempts_before
    kept = client.get("/api/settings").json()
    assert kept["hmac_secret"] == "after-backup", "restore must preserve current settings"
    assert kept["target_url"] == "http://testserver/recv"
    # restored data is intact
    d = client.get("/api/events/evt-001").json()
    assert d["status"] == "success" and d["payload_parsed"]["type"] == "a2"
    assert len(d["timeline"]) == 3
    assert client.get("/api/events/evt-bad").json()["timeline"][1]["idem_key"] != \
        client.get("/api/events/evt-bad").json()["timeline"][0]["idem_key"]

    # corrupted payload line (manifest hash updated so the hash check does not
    # mask the deeper validation) -> rejected, DB untouched
    events_data = zipfile.ZipFile(io.BytesIO(good_zip)).read("events.jsonl")
    first_line = events_data.split(b"\n")[0]
    obj = json.loads(first_line)
    obj["payload"] = "{not-json"
    tampered_payload = repack(
        good_zip,
        rewrites={"events.jsonl":
                  (json.dumps(obj, separators=(",", ":")).encode()
                   + events_data[len(first_line):])})
    r = client.post("/api/restore", content=tampered_payload,
                    headers={"Content-Type": "application/zip"})
    assert r.status_code == 422 and "事件载荷不是合法 JSON" in r.text, r.text
    assert client.get("/api/events/evt-001").json()["payload_parsed"]["type"] == "a2"

    # foreign key violation: attempts row pointing at a missing event
    att_data = zipfile.ZipFile(io.BytesIO(good_zip)).read("attempts.jsonl")
    att_first = json.loads(att_data.split(b"\n")[0])
    att_first["event_id"] = "evt-does-not-exist"
    tampered_fk = repack(
        good_zip,
        rewrites={"attempts.jsonl":
                  (json.dumps(att_first, separators=(",", ":")).encode()
                   + att_data[len(att_data.split(b"\n")[0]):])})
    r = client.post("/api/restore", content=tampered_fk,
                    headers={"Content-Type": "application/zip"})
    assert r.status_code == 422 and "外键" in r.text, r.text

    # stale/wrong manifest sha256 -> rejected
    bad_manifest = repack(good_zip, manifest_edit=lambda m: m["files"]["events.jsonl"].update(
        sha256="0" * 64))
    r = client.post("/api/restore", content=bad_manifest,
                    headers={"Content-Type": "application/zip"})
    assert r.status_code == 422 and "SHA-256" in r.text, r.text

    # incompatible db version -> rejected
    bad_dbv = repack(good_zip, manifest_edit=lambda m: m.update(db_version=999))
    r = client.post("/api/restore", content=bad_dbv,
                    headers={"Content-Type": "application/zip"})
    assert r.status_code == 422 and "数据库版本" in r.text, r.text

    # not a zip at all -> rejected
    r = client.post("/api/restore", content=b"definitely not a zip",
                    headers={"Content-Type": "application/zip"})
    assert r.status_code == 422 and "zip" in r.text

    # restore rejects while the dispatcher is busy
    client.post("/api/settings", json={**settings,
                "target_url": "http://testserver/recv?sleep=2",
                "timeout_seconds": 5.0, "max_retries": 0})
    client.post("/api/events/import", json=[{"event_id": "evt-busy", "type": "z"}])
    client.post("/api/events/evt-busy/replay")
    time.sleep(0.3)  # let the worker pick it up
    r = client.post("/api/restore", content=good_zip,
                    headers={"Content-Type": "application/zip"})
    assert r.status_code == 409, r.text
    wait_until(lambda: client.get("/api/events/evt-busy").json()["status"] == "success",
               timeout=15, label="evt-busy delivered")

    # backup + restore also works on a freshly cleared (empty) database
    client.delete("/api/events")
    empty_zip = client.get("/api/backup").content
    assert json.loads(zipfile.ZipFile(io.BytesIO(empty_zip))
                      .read("manifest.json"))["files"]["events.jsonl"]["lines"] == 0
    r = client.post("/api/restore", content=empty_zip,
                    headers={"Content-Type": "application/zip"})
    assert r.status_code == 200 and r.json()["counts"]["events_after"] == 0

    # clear history: events/attempts gone, settings preserved
    assert client.delete("/api/events").json() == {"ok": True}
    assert client.get("/api/events").json()["events"] == []
    assert client.get("/api/timeline").json()["attempts"] == []
    assert client.get("/api/settings").json()["hmac_secret"] == "topsecret"

    print("\nALL SMOKE TESTS PASSED")


if __name__ == "__main__":
    import httpx

    # ASGITransport (intentionally) does not enforce httpx timeouts, so wrap
    # it to emulate the real httpcore transport's read-timeout behaviour.
    class TimedASGITransport(httpx.ASGITransport):
        async def handle_async_request(self, request):
            timeout = request.extensions.get("timeout", {})
            deadline = timeout.get("read") if timeout else None
            try:
                if deadline is not None:
                    return await asyncio.wait_for(
                        super().handle_async_request(request), timeout=deadline)
                return await super().handle_async_request(request)
            except asyncio.TimeoutError:
                raise httpx.ReadTimeout("simulated read timeout", request=request)

    # Make the background worker's async httpx client talk to this ASGI app
    # instead of performing real DNS for the "testserver" hostname.
    dispatcher._client_factory = lambda: httpx.AsyncClient(
        transport=TimedASGITransport(app=app),
        base_url="http://testserver",
    )
    with TestClient(app) as client:
        run(client)
