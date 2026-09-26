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

SNAPSHOT_FILES = {"manifest.json", "settings.jsonl", "targets.jsonl",
                  "events.jsonl", "deliveries.jsonl", "attempts.jsonl"}


def wait_until(predicate, timeout=20, label="condition"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.2)
    raise AssertionError(f"timeout waiting for {label}")


def make_idle(client):
    def idle():
        st = client.get("/api/status").json()
        return st["queue_depth"] == 0 and not st["inflight"]
    return idle


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


def run(client: TestClient) -> None:
    idle = make_idle(client)
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
    # legacy single-target config maps onto the 'default' target
    assert [t["name"] for t in r.json()["targets"]] == ["default"]
    assert r.json()["targets"][0]["url"].endswith("times=2")
    got = client.get("/api/targets").json()
    assert got["enabled"] == ["default"]

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
    assert st["targets"] == ["default"]
    print("status after replay:", st)

    # timeline: each event had 500,500,200 with a shared idem key
    detail = client.get("/api/events/evt-001").json()
    attempts = detail["timeline"]
    assert len(attempts) == 3, [(a["outcome"], a["http_status"]) for a in attempts]
    assert [a["http_status"] for a in attempts] == [500, 500, 200]
    assert [a["outcome"] for a in attempts] == ["retriable", "retriable", "delivered"]
    assert all(a["target"] == "default" for a in attempts)
    keys = {a["idem_key"] for a in attempts}
    assert len(keys) == 1, "retries within one replay must share idempotency key"
    hdr = json.loads(attempts[0]["req_headers"])
    assert hdr["X-Webhook-Event"] == "evt-001"
    assert hdr["X-Webhook-Target"] == "default"
    assert hdr["Idempotency-Key"] == list(keys)[0]
    assert "t=" in hdr["X-Webhook-Signature"] and "v1=" in hdr["X-Webhook-Signature"]
    assert detail["payload_parsed"]["type"] == "a2"
    assert {d["target"]: d["status"] for d in detail["deliveries"]} == \
        {"default": "delivered"}
    assert detail["pending_targets"] == []
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
    assert {l["target"] for l in delivered} == {"default"}
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

    # ---------- snapshot backup / restore ----------

    n_events = len(client.get("/api/events").json()["events"])
    n_attempts_before = len(client.get("/api/timeline?limit=2000").json()["attempts"])

    r = client.get("/api/backup")
    assert r.status_code == 200, r.text
    assert r.headers["content-type"] == "application/zip"
    cd = r.headers["content-disposition"]
    assert "attachment" in cd and cd.endswith('.zip"'), cd
    good_zip = r.content
    zf = zipfile.ZipFile(io.BytesIO(good_zip))
    assert set(zf.namelist()) == SNAPSHOT_FILES
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
        assert list(json.loads(data.split(b"\n")[0]).keys()) == {
            "settings.jsonl": ["id", "target_url", "hmac_secret", "max_retries",
                               "timeout_seconds"],
            "targets.jsonl": ["name", "url", "enabled", "position",
                              "created_at", "updated_at"],
            "events.jsonl": ["event_id", "seq", "payload", "status",
                             "created_at", "updated_at"],
            "deliveries.jsonl": ["event_id", "target", "status", "updated_at"],
            "attempts.jsonl": ["id", "event_id", "target", "attempt_no", "replay_no",
                               "idem_key", "request_at", "response_at",
                               "http_status", "outcome", "elapsed_ms",
                               "req_headers", "resp_excerpt"],
        }[fname]
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
    assert "deliveries_after" in summary["counts"]
    kept = client.get("/api/settings").json()
    assert kept["hmac_secret"] == "after-backup", "restore must preserve current settings"
    assert kept["target_url"] == "http://testserver/recv"
    # restored data is intact
    d = client.get("/api/events/evt-001").json()
    assert d["status"] == "success" and d["payload_parsed"]["type"] == "a2"
    assert len(d["timeline"]) == 3
    assert {x["target"]: x["status"] for x in d["deliveries"]} == \
        {"default": "delivered"}
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

    # foreign key violation on a deliveries row -> rejected as well
    del_data = zipfile.ZipFile(io.BytesIO(good_zip)).read("deliveries.jsonl")
    del_first = json.loads(del_data.split(b"\n")[0])
    del_first["event_id"] = "evt-does-not-exist"
    tampered_fk2 = repack(
        good_zip,
        rewrites={"deliveries.jsonl":
                  (json.dumps(del_first, separators=(",", ":")).encode()
                   + del_data[len(del_data.split(b"\n")[0]):])})
    r = client.post("/api/restore", content=tampered_fk2,
                    headers={"Content-Type": "application/zip"})
    assert r.status_code == 422 and "外键" in r.text, r.text

    # illegal delivery status -> rejected
    del_bad = json.loads(del_data.split(b"\n")[0])
    del_bad["status"] = "half-delivered"
    tampered_status = repack(
        good_zip,
        rewrites={"deliveries.jsonl":
                  (json.dumps(del_bad, separators=(",", ":")).encode()
                   + del_data[len(del_data.split(b"\n")[0]):])})
    r = client.post("/api/restore", content=tampered_status,
                    headers={"Content-Type": "application/zip"})
    assert r.status_code == 422 and "status 非法" in r.text, r.text

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

    print("single-target + snapshot tests passed")

    # ---------- multi-target fan-out ----------

    # target-set validation
    r = client.put("/api/targets", json={"targets": []})
    assert r.status_code == 422, r.text
    r = client.put("/api/targets", json={"targets": [
        {"name": f"t{i}", "url": "http://testserver/recv"} for i in range(6)]})
    assert r.status_code == 422, r.text
    r = client.put("/api/targets", json={"targets": [
        {"name": "dup", "url": "http://testserver/recv"},
        {"name": "dup", "url": "http://testserver/recv"}]})
    assert r.status_code == 422 and "重复" in r.text, r.text
    r = client.put("/api/targets", json={"targets": [
        {"name": "bad", "url": "ftp://nope"}]})
    assert r.status_code == 422 and "http" in r.text, r.text
    r = client.put("/api/targets", json={"targets": [
        {"name": "off", "url": "http://testserver/recv", "enabled": False}]})
    assert r.status_code == 422 and "启用" in r.text, r.text
    r = client.put("/api/targets", json={"targets": [
        {"name": "bad name!", "url": "http://testserver/recv"}]})
    assert r.status_code == 422, r.text

    # configure 3 named targets via the combined settings endpoint:
    #   alpha healthy, beta fails twice then recovers, gamma always 422
    fanout = {
        "hmac_secret": "fanout-secret",
        "max_retries": 3,
        "timeout_seconds": 5.0,
        "targets": [
            {"name": "alpha", "url": "http://testserver/recv"},
            {"name": "beta", "url": "http://testserver/recv?fail=500&times=2"},
            {"name": "gamma", "url": "http://testserver/recv?fail=422&times=9"},
        ],
    }
    r = client.post("/api/settings", json=fanout)
    assert r.status_code == 200, r.text
    assert [t["name"] for t in r.json()["targets"]] == ["alpha", "beta", "gamma"]
    assert client.get("/api/targets").json()["enabled"] == ["alpha", "beta", "gamma"]

    client.post("/api/events/import", json=[
        {"event_id": "evt-m1", "type": "fanout"},
        {"event_id": "evt-m2", "type": "fanout"},
    ])
    r = client.post("/api/replay", json={})
    assert len(r.json()["queued"]) == 2

    # gamma can never succeed -> both events end up failed (partial fan-out)
    wait_until(lambda: client.get("/api/status").json()["counts"]["failed"] == 2,
               timeout=60, label="both events partially failed")
    st = client.get("/api/status").json()
    assert st["counts"]["success"] == 0 and st["targets"] == ["alpha", "beta", "gamma"]

    d1 = client.get("/api/events/evt-m1").json()
    assert d1["status"] == "failed"
    states = {x["target"]: x["status"] for x in d1["deliveries"]}
    assert states == {"alpha": "delivered", "beta": "delivered", "gamma": "failed"}, states
    assert d1["pending_targets"] == ["gamma"]

    # per-target independent retries and idempotency keys
    by_target = {}
    for a in d1["timeline"]:
        by_target.setdefault(a["target"], []).append(a)
    assert [a["http_status"] for a in by_target["alpha"]] == [200]
    assert [a["http_status"] for a in by_target["beta"]] == [500, 500, 200]
    assert [a["http_status"] for a in by_target["gamma"]] == [422]
    assert len({a["idem_key"] for a in by_target["beta"]}) == 1, \
        "retries of one target round share the idempotency key"
    all_keys = {t: {a["idem_key"] for a in v} for t, v in by_target.items()}
    assert len({k for keys in all_keys.values() for k in keys}) == 3, \
        "each target gets its own idempotency key"
    # per-target signature headers verify against the shared secret
    from app.signing import canonical_body, verify as verify_sig
    body_raw = canonical_body(d1["payload_parsed"])
    for t, rows in by_target.items():
        h = json.loads(rows[0]["req_headers"])
        assert h["X-Webhook-Target"] == t
        assert verify_sig(body_raw, "fanout-secret", h["X-Webhook-Signature"])
    # receiver saw the fan-out with target labels
    logs = client.get("/recv/logs?limit=200").json()["logs"]
    m1_logs = [l for l in logs if l["event"] == "evt-m1"]
    assert {l["target"] for l in m1_logs} == {"alpha", "beta", "gamma"}
    print("evt-m1 per-target attempts:",
          {t: [a["http_status"] for a in v] for t, v in by_target.items()})

    # fix gamma, then retry ONLY the failed target of evt-m1
    fanout["targets"][2]["url"] = "http://testserver/recv"
    r = client.post("/api/settings", json=fanout)
    assert r.status_code == 200, r.text
    n_attempts_before = len(client.get("/api/events/evt-m1").json()["timeline"])
    r = client.post("/api/events/evt-m1/replay", json={"only_failed_targets": True})
    assert r.status_code == 200 and r.json()["queued"] is True
    assert r.json()["targets"] == ["gamma"], r.json()
    wait_until(lambda: client.get("/api/events/evt-m1").json()["status"] == "success",
               timeout=15, label="evt-m1 completed via failed-target retry")
    d1 = client.get("/api/events/evt-m1").json()
    assert {x["target"]: x["status"] for x in d1["deliveries"]} == \
        {"alpha": "delivered", "beta": "delivered", "gamma": "delivered"}
    new_attempts = d1["timeline"][n_attempts_before:]
    assert len(new_attempts) == 1 and new_attempts[0]["target"] == "gamma"
    assert new_attempts[0]["http_status"] == 200
    # gamma got a fresh idem key for the new round
    gamma_keys = {a["idem_key"] for a in d1["timeline"] if a["target"] == "gamma"}
    assert len(gamma_keys) == 2

    # nothing left to retry -> explicit note, not an error
    r = client.post("/api/events/evt-m1/replay", json={"only_failed_targets": True})
    assert r.status_code == 200 and r.json()["queued"] is False

    # explicit target selection: re-deliver evt-m1 to alpha only
    n_before = len(client.get("/api/events/evt-m1").json()["timeline"])
    r = client.post("/api/events/evt-m1/replay", json={"targets": ["alpha"]})
    assert r.json()["targets"] == ["alpha"]
    # the attempt row is written before the final status flip, so also wait
    # for the worker to go idle
    wait_until(lambda: len(client.get("/api/events/evt-m1").json()["timeline"]) > n_before
               and idle(), timeout=15, label="evt-m1 alpha-only replay done")
    d1 = client.get("/api/events/evt-m1").json()
    assert d1["timeline"][-1]["target"] == "alpha"
    assert d1["status"] == "success"
    # unknown target name -> 422
    r = client.post("/api/events/evt-m1/replay", json={"targets": ["nope"]})
    assert r.status_code == 422, r.text

    # batch retry of failed targets only: break beta, replay evt-m2, then heal
    fanout["targets"][1]["url"] = "http://testserver/recv?fail=500&times=9"
    fanout["max_retries"] = 1
    r = client.post("/api/settings", json=fanout)
    assert r.status_code == 200
    r = client.post("/api/events/evt-m2/replay")  # full fan-out, beta now fails
    assert r.json()["queued"] is True
    # evt-m2 was already 'failed' before this round, so also wait for the
    # worker to go idle to be sure the NEW round has finished
    wait_until(lambda: client.get("/api/events/evt-m2").json()["status"] == "failed"
               and idle(), timeout=30, label="evt-m2 failed on beta")
    d2 = client.get("/api/events/evt-m2").json()
    states = {x["target"]: x["status"] for x in d2["deliveries"]}
    assert states["beta"] == "failed" and states["alpha"] == "delivered", states
    n_alpha_attempts = len([a for a in d2["timeline"] if a["target"] == "alpha"])

    fanout["targets"][1]["url"] = "http://testserver/recv"
    client.post("/api/settings", json=fanout)
    r = client.post("/api/replay",
                    json={"only_failed": True, "only_failed_targets": True})
    assert "evt-m2" in r.json()["queued"], r.json()
    wait_until(lambda: client.get("/api/events/evt-m2").json()["status"] == "success",
               timeout=15, label="evt-m2 healed via batch failed-target retry")
    d2 = client.get("/api/events/evt-m2").json()
    # alpha was already delivered: the batch retry must not have hit it again
    assert len([a for a in d2["timeline"] if a["target"] == "alpha"]) == n_alpha_attempts
    assert {x["target"]: x["status"] for x in d2["deliveries"]} == \
        {"alpha": "delivered", "beta": "delivered", "gamma": "delivered"}

    # event list carries the per-target delivery summary for the UI
    lst = {e["event_id"]: e for e in client.get("/api/events").json()["events"]}
    assert lst["evt-m2"]["deliveries"] == \
        {"alpha": "delivered", "beta": "delivered", "gamma": "delivered"}

    # ---------- snapshot with multi-target state ----------

    # gamma -> always 422 again; evt-m3 partially fails
    fanout["targets"][2]["url"] = "http://testserver/recv?fail=422&times=9"
    fanout["max_retries"] = 3
    client.post("/api/settings", json=fanout)
    client.post("/api/events/import", json=[{"event_id": "evt-m3", "type": "fanout"}])
    client.post("/api/events/evt-m3/replay")
    wait_until(lambda: client.get("/api/events/evt-m3").json()["status"] == "failed",
               timeout=30, label="evt-m3 partially failed")
    d3 = client.get("/api/events/evt-m3").json()
    assert {x["target"]: x["status"] for x in d3["deliveries"]}["gamma"] == "failed"

    multi_zip = client.get("/api/backup").content
    mtargets = [json.loads(l) for l in zipfile.ZipFile(io.BytesIO(multi_zip))
                .read("targets.jsonl").decode().splitlines()]
    assert [t["name"] for t in mtargets] == ["alpha", "beta", "gamma"]

    # wipe, restore, and confirm per-target states survive the round trip
    client.delete("/api/events")
    assert client.get("/api/events").json()["events"] == []
    r = client.post("/api/restore", content=multi_zip,
                    headers={"Content-Type": "application/zip"})
    assert r.status_code == 200, r.text
    d3 = client.get("/api/events/evt-m3").json()
    assert d3["status"] == "failed"
    assert {x["target"]: x["status"] for x in d3["deliveries"]}["gamma"] == "failed"
    assert d3["pending_targets"] == ["gamma"]

    # heal gamma and retry only the failed target of the restored event
    fanout["targets"][2]["url"] = "http://testserver/recv"
    client.post("/api/settings", json=fanout)
    n_before = len(d3["timeline"])
    r = client.post("/api/events/evt-m3/replay", json={"only_failed_targets": True})
    assert r.json()["targets"] == ["gamma"]
    wait_until(lambda: client.get("/api/events/evt-m3").json()["status"] == "success",
               timeout=15, label="restored evt-m3 healed")
    d3 = client.get("/api/events/evt-m3").json()
    assert all(a["target"] == "gamma" for a in d3["timeline"][n_before:])

    # legacy single-target config still works after a multi-target setup:
    # it updates (or creates) the target named 'default'
    r = client.post("/api/settings", json={
        "target_url": "http://testserver/recv",
        "hmac_secret": "legacy", "max_retries": 2, "timeout_seconds": 4.0})
    names = [t["name"] for t in r.json()["targets"]]
    assert "default" in names and len(names) == 4, names
    assert client.get("/api/settings").json()["target_url"] == \
        "http://testserver/recv"

    # back to a single-target setup via PUT /api/targets
    r = client.put("/api/targets", json={"targets": [
        {"name": "default", "url": "http://testserver/recv"}]})
    assert r.status_code == 200 and r.json()["enabled"] == ["default"]
    assert [t["name"] for t in client.get("/api/settings").json()["targets"]] == ["default"]

    print("multi-target fan-out tests passed")
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
