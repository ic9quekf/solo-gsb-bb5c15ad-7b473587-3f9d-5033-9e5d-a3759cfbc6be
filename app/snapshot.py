"""Experimental-data snapshot backup & atomic restore.

A snapshot is a zip archive (always written with forward-slash zip member
names) containing one JSONL file per table plus a ``manifest.json``:

    manifest.json
    settings.jsonl   -- exactly 1 row, kept only for reference
    events.jsonl     -- one event per line, fixed field order
    attempts.jsonl   -- one attempt per line, fixed field order

Each JSONL file lists its fields in a single fixed order (the order declared
in ``FIELDS`` below), and the manifest records per-file line counts, sizes,
SHA-256 digests and the database schema version. Restore validates *everything*
(manifest, digests, field sets, types, event-payload parseability, foreign
keys) and raises :class:`SnapshotError` on the first problem -- no row is
written until all checks pass.
"""
from __future__ import annotations

import hashlib
import io
import json
import zipfile
from datetime import datetime, timezone
from typing import Any

from . import db

FORMAT = "webhook-replay-lab-snapshot"
FORMAT_VERSION = 1

SETTINGS_FILE = "settings.jsonl"
EVENTS_FILE = "events.jsonl"
ATTEMPTS_FILE = "attempts.jsonl"
MANIFEST_NAME = "manifest.json"

# Fixed on-disk field order for every JSONL table.
FIELDS: dict[str, list[str]] = {
    SETTINGS_FILE: [
        "id", "target_url", "hmac_secret", "max_retries", "timeout_seconds",
    ],
    EVENTS_FILE: [
        "event_id", "seq", "payload", "status", "created_at", "updated_at",
    ],
    ATTEMPTS_FILE: [
        "id", "event_id", "attempt_no", "replay_no", "idem_key", "request_at",
        "response_at", "http_status", "outcome", "elapsed_ms", "req_headers",
        "resp_excerpt",
    ],
}

EVENT_STATUSES = {"pending", "queued", "running", "success", "failed"}
ATTEMPT_OUTCOMES = {"delivered", "retriable", "non_retriable"}

# Simple guard against oversized / decompression-bomb uploads.
MAX_ARCHIVE_BYTES = 64 * 1024 * 1024
MAX_TOTAL_UNCOMPRESSED = 256 * 1024 * 1024

# statuses that describe dispatch work which cannot survive a restart
TRANSIENT_STATUSES = {"queued", "running"}


class SnapshotError(Exception):
    """Raised when a snapshot archive fails validation; never partially written."""


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _encode_jsonl(rows: list[dict[str, Any]], fields: list[str]) -> bytes:
    buf = io.BytesIO()
    for row in rows:
        line = json.dumps({f: row.get(f) for f in fields},
                          ensure_ascii=False, separators=(",", ":"))
        buf.write(line.encode("utf-8"))
        buf.write(b"\n")
    return buf.getvalue()


def _file_meta(name: str, data: bytes) -> dict[str, Any]:
    return {
        "name": name,
        "lines": data.count(b"\n"),
        "bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
    }


def create_backup() -> tuple[bytes, str, dict[str, Any]]:
    """Export current data. Returns (zip_bytes, download_name, manifest)."""
    snap = db.dump_snapshot()

    payload: dict[str, bytes] = {
        SETTINGS_FILE: _encode_jsonl(snap["settings"], FIELDS[SETTINGS_FILE]),
        EVENTS_FILE: _encode_jsonl(snap["events"], FIELDS[EVENTS_FILE]),
        ATTEMPTS_FILE: _encode_jsonl(snap["attempts"], FIELDS[ATTEMPTS_FILE]),
    }
    files = [_file_meta(name, data) for name, data in payload.items()]
    manifest = {
        "format": FORMAT,
        "format_version": FORMAT_VERSION,
        "db_version": db.SCHEMA_VERSION,
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "files": {m["name"]: m for m in files},
        "note": "settings.jsonl is informational; restore preserves current settings",
    }

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED,
                         compresslevel=9) as zf:
        zf.writestr(MANIFEST_NAME,
                    json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True))
        for name, data in payload.items():
            zf.writestr(name, data)
    data = buf.getvalue()
    name = f"webhook-replay-snapshot-{_utc_stamp()}.zip"
    return data, name, manifest


# ---------- parsing / validation ----------

def _is_str(v: Any) -> bool:
    return isinstance(v, str) and v != ""


def _is_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _is_number(v: Any) -> bool:
    return (isinstance(v, (int, float)) and not isinstance(v, bool))


def parse_snapshot(content: bytes) -> dict[str, Any]:
    """Validate an uploaded archive fully without touching the database.

    Returns ``{"manifest", "settings", "events", "attempts"}`` with parsed
    rows. Raises :class:`SnapshotError` on any problem.
    """
    if not content:
        raise SnapshotError("上传内容为空，请选择有效的快照压缩包")
    if len(content) > MAX_ARCHIVE_BYTES:
        raise SnapshotError(
            f"压缩包过大（{len(content)} 字节），上限 {MAX_ARCHIVE_BYTES} 字节")
    try:
        zf = zipfile.ZipFile(io.BytesIO(content))
    except zipfile.BadZipFile:
        raise SnapshotError("不是有效的 zip 压缩包")

    names = zf.namelist()
    expected = {MANIFEST_NAME, SETTINGS_FILE, EVENTS_FILE, ATTEMPTS_FILE}
    if set(names) != expected:
        missing = sorted(expected - set(names))
        extra = sorted(set(names) - expected)
        bits = []
        if missing:
            bits.append(f"缺少文件 {missing}")
        if extra:
            bits.append(f"存在未预期文件 {extra}")
        raise SnapshotError("快照结构不合法：" + "；".join(bits))

    total = 0
    raw: dict[str, bytes] = {}
    for info in zf.infolist():
        if info.is_dir():
            raise SnapshotError(f"快照不允许包含目录条目：{info.filename}")
        total += info.file_size
        if total > MAX_TOTAL_UNCOMPRESSED:
            raise SnapshotError("解压后体积超过上限，疑似压缩炸弹")
        raw[info.filename] = zf.read(info.filename)
    zf.close()

    errors: list[str] = []
    manifest = _validate_manifest(raw[MANIFEST_NAME], raw, errors)

    tables: dict[str, list[dict[str, Any]]] = {}
    for fname in (SETTINGS_FILE, EVENTS_FILE, ATTEMPTS_FILE):
        tables[fname] = _validate_jsonl(fname, raw[fname],
                                        manifest, errors)

    settings = tables[SETTINGS_FILE]
    events = tables[EVENTS_FILE]
    attempts = tables[ATTEMPTS_FILE]

    # ---- table-specific semantic checks ----
    if len(settings) != 1:
        errors.append(f"{SETTINGS_FILE}: 必须恰好包含 1 行设置，实际 {len(settings)} 行")
    elif settings[0].get("id") != 1:
        errors.append(f"{SETTINGS_FILE}: 唯一一行的 id 必须为 1")
    else:
        s = settings[0]
        if not _is_str(s.get("target_url")):
            errors.append(f"{SETTINGS_FILE}: target_url 必须是非空字符串")
        if not isinstance(s.get("hmac_secret"), str):
            errors.append(f"{SETTINGS_FILE}: hmac_secret 必须是字符串")
        if not _is_int(s.get("max_retries")) or not 0 <= s["max_retries"] <= 10:
            errors.append(f"{SETTINGS_FILE}: max_retries 必须是 0–10 的整数")
        if not _is_number(s.get("timeout_seconds")) or not 0 < s["timeout_seconds"] <= 60:
            errors.append(f"{SETTINGS_FILE}: timeout_seconds 必须是 0–60 之间的数字")

    event_ids: set[str] = set()
    seqs: set[int] = set()
    for i, e in enumerate(events, 1):
        eid = e.get("event_id")
        if not _is_str(eid):
            errors.append(f"{EVENTS_FILE} 第 {i} 行: event_id 必须是非空字符串")
        elif eid in event_ids:
            errors.append(f"{EVENTS_FILE} 第 {i} 行: event_id 重复：{eid!r}")
        else:
            event_ids.add(eid)
        seq = e.get("seq")
        if not _is_int(seq) or seq < 1:
            errors.append(f"{EVENTS_FILE} 第 {i} 行: seq 必须是 ≥1 的整数")
        elif seq in seqs:
            errors.append(f"{EVENTS_FILE} 第 {i} 行: seq 重复：{seq}")
        else:
            seqs.add(seq)
        if not isinstance(e.get("payload"), str) or not e["payload"].strip():
            errors.append(f"{EVENTS_FILE} 第 {i} 行: payload 必须是非空 JSON 字符串")
        else:
            try:
                parsed = json.loads(e["payload"])
                if not isinstance(parsed, dict):
                    errors.append(
                        f"{EVENTS_FILE} 第 {i} 行: 事件载荷必须是 JSON 对象")
            except json.JSONDecodeError as exc:
                errors.append(
                    f"{EVENTS_FILE} 第 {i} 行: 事件载荷不是合法 JSON（{exc.msg}）")
        if e.get("status") not in EVENT_STATUSES:
            errors.append(
                f"{EVENTS_FILE} 第 {i} 行: status 非法（{e.get('status')!r}），"
                f"允许值：{sorted(EVENT_STATUSES)}")
        for col in ("created_at", "updated_at"):
            if not _is_str(e.get(col)):
                errors.append(f"{EVENTS_FILE} 第 {i} 行: {col} 必须是非空字符串")

    attempt_ids: set[int] = set()
    for i, a in enumerate(attempts, 1):
        aid = a.get("id")
        if not _is_int(aid) or aid < 1:
            errors.append(f"{ATTEMPTS_FILE} 第 {i} 行: id 必须是 ≥1 的整数")
        elif aid in attempt_ids:
            errors.append(f"{ATTEMPTS_FILE} 第 {i} 行: id 重复：{aid}")
        else:
            attempt_ids.add(aid)
        if a.get("event_id") not in event_ids:
            errors.append(
                f"{ATTEMPTS_FILE} 第 {i} 行: 外键 event_id={a.get('event_id')!r}"
                " 在 events.jsonl 中不存在")
        if not _is_int(a.get("attempt_no")) or a["attempt_no"] < 1:
            errors.append(f"{ATTEMPTS_FILE} 第 {i} 行: attempt_no 必须是 ≥1 的整数")
        if not _is_int(a.get("replay_no")) or a["replay_no"] < 1:
            errors.append(f"{ATTEMPTS_FILE} 第 {i} 行: replay_no 必须是 ≥1 的整数")
        if not _is_str(a.get("idem_key")):
            errors.append(f"{ATTEMPTS_FILE} 第 {i} 行: idem_key 必须是非空字符串")
        if not _is_str(a.get("request_at")):
            errors.append(f"{ATTEMPTS_FILE} 第 {i} 行: request_at 必须是非空字符串")
        if a.get("response_at") is not None and not _is_str(a["response_at"]):
            errors.append(f"{ATTEMPTS_FILE} 第 {i} 行: response_at 必须是字符串或 null")
        status = a.get("http_status")
        if status is not None and (not _is_int(status) or not 100 <= status <= 599):
            errors.append(
                f"{ATTEMPTS_FILE} 第 {i} 行: http_status 必须是 100–599 的整数或 null")
        if a.get("outcome") not in ATTEMPT_OUTCOMES:
            errors.append(
                f"{ATTEMPTS_FILE} 第 {i} 行: outcome 非法（{a.get('outcome')!r}），"
                f"允许值：{sorted(ATTEMPT_OUTCOMES)}")
        elapsed = a.get("elapsed_ms")
        if elapsed is not None and (not _is_int(elapsed) or elapsed < 0):
            errors.append(f"{ATTEMPTS_FILE} 第 {i} 行: elapsed_ms 必须是 ≥0 的整数或 null")
        if a.get("req_headers") is not None and not isinstance(a["req_headers"], str):
            errors.append(f"{ATTEMPTS_FILE} 第 {i} 行: req_headers 必须是 JSON 字符串或 null")
        if a.get("resp_excerpt") is not None and not isinstance(a["resp_excerpt"], str):
            errors.append(f"{ATTEMPTS_FILE} 第 {i} 行: resp_excerpt 必须是字符串或 null")

    if errors:
        raise SnapshotError(
            f"快照校验失败，共 {len(errors)} 处错误，已拒绝写入：\n- "
            + "\n- ".join(errors))

    return {
        "manifest": manifest,
        "settings": settings,
        "events": events,
        "attempts": attempts,
    }


def _validate_manifest(raw_manifest: bytes, raw_files: dict[str, bytes],
                       errors: list[str]) -> dict[str, Any] | None:
    try:
        text = raw_manifest.decode("utf-8")
    except UnicodeDecodeError:
        errors.append(f"{MANIFEST_NAME}: 不是 UTF-8 文本")
        return None
    try:
        manifest = json.loads(text)
    except json.JSONDecodeError as exc:
        errors.append(f"{MANIFEST_NAME}: 不是合法 JSON（{exc.msg}）")
        return None
    if not isinstance(manifest, dict):
        errors.append(f"{MANIFEST_NAME}: 必须是 JSON 对象")
        return None
    if manifest.get("format") != FORMAT:
        errors.append(f"{MANIFEST_NAME}: format 不匹配（{manifest.get('format')!r}）")
    if manifest.get("format_version") != FORMAT_VERSION:
        errors.append(
            f"{MANIFEST_NAME}: format_version 必须为 {FORMAT_VERSION}，"
            f"实际为 {manifest.get('format_version')!r}")
    if not _is_int(manifest.get("db_version")):
        errors.append(f"{MANIFEST_NAME}: db_version 缺失或不是整数")
    elif manifest["db_version"] != db.SCHEMA_VERSION:
        errors.append(
            f"{MANIFEST_NAME}: 数据库版本不兼容（快照 v{manifest['db_version']}，"
            f"当前 v{db.SCHEMA_VERSION}）")
    files = manifest.get("files")
    if not isinstance(files, dict):
        errors.append(f"{MANIFEST_NAME}: files 缺失或不是对象")
        return manifest

    for fname, data in raw_files.items():
        if fname == MANIFEST_NAME:
            continue
        meta = files.get(fname)
        if not isinstance(meta, dict):
            errors.append(f"{MANIFEST_NAME}: files 缺少 {fname} 的元数据")
            continue
        digest = hashlib.sha256(data).hexdigest()
        if meta.get("sha256") != digest:
            errors.append(f"{fname}: SHA-256 与 manifest 不符（文件可能已损坏或被篡改）")
        n_lines = data.count(b"\n")
        if meta.get("lines") != n_lines:
            errors.append(
                f"{fname}: 行数与 manifest 不符（记录 {meta.get('lines')}，实际 {n_lines}）")
        if meta.get("bytes") != len(data):
            errors.append(
                f"{fname}: 字节数与 manifest 不符（记录 {meta.get('bytes')}，实际 {len(data)}）")
    return manifest


def _validate_jsonl(fname: str, data: bytes, manifest: dict[str, Any] | None,
                    errors: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    expected_fields = FIELDS[fname]
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        errors.append(f"{fname}: 不是 UTF-8 文本")
        return rows

    lines = text.split("\n")
    if lines and lines[-1] == "":
        lines.pop()  # trailing newline after the last record
    if data and not data.endswith(b"\n"):
        errors.append(f"{fname}: 必须以换行符结尾（每行一条 JSON 记录）")

    for lineno, line in enumerate(lines, 1):
        if not line:
            errors.append(f"{fname} 第 {lineno} 行: 不允许空行")
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            errors.append(f"{fname} 第 {lineno} 行: 不是合法 JSON（{exc.msg}）")
            continue
        if not isinstance(row, dict):
            errors.append(f"{fname} 第 {lineno} 行: 每条记录必须是 JSON 对象")
            continue
        if list(row.keys()) != expected_fields:
            missing = [f for f in expected_fields if f not in row]
            extra = [f for f in row if f not in expected_fields]
            order_ok = set(row.keys()) == set(expected_fields) and not missing
            detail = "字段顺序与约定不符" if order_ok else f"缺少 {missing}，多余 {extra}"
            errors.append(f"{fname} 第 {lineno} 行: 固定字段顺序不符（{detail}）")
            continue
        rows.append(row)
    return rows
