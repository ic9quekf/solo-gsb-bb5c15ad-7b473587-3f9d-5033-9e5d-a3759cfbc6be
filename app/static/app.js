/* Webhook Replay Lab — frontend (vanilla JS) */
"use strict";

const $ = (id) => document.getElementById(id);
const esc = (s) => String(s ?? "").replace(/[&<>"']/g,
  (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

const SAMPLE = [
  { event_id: "evt-001", type: "invoice.created", customer: "Acme", amount: 100, currency: "CNY" },
  { event_id: "evt-002", type: "invoice.paid", customer: "Acme", amount: 100, currency: "CNY", paid_at: "2026-09-22T10:00:00Z" },
  { event_id: "evt-003", type: "invoice.refunded", customer: "Acme", amount: 50, currency: "CNY" },
];

const MAX_TARGETS = 5;
let lastEvents = [];
let configuredTargets = [];   // cached from /api/settings

async function api(path, opts = {}) {
  const resp = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...opts,
  });
  const text = await resp.text();
  const data = text ? JSON.parse(text) : {};
  if (!resp.ok) throw new Error(typeof data.detail === "string" ? data.detail : JSON.stringify(data.detail));
  return data;
}

function flash(el, text, ok = true) {
  el.textContent = text;
  el.style.color = ok ? "var(--green)" : "var(--red)";
  setTimeout(() => { el.textContent = ""; }, 4000);
}

// ---------- settings & targets ----------

function targetRow(t) {
  const div = document.createElement("div");
  div.className = "target-row";
  div.innerHTML =
    `<label class="tgt-enabled" title="启用该目标"><input type="checkbox" class="tgt-enabled-box" ${t.enabled ? "checked" : ""}/><span>启用</span></label>
     <input class="tgt-name" placeholder="名称" value="${esc(t.name || "")}"/>
     <input class="tgt-url" placeholder="http://127.0.0.1:8000/recv" value="${esc(t.url || "")}"/>
     <button class="mini danger tgt-remove" title="移除该目标">✕</button>`;
  div.querySelector(".tgt-remove").addEventListener("click", () => {
    const rows = document.querySelectorAll("#targets-editor .target-row");
    if (rows.length <= 1) { flash($("settings-msg"), "至少保留一个目标", false); return; }
    div.remove();
  });
  return div;
}

function renderTargetsEditor(targets) {
  const box = $("targets-editor");
  box.innerHTML = "";
  targets.forEach((t) => box.appendChild(targetRow(t)));
}

$("add-target").addEventListener("click", () => {
  const box = $("targets-editor");
  if (box.querySelectorAll(".target-row").length >= MAX_TARGETS) {
    flash($("settings-msg"), `最多 ${MAX_TARGETS} 个目标`, false);
    return;
  }
  box.appendChild(targetRow({ name: "", url: "", enabled: true }));
});

function collectTargets() {
  const rows = [...document.querySelectorAll("#targets-editor .target-row")];
  return rows.map((r) => ({
    name: r.querySelector(".tgt-name").value.trim(),
    url: r.querySelector(".tgt-url").value.trim(),
    enabled: r.querySelector(".tgt-enabled-box").checked,
  }));
}

async function loadSettings() {
  const s = await api("/api/settings");
  configuredTargets = s.targets || [];
  renderTargetsEditor(configuredTargets.length
    ? configuredTargets
    : [{ name: "default", url: s.target_url, enabled: true }]);
  $("hmac_secret").value = s.hmac_secret;
  $("max_retries").value = s.max_retries;
  $("timeout_seconds").value = s.timeout_seconds;
}

$("save-settings").addEventListener("click", async () => {
  const targets = collectTargets();
  try {
    const s = await api("/api/settings", {
      method: "POST",
      body: JSON.stringify({
        target_url: (targets.find((t) => t.name === "default") || targets[0] || {}).url || "",
        hmac_secret: $("hmac_secret").value,
        max_retries: Number($("max_retries").value),
        timeout_seconds: Number($("timeout_seconds").value),
        targets,
      }),
    });
    configuredTargets = s.targets || [];
    renderTargetsEditor(configuredTargets);
    flash($("settings-msg"), "已保存 ✓");
    refreshAll();
  } catch (e) { flash($("settings-msg"), e.message, false); }
});

// ---------- import ----------

$("load-sample").addEventListener("click", () => {
  $("payload").value = JSON.stringify(SAMPLE, null, 2);
});

$("file").addEventListener("change", async (ev) => {
  const f = ev.target.files[0];
  if (!f) return;
  $("payload").value = await f.text();
});

$("import").addEventListener("click", async () => {
  let doc;
  try {
    doc = JSON.parse($("payload").value);
  } catch (e) {
    flash($("import-msg"), "JSON 解析失败: " + e.message, false);
    return;
  }
  try {
    const r = await api("/api/events/import", { method: "POST", body: JSON.stringify(doc) });
    flash($("import-msg"), `入库 ${r.imported} 条（新增 ${r.created.length} / 覆盖 ${r.updated.length}）✓`);
    refreshAll();
  } catch (e) { flash($("import-msg"), e.message, false); }
});

// ---------- events ----------

function targetBadges(e) {
  const deliveries = e.deliveries || {};
  const names = [...new Set([...configuredTargets.map((t) => t.name), ...Object.keys(deliveries)])];
  if (!names.length) return '<span class="muted">—</span>';
  return names.map((n) => {
    const st = deliveries[n];
    const cls = st === "delivered" ? "success" : st === "failed" ? "failed" : "pending";
    const mark = st === "delivered" ? "✓" : st === "failed" ? "✗" : "·";
    const label = st === "delivered" ? "已送达" : st === "failed" ? "未送达" : "未投递";
    return `<span class="badge ${cls}" title="${esc(n)}：${label}">${esc(n)} ${mark}</span>`;
  }).join(" ");
}

async function refreshEvents() {
  const { events } = await api("/api/events");
  lastEvents = events;
  const body = $("events-body");
  if (!events.length) {
    body.innerHTML = '<tr><td colspan="6" class="muted center">暂无事件</td></tr>';
    return;
  }
  body.innerHTML = events.map((e) => {
    const busy = e.status === "queued" || e.status === "running";
    const label = { pending: "待发送", queued: "排队中", running: "发送中…", success: "成功", failed: "失败" }[e.status] || e.status;
    const count = e.attempt_count ? `${e.attempt_count} 次` : "—";
    const enc = encodeURIComponent(e.event_id);
    return `<tr>
      <td>${e.seq}</td>
      <td class="mono"><a class="link" onclick="showEvent('${enc}')">${esc(e.event_id)}</a></td>
      <td><span class="badge ${e.status}">${label}</span></td>
      <td class="tgt-badges">${targetBadges(e)}</td>
      <td>${count}</td>
      <td>
        <button class="mini" onclick="showEvent('${enc}')">详情</button>
        <button class="mini" ${busy ? "disabled" : ""} onclick="replayOne('${enc}')">重放</button>
        <button class="mini" ${busy || e.status !== "failed" ? "disabled" : ""}
          title="只重试尚未送达的目标" onclick="replayFailedTargets('${enc}')">补投失败目标</button>
      </td>
    </tr>`;
  }).join("");
}

async function replayOne(encId) {
  try {
    await api(`/api/events/${encId}/replay`, { method: "POST" });
    refreshAll();
  } catch (e) { alert(e.message); }
}

async function replayFailedTargets(encId) {
  try {
    const r = await api(`/api/events/${encId}/replay`, {
      method: "POST",
      body: JSON.stringify({ only_failed_targets: true }),
    });
    if (!r.queued) alert(r.note || "没有需要重试的目标");
    refreshAll();
  } catch (e) { alert(e.message); }
}

async function showEvent(encId) {
  const d = await api(`/api/events/${encId}`);
  $("modal-title").textContent = `事件 ${d.event_id}（#${d.seq}，${d.status}）`;
  const out = {
    event_id: d.event_id,
    status: d.status,
    created_at: d.created_at,
    updated_at: d.updated_at,
    deliveries: Object.fromEntries((d.deliveries || []).map((x) => [x.target, x.status])),
    pending_targets: d.pending_targets,
    raw_payload: d.payload_parsed,
    attempts: d.timeline.map((t) => ({
      target: t.target, replay: t.replay_no, attempt: t.attempt_no,
      at: t.request_at, http_status: t.http_status, outcome: t.outcome,
      elapsed_ms: t.elapsed_ms, idempotency_key: t.idem_key,
      response: t.resp_excerpt,
    })),
  };
  $("modal-body").textContent = JSON.stringify(out, null, 2);
  $("modal").classList.remove("hidden");
}

$("modal-close").addEventListener("click", () => $("modal").classList.add("hidden"));
$("modal").addEventListener("click", (e) => { if (e.target.id === "modal") $("modal").classList.add("hidden"); });

$("replay-all").addEventListener("click", async () => {
  const r = await api("/api/replay", { method: "POST", body: "{}" });
  alert(`已入队 ${r.queued.length} 条（跳过 ${r.skipped} 条进行中的事件）`);
  refreshAll();
});
$("replay-failed").addEventListener("click", async () => {
  const r = await api("/api/replay", {
    method: "POST",
    body: JSON.stringify({ only_failed: true, only_failed_targets: true }),
  });
  alert(`已入队 ${r.queued.length} 条失败事件（仅补投未送达目标；跳过 ${r.skipped} 条）`);
  refreshAll();
});
$("refresh").addEventListener("click", refreshAll);

$("clear").addEventListener("click", async () => {
  if (!confirm("确定删除全部事件、投递状态与尝试记录？（目标配置保留）")) return;
  await api("/api/events", { method: "DELETE" });
  await fetch("/recv/logs", { method: "DELETE" });
  refreshAll();
});

// ---------- snapshot backup / restore ----------

let restoreFile = null;

$("restore-file").addEventListener("change", (ev) => {
  restoreFile = ev.target.files[0] || null;
  $("restore-filename").textContent = restoreFile ? restoreFile.name : "未选择文件";
  $("restore").disabled = !restoreFile;
});

$("restore").addEventListener("click", async () => {
  if (!restoreFile) return;
  if (!confirm(`将用快照「${restoreFile.name}」原子替换全部事件、目标投递状态与尝试记录。\n\n` +
      "恢复前会完整校验（manifest / SHA-256 / 外键 / 载荷），任一错误都拒绝写入；\n" +
      "当前目标、HMAC 密钥、重试与超时配置会保留。\n\n确定继续？")) return;
  const btn = $("restore");
  btn.disabled = true;
  btn.textContent = "校验并恢复中…";
  try {
    const resp = await fetch("/api/restore", { method: "POST", body: restoreFile });
    const text = await resp.text();
    let data = {};
    try { data = text ? JSON.parse(text) : {}; } catch (_) { data = { detail: text }; }
    if (!resp.ok) {
      showSummary(false, { status: resp.status, ...data });
    } else {
      showSummary(true, data);
      refreshAll();
    }
  } catch (e) {
    showSummary(false, { detail: e.message });
  } finally {
    btn.textContent = "⬆ 校验并恢复";
    btn.disabled = !restoreFile;
  }
});

function kvRow(k, v, cls) {
  return `<div class="sum-row"><span class="sum-k">${esc(k)}</span>` +
         `<span class="sum-v ${cls || ""}">${v}</span></div>`;
}

function showSummary(ok, r) {
  $("summary-title").textContent = ok ? "✅ 恢复成功 — 结果摘要" : "❌ 恢复被拒绝（未写入任何数据）";
  const box = $("summary-body");
  if (!ok) {
    const detail = typeof r.detail === "string" ? r.detail : JSON.stringify(r.detail, null, 2);
    box.innerHTML =
      `<div class="sum-banner fail">校验未通过，数据库保持原样。</div>` +
      (r.status ? kvRow("HTTP", String(r.status)) : "") +
      `<pre class="sum-errors">${esc(detail || "未知错误")}</pre>`;
  } else {
    const c = r.counts || {};
    const s = r.current_settings || {};
    const tgts = (s.targets || []).map((t) =>
      `<span class="badge ${t.enabled ? "success" : "pending"}">${esc(t.name)}</span>`).join(" ");
    box.innerHTML =
      `<div class="sum-banner ok">已在单事务内整体替换数据，现有配置保留。</div>` +
      kvRow("恢复完成时间", esc(r.restored_at || "")) +
      kvRow("快照创建时间", esc(r.snapshot_created_at || "")) +
      kvRow("数据库版本", `v${esc(r.db_version)}`) +
      kvRow("事件数量", `${c.events_before ?? "?"} → <b>${c.events_after ?? "?"}</b>`) +
      kvRow("目标投递状态", `${c.deliveries_after ?? "?"} 条`) +
      kvRow("尝试记录数量", `${c.attempts_after ?? "?"}`) +
      (c.statuses_normalized_to_pending
        ? kvRow("状态归一化", `${c.statuses_normalized_to_pending} 条进行中事件已重置为 pending`)
        : "") +
      `<div class="sum-section">保留的当前配置</div>` +
      kvRow("投递目标", tgts || "—") +
      kvRow("HMAC 密钥", `<span class="mono">${esc(s.hmac_secret)}</span>`) +
      kvRow("最大重试 / 超时", `${esc(s.max_retries)} 次 / ${esc(s.timeout_seconds)} 秒`) +
      `<div class="muted" style="margin-top:8px">${esc(r.snapshot_settings_note || "")}</div>`;
  }
  $("summary-modal").classList.remove("hidden");
}

$("summary-close").addEventListener("click", () => $("summary-modal").classList.add("hidden"));
$("summary-modal").addEventListener("click", (e) => {
  if (e.target.id === "summary-modal") $("summary-modal").classList.add("hidden");
});

// ---------- timeline ----------

function outcomeBadge(o) {
  const map = {
    delivered: ["success", "已送达"],
    retriable: ["running", "可重试错误"],
    non_retriable: ["failed", "不可重试"],
  };
  const [cls, label] = map[o] || ["pending", o];
  return `<span class="badge ${cls}">${label}</span>`;
}

async function refreshTimeline() {
  const { attempts } = await api("/api/timeline?limit=200");
  $("timeline-body").innerHTML = attempts.length ? attempts.map((t) => {
    const short = t.idem_key ? t.idem_key.slice(0, 10) + "…" : "";
    const status = t.http_status == null
      ? '<span class="badge failed">无响应</span>'
      : `<span class="badge ${t.http_status < 300 ? "success" : "failed"}">${t.http_status}</span>`;
    return `<tr>
      <td class="mono">${esc((t.request_at || "").replace("T", " ").slice(11, 23))}</td>
      <td class="mono"><a class="link" onclick="showEvent('${encodeURIComponent(t.event_id)}')">${esc(t.event_id)}</a></td>
      <td><span class="badge pending">${esc(t.target || "")}</span></td>
      <td>${t.replay_no}-${t.attempt_no}<div class="muted" title="${esc(t.idem_key)}">idem ${esc(short)}</div></td>
      <td>${status}</td>
      <td>${outcomeBadge(t.outcome)}</td>
      <td>${t.elapsed_ms ?? ""} ms</td>
      <td><div class="excerpt" title="${esc(t.resp_excerpt || "")}">${esc(t.resp_excerpt || "")}</div></td>
    </tr>`;
  }).join("") : '<tr><td colspan="8" class="muted center">暂无请求记录</td></tr>';
}

async function refreshStatus() {
  const s = await api("/api/status");
  const c = s.counts;
  $("status-pills").innerHTML =
    (s.targets && s.targets.length > 1
      ? `<span class="pill">扇出目标 <b>${s.targets.length}</b></span>` : "") +
    `<span class="pill">总数 <b>${s.total}</b></span>` +
    `<span class="pill">待发送 <b>${c.pending}</b></span>` +
    `<span class="pill">队列/发送中 <b>${c.queued + c.running}</b></span>` +
    `<span class="pill">成功 <b style="color:var(--green)">${c.success}</b></span>` +
    `<span class="pill">失败 <b style="color:var(--red)">${c.failed}</b></span>`;
}

function refreshAll() {
  refreshStatus();
  refreshEvents();
  refreshTimeline();
}

refreshAll();
loadSettings().then(refreshEvents);
setInterval(() => { refreshStatus(); refreshEvents(); refreshTimeline(); }, 2500);
